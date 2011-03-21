"""Revision management for Reversion."""
try:
    from functools import wraps
except ImportError:
    from django.utils.functional import wraps  # Python 2.4 fallback.

import operator
from threading import local
import copy

from django.contrib.admin.models import ADDITION, CHANGE, DELETION
from django.contrib.contenttypes.models import ContentType
from django.core import serializers
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models import Q, Max
from django.db.models.query import QuerySet
from django.db.models.signals import post_save, pre_delete, pre_save, post_init

from reversion.errors import RevisionManagementError, RegistrationError
from reversion.models import Revision, Version, VERSION_ADD, VERSION_CHANGE, VERSION_DELETE
from reversion.storage import VersionFileStorageWrapper


class RegistrationInfo(object):
    
    """Stored registration information about a model."""
    
    __slots__ = "fields", "file_fields", "follow", "format",
    
    def __init__(self, fields, file_fields, follow, format):
        """Initializes the registration info."""
        self.fields = fields
        self.file_fields = file_fields
        if isinstance(follow, tuple):
            self.follow = follow
        elif isinstance(follow, str):
            self.follow = (follow,)
        elif isinstance(follow, list):
            self.follow = tuple(follow)
        else:
            raise ValueError, follow
        self.format = format

          
class RevisionState(local):
    
    """Manages the state of the current revision."""
    
    def __init__(self):
        """Initializes the revision state."""
        self.clear()
    
    def clear(self):
        """Puts the revision manager back into its default state."""
        self.objects = set()
        self.dead_objects = set()
        self.user = None
        self.comment = ""
        self.depth = 0
        self.is_invalid = False
        self.meta = []
   

class ReversionMeta(object):
    
    """Stores information about the action performed on an instance."""
    
    def __init__(self):
        """Initializes the meta data."""
        self.clear()
    
    def clear(self):
        """Set the default values."""
        self.action = 0
        self.serialized_data = ''


DEFAULT_SERIALIZATION_FORMAT = "python"
   
   
class RevisionManager(object):
    
    """Manages the configuration and creation of revisions."""
    
    __slots__ = "__weakref__", "_registry", "_state",
    
    def __init__(self):
        """Initializes the revision manager."""
        self._registry = {}
        self._state = RevisionState()

    # Registration methods.

    def is_registered(self, model_class):
        """
        Checks whether the given model has been registered with this revision
        manager.
        """
        return model_class in self._registry
        
    def register(self, model_class, fields=None, follow=(), 
                 format=DEFAULT_SERIALIZATION_FORMAT, exclude_fields=()):
        """Registers a model with this revision manager."""
        # Prevent multiple registration.
        if self.is_registered(model_class):
            raise RegistrationError, "%r has already been registered with Reversion." % model_class
        # Ensure the parent model of proxy models is registered.
        if model_class._meta.proxy and not \
           self.is_registered(model_class._meta.parents.keys()[0]):
            raise RegistrationError, "%r is a proxy model, and its parent has "\
                            "not been registered with Reversion." % model_class
        # Calculate serializable model fields.
        opts = model_class._meta

        # Reasoning:
        # In case of hierarchical models the we need all fields to be registered
        # This way when we serialize the parent it will show all needed fields.
        #local_fields = opts.local_fields + opts.local_many_to_many
        local_fields = opts.fields + opts.many_to_many
        if fields is None:
            fields = [f.name for f in local_fields]

        fields = tuple(fields)
        # Calculate serializable model file fields.
        file_fields = []
        for field in local_fields:
            if isinstance(field, models.FileField) and field.name in fields:
                field.storage = VersionFileStorageWrapper(field.storage)
                file_fields.append(field)
        file_fields = tuple(file_fields)
        # Register the generated registration information.
        if isinstance(follow, basestring):
            follow = (follow,)
        else:
            follow = tuple(follow)
        tmp_fields = [f for f in fields if f not in exclude_fields]
        fields = tuple(tmp_fields)
        registration_info = RegistrationInfo(fields, file_fields, follow, 
                                             format)
        self._registry[model_class] = registration_info
        # Connect to the post save signal of the model.
        post_save.connect(self.post_save_receiver, model_class)
        pre_delete.connect(self.pre_delete_receiver, model_class)
        pre_save.connect(self.pre_save_receiver, model_class)
        post_init.connect(self.post_init_receiver, model_class)
    
    def get_registration_info(self, model_class):
        """Returns the registration information for the given model class."""
        try:
            registration_info = self._registry[model_class]
        except KeyError:
            raise RegistrationError, "%r has not been registered with " \
                                     "Reversion." % model_class
        else:
            return registration_info
        
    def unregister(self, model_class):
        """Removes a model from version control."""
        try:
            registration_info = self._registry.pop(model_class)
        except KeyError:
            raise RegistrationError, "%r has not been registered with " \
                                     "Reversion." % model_class
        else:
            for field in registration_info.file_fields:
                field.storage = field.storage.wrapped_storage
            post_save.disconnect(self.post_save_receiver, model_class)
            pre_delete.disconnect(self.pre_delete_receiver, model_class)
            pre_save.disconnect(self.pre_save_receiver, model_class)
            post_init.disconnect(self.post_init_receiver, model_class)
    
    # Low-level revision management methods.
    
    def start(self):
        """
        Begins a revision for this thread.
        
        This MUST be balanced by a call to `end`.  It is recommended that you
        leave these methods alone and instead use the revision context manager
        or the `create_on_success` decorator.
        """
        self._state.depth += 1
        
    def is_active(self):
        """Returns whether there is an active revision for this thread."""
        return self._state.depth > 0
    
    def assert_active(self):
        """Checks for an active revision, throwning an exception if none."""
        if not self.is_active():
            raise RevisionManagementError, "There is no active revision for " \
                                           "this thread."
    
    def add(self, obj):
        """Adds an object to the current revision."""
        self.assert_active()
        if obj._reversion.action == DELETION:
            neighbours = self.follow_relationships([obj], max_recursion = 1, 
                                                    inclusive = False)
            self._state.dead_objects.add(obj)
            # 06/05/10: Don't add dead neighbours. This happens when you want
            # to delete a model that is linked to anohter one by OneTnOne
            # relation. This will still work for inherited models that are using
            # the same OneToOne relation.
            self._state.objects |= neighbours - self._state.dead_objects
            self._state.objects.discard(obj)
        else:
            self._state.objects.add(obj)
        
    def set_user(self, user):
        """Sets the user for the current revision"""
        self.assert_active()
        self._state.user = user
        
    def get_user(self):
        """Gets the user for the current revision."""
        self.assert_active()
        return self._state.user
    
    user = property(get_user,
                    set_user,
                    doc="The user for the current revision.")
        
    def set_comment(self, comment):
        """Sets the comment for the current revision"""
        self.assert_active()
        self._state.comment = comment
        
    def get_comment(self):
        """Gets the comment for the current revision."""
        self.assert_active()
        return self._state.comment
    
    comment = property(get_comment,
                       set_comment,
                       doc="The comment for the current revision.")
        
    def add_meta(self, cls, **kwargs):
        """Adds a class of meta information to the current revision."""
        self.assert_active()
        self._state.meta.append((cls, kwargs))
    
    def invalidate(self):
        """Marks this revision as broken, so should not be commited."""
        self.assert_active()
        self._state.is_invalid = True
        
    def is_invalid(self):
        """Checks whether this revision is invalid."""
        return self._state.is_invalid
        
    def follow_relationships(self, object_set, max_recursion = None,
                            inclusive = True, ancestors = False):
        """
        Follows all the registered relationships in the given set of models to
        yield a set containing the original models plus all their related
        models.
        """
        result_set = set()
        def _follow_relationships(obj, level = 0):
            # Prevent recursion.
            if obj in result_dict or obj.pk is None:  # This last condition is because during a delete action the parent field for a subclassing model will be set to None.
                return
            if inclusive or level > 0:
                result_set.add(obj)
            # Follow relations.
            if ancestors:
                to_follow = [f.name for f in obj._meta.parents.values()]
            else:
                registration_info = self.get_registration_info(obj.__class__)
                to_follow = registration_info.follow
            for relationship in to_follow:
                # Clear foreign key cache.
                try:
                    related_field = obj._meta.get_field(relationship)
                except models.FieldDoesNotExist:
                    pass
                else:
                    if isinstance(related_field, models.ForeignKey):
                        if hasattr(obj, related_field.get_cache_name()):
                            delattr(obj, related_field.get_cache_name())
                # Recursion level reached.
                if max_recursion != None and level >= max_recursion:
                    continue
                # Get the references obj(s).
                try:
                    related = getattr(obj, relationship, None)
                except ObjectDoesNotExist:
                    related = None
                if isinstance(related, models.Model):
                    # Notify the parents about the change excluding those 
                    # already marked for deletion.
                    if getattr(related, '_reversion', False) and \
                       related._reversion.action is not DELETION:
                        related._reversion.action = CHANGE
                    _follow_relationships(related, level + 1) 
                elif isinstance(related, (models.Manager, QuerySet)):
                    for related_obj in related.all():
                        # Notify many-to-many friends about the change.
                        if getattr(related_obj, '_reversion', False) and \
                           related_obj._reversion.action is not DELETION:
                            related_obj._reversion.action = CHANGE
                        _follow_relationships(related_obj, level + 1)
                elif related is not None:
                    raise TypeError, "Cannot follow the relationship %r. " \
                                "Expected a model or QuerySet, found %r." % \
                                (relationship, related)
            # If a proxy model's parent is registered, add it.
            if obj._meta.proxy:
                parent_cls = obj._meta.parents.keys()[0]
                if self.is_registered(parent_cls):
                    parent_obj = parent_cls.objects.get(pk=obj.pk)
                    _follow_relationships(parent_obj, level + 1)
        map(_follow_relationships, object_set)
        return result_set
        
    def end(self):
        """Ends a revision."""
        self.assert_active()
        self._state.depth -= 1
        # Handle end of revision conditions here.
        if self._state.depth == 0:
            models = self._state.objects
            dead_models = self._state.dead_objects
            models -= dead_models
            try:
                if (dead_models or models) and not self.is_invalid():
                    # Save a new revision.
                    revision = Revision.objects.create(user=self._state.user,
                                                    comment=self._state.comment)
                    # Follow relationships.
                    revision_set = \
                            self.follow_relationships(self._state.objects)
                    # Because we might have uncomitted data in models, we need 
                    # to replace the models in revision_set which might have 
                    # come from the db, with the actual models sent to 
                    # reversion.
                    diff = revision_set.difference(models)
                    revision_set = models.union(diff)
                    # Save version models.
                    for obj in revision_set:
                        # Proxy models should not actually be saved to the 
                        # revision set.
                        if obj._meta.proxy:
                            continue
                        action = obj._reversion.action
                        registration_info = self.get_registration_info(obj.__class__)
                        object_id = unicode(obj.pk)
                        content_type = ContentType.objects.get_for_model(obj)
                        if action is DELETION:
                            raise ValueError, "BUG: there's a dead model " \
                                              "among live ones. %r" % obj
                        else:
                            ancestors_and_self = \
                                self.follow_relationships([obj], ancestors=True)
                            serialized_data = \
                                serializers.serialize(registration_info.format, 
                                                      ancestors_and_self,
                                                fields=registration_info.fields)
                        Version.objects.create(revision=revision,
                                               object_id=object_id,
                                               content_type=content_type,
                                               format=registration_info.format,
                                               serialized_data=serialized_data,
                                               object_repr=unicode(repr(obj)),
                                               action_flag=action)
                    
                    # For objects that have already been deleted, get the stored 
                    # serialized data and attach it to the version.
                    for obj in dead_models:
                        action = obj._reversion.action
                        assert action is DELETION, "%r action: %d" % (obj, action)
                        registration_info = self.get_registration_info(obj.__class__)
                        object_id = unicode(obj.pk)
                        content_type = ContentType.objects.get_for_model(obj)
                        serialized_data = obj._reversion.serialized_data
                        original_repr = obj._reversion.repr
                        Version.objects.create(revision=revision,
                                               object_id=object_id,
                                               content_type=content_type,
                                               format=registration_info.format,
                                               serialized_data=serialized_data,
                                               object_repr=unicode(original_repr),
                                               action_flag=action)
                        
                    for cls, kwargs in self._state.meta:
                        cls._default_manager.create(revision=revision, **kwargs)
            finally:
                self._state.clear()
        
    # Signal receivers.
        
    def post_init_receiver(self, instance, sender, **kwargs):
        """Creates the reversion meta and attaches it to the instance."""
        instance._reversion = ReversionMeta()

    def pre_save_receiver(self, instance, sender, **kwargs):
        """Detect the kind of update and stores it in the reversion meta."""
        if instance.pk is None:
            instance._reversion.action = ADDITION
        else:
            instance._reversion.action = CHANGE

    def post_save_receiver(self, instance, sender, **kwargs):
        """Adds registered models to the current revision, if any."""
        if self.is_active():
            if created:
                self.add(instance, VERSION_ADD)
            else:
                self.add(instance, VERSION_CHANGE)
            
    def pre_delete_receiver(self, instance, **kwargs):
        """Adds registerted models to the current revision, if any."""
        if self.is_active():
            self.add(instance, VERSION_DELETE)
       
    def pre_delete_receiver(self, instance, sender, **kwargs):
        """
        Freezes the instance contents and adds registered models to the current 
        revision, if any.
        """
        instance._reversion.action = DELETION
        tmp = copy.copy(instance)
        ancestors_and_self = self.follow_relationships([instance], 
                                                       ancestors=True)
        registration_info = self.get_registration_info(tmp.__class__)
        tmp._reversion.serialized_data = serializers.serialize(registration_info.format, 
                                                               ancestors_and_self, 
                                                               fields=registration_info.fields)
        tmp._reversion.repr = repr(tmp)

        if self.is_active():
            self.add(tmp)

    # High-level revision management methods.
        
    def __enter__(self):
        """Enters a block of revision management."""
        self.start()
        
    def __exit__(self, exc_type, exc_value, traceback):
        """Leaves a block of revision management."""
        if exc_type is not None:
            self.invalidate()
        self.end()
        return False
        
    def create_on_success(self, func):
        """Creates a revision when the given function exits successfully."""
        def _create_on_success(*args, **kwargs):
            self.start()
            try:
                try:
                    result = func(*args, **kwargs)
                except:
                    self.invalidate()
                    raise
            finally:
                self.end()
            return result
        return wraps(func)(_create_on_success)

        
# A thread-safe shared revision manager.
revision = RevisionManager()
