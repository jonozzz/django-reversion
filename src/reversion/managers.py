"""Model managers for Reversion."""
from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.contrib.auth.models import User
from django.db.models.query import QuerySet
from django.core.exceptions import ObjectDoesNotExist
from datetime import datetime

def diff_vers(v1, v2=None):
    from reversion.revisions import revision
    
    o1 = v1.get_object_version()
    registered_fields = revision.get_registration_info(o1.object.__class__).fields
    
    o2 = None
    result = []
    
    if v2:
        o2 = v2.get_object_version()

    for field in registered_fields: #IGNORE:W0212
        field = o1.object._meta.get_field(field)
        
        # Hide the internal plumbing 
        #if field.rel or field.primary_key or field.db_index:
        if field.primary_key:
            continue

        try:
            # Get related fields by their pk is much faster.
            #new_value = getattr(o1.object, field.name)
            new_value = getattr(o1.object, field.attname)
            if field.choices:
                new_value = o1.object._get_FIELD_display(field)
        except ObjectDoesNotExist:
            new_value = getattr(o1.object, field.attname)
        
        if isinstance(new_value, (models.Manager, QuerySet)):
            new_value = new_value.model.objects.filter(id__in=o1.m2m_data.get(field.name, []))
            #new_value = list(new_value.all())
            #print new_value, o1.m2m_data.get(field.name)
        
        if o2:
            try:
                #old_value = getattr(o2.object, field.name, None)
                old_value = getattr(o2.object, field.attname, None)
                if field.choices:
                    old_value = o2.object._get_FIELD_display(field)
            except ObjectDoesNotExist:
                old_value = getattr(o2.object, field.attname, None)

            if isinstance(old_value, (models.Manager, QuerySet)):
                old_value = old_value.model.objects.filter(id__in=o2.m2m_data.get(field.name, []))
                #old_value = list(old_value.all())
                #print new_value, old_value
        else:
            old_value = None

#        if field.name == '_peer':
#            print new_value, old_value
        if (v1.is_change() and (new_value or old_value) and new_value != old_value) or \
           (v1.is_addition() or v1.is_deletion()):
            # Strip microseconds from datetime objects
            if isinstance(new_value, datetime) and isinstance(old_value, datetime) and \
               new_value.strftime('%s') == old_value.strftime('%s'):
                continue
            
            # XXX: Incomplete.
            if field.rel and isinstance(new_value, int):
                if issubclass(field.rel.to, User):
                    new_value = getattr(o1.object, field.name, None)
                    if o2:
                        old_value = getattr(o2.object, field.name, None)
                else:
                    content_type = ContentType.objects.get_for_model(field.rel.to)
                    try:
                        rel_v = v1.revision.version_set.get(content_type=content_type,
                                                            object_id=new_value)
                        new_value = rel_v.object_repr
                    except ObjectDoesNotExist:
                        pass

            result.append((field.name, new_value, old_value))
    
    return result

def diff_as_text(diff):
    i = 0
    output = []
    user_cache = {}
    for k, r in iter(sorted(diff.iteritems(), reverse=True)):
        if not r['changes']:
            continue
        i += 1
        output.append('-' * 80)
        
        # Try to avoid hitting the DB for every user.
        # Don't know why the select_related() didn't get the user data, although
        # I have a hunch it's because of the subquery which masked the user field.
        if user_cache.get(r['revision'].user_id):
            user = user_cache.get(r['revision'].user_id)
        else:
            user = r['revision'].user
            user_cache[user.id] = user
        
        output.append("%d|%s|%s" % (i, r['_date'], user))
        for c in r['changes']:
            output.append("%s%s %s" % (' ' * 4, c['_type'], c['version']))
            for f in c['fields']:
                if f[2]:
                    output.append("%s%s: '%s' was: '%s'" % (' ' * 8, f[0], f[1], f[2]))
                else:
                    output.append("%s%s: '%s'" % (' ' * 8, f[0], f[1]))
    return '\n'.join(output)

class VersionManager(models.Manager):
    
    """Manager for Version models."""
    
    def get_for_object_reference(self, model, object_id):
        """Returns all versions for the given object reference."""
        content_type = ContentType.objects.get_for_model(model)
        object_id = unicode(object_id)
        versions = self.filter(content_type=content_type, object_id=object_id)
        versions = versions.order_by("pk")
        return versions
    
    def get_for_object(self, obj):
        """
        Returns all the versions of the given object, ordered by date created.
        """
        return self.get_for_object_reference(object.__class__, obj.pk)
    
    def get_unique_for_object(self, obj):
        """Returns unique versions associated with the object."""
        versions = self.get_for_object(obj)
        changed_versions = []
        last_serialized_data = None
        for version in versions:
            if last_serialized_data != version.serialized_data:
                changed_versions.append(version)
            last_serialized_data = version.serialized_data
        return changed_versions
    
    def get_for_date(self, obj, date):
        """Returns the latest version of an object for the given date."""
        versions = self.get_for_object(obj)
        versions = versions.filter(revision__date_created__lte=date)
        versions = versions.order_by("-pk")
        try:
            version = versions[0]
        except IndexError:
            raise self.model.DoesNotExist
        else:
            return version

    def get_previous(self, version):
        """Get the previous version of a given version."""
        versions = self.filter(content_type=version.content_type,
                               object_id=version.object_id,
                               pk__lt=version.pk)
        versions = versions.order_by("-pk")
        try:
            version = versions[0]
        except IndexError:
            return None
        else:
            return version
    
    def get_next(self, version):
        """Get the next version of a given version."""
        versions = self.filter(content_type=version.content_type,
                               object_id=version.object_id,
                               pk__gt=version.pk)
        versions = versions.order_by("-pk")
        try:
            version = versions[0]
        except IndexError:
            return None
        else:
            return version

    def get_deleted_object(self, model_class, object_id, select_related=None):
        """
        Returns the version corresponding to the deletion of the object with
        the given id.
        
        You can specify a tuple of related fields to fetch using the
        `select_related` argument.
        """
        # Ensure that the revision is in the select_related tuple.
        select_related = select_related or ()
        if not "revision" in select_related:
            select_related = tuple(select_related) + ("revision",)
        # Fetch the version.
        content_type = ContentType.objects.get_for_model(model_class)
        object_id = unicode(object_id)
        versions = self.filter(content_type=content_type, object_id=object_id)
        versions = versions.order_by("-pk")
        if select_related:
            versions = versions.select_related(*select_related)
        try:
            version = versions[0]
        except IndexError:
            raise self.model.DoesNotExist
        else:
            return version
    
    def get_deleted(self, model_class, select_related=None):
        """
        Returns all the deleted versions for the given model class.
        
        You can specify a tuple of related fields to fetch using the
        `select_related` argument.
        """
        content_type = ContentType.objects.get_for_model(model_class)
        deleted = []
        # HACK: This join can't be done in the database, due to incompatibilities
        # between unicode object_ids and integer pks on strict backends like postgres.
        for object_id in self.filter(content_type=content_type).values_list("object_id", flat=True).distinct().iterator():
            if model_class._default_manager.filter(pk=object_id).count() == 0:
                deleted.append(self.get_deleted_object(model_class, object_id, select_related))
        deleted.sort(lambda a, b: cmp(a.revision.date_created, b.revision.date_created))
        return deleted
        
    def diff_ver(self, version):
        #diff = self.diff(obj=version.get_object_version().object, limit=2, 
        #                 topver=version)
        #return diff[max(diff.keys())]
        prev_version = self.get_previous(version)
        return diff_vers(version, prev_version)

    # A highly optimized version diff'er.
    def diff(self, obj=None, limit=128, topver=None):

        if topver:
            q = self.filter(pk__lte=topver.pk)
        else:
            q = self.all()
        
        if obj:
            cts = []
            cts.append(ContentType.objects.get_for_model(obj))
            for parent_class in obj._meta.get_parent_list():
                cts.append(ContentType.objects.get_for_model(parent_class))
            q = q.filter(object_id=obj.id, content_type__in=cts).values('revision_id')
        else:
            q = q.values('revision_id')
            q.query.group_by = ['revision_id']
        
        q = q.order_by('-pk')[:limit]
        
        s = q.query.get_compiler(q.db).as_sql()
        # String-i-fy the subquery.
        subq = s[0] % s[1]
        # Generate the master query joined to the subquery.
        # XXX: Needs the extra-join Django patch.
        q = self.select_related().order_by('object_id', 'content_type__id', 
                                           '-pk')
        versions = q.select_related().extra(
            join=['INNER JOIN (%s) AS dt on dt.`revision_id` = '
                  '`reversion_version`.`revision_id`' % subq]
        )

        revisions = {}
        # Grab all version records in memory.
        versions_list = list(versions)
        for i in xrange(len(versions)):
            v = versions[i]
            rid = versions_list[i].revision.id
            if not revisions.get(rid):
                rdiff = {}
                rdiff['revision'] = versions_list[i].revision
                rdiff['_date'] = versions_list[i].revision.date_created
                rdiff['changes'] = []
                revisions[rid] = rdiff
            else:
                rdiff = revisions[rid]
            
            vdiff = {}
            vdiff['version'] = v
            #vdiff['_ct'] = v.content_type
            #vdiff['_repr'] = v.object_repr
            vdiff['_type'] = v.get_action_flag_display()
            #vdiff['_id'] = v.id
            vdiff['fields'] = []
            prev_ver = None
            # The version is marked as "CHANGED" but we hit the end of  the 
            # list or there are no more revisions because we limited the query.
            # then this means we can't generate the side-by-side comparison of 
            # changed fields. So it's going to look like an addition.
            if v.is_change():
                try:
                    prev_ver = versions_list[i + 1]
                    if v.object_id != prev_ver.object_id or \
                       v.content_type_id != prev_ver.content_type_id:
                        prev_ver = None
                        vdiff['_type'] = 'Add or Change'
                except IndexError:
                    pass

            vdiff['fields'] = diff_vers(v, prev_ver)
            if vdiff['fields']:
                rdiff['changes'].append(vdiff)
            #rdiff['changes'].append(vdiff)

        return revisions

class RevisionManager(models.Manager):
    
    def get_for_object(self, obj):
        content_type = ContentType.objects.get_for_model(obj)
        object_id = unicode(obj.id)

        revisions = self.filter(version__content_type=content_type, 
                                version__object_id=object_id)
        revisions = revisions.order_by("-pk")
        return revisions
