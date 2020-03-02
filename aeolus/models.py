#-------------------------------------------------------------------------------
#
# VirES specific Djnago DB models.
#
# Project: VirES
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#          Martin Paces <martin.paces@eox.at>
#
#-------------------------------------------------------------------------------
# Copyright (C) 2014 EOX IT Services GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies of this Software or works derived from this Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#-------------------------------------------------------------------------------

#pylint: disable=missing-docstring,fixme,unused-argument
#pylint: disable=old-style-class,no-init,too-few-public-methods


from django.dispatch import receiver
from django.db.models import (
    Model, ForeignKey, OneToOneField, CharField, DateTimeField, CASCADE,
)
from django.db.models.signals import post_save, post_migrate, pre_delete
from django.contrib.auth.models import User, Permission, Group
from django.contrib.contenttypes.models import ContentType

from eoxserver.backends.models import DataItem
from eoxserver.resources.coverages.models import (
    Collection, CollectionType, Product
)


class Job(Model):
    """ VirES WPS asynchronous job.
    """
    ACCEPTED = 'A'  # Accepted, enqueued for processing
    STARTED = 'R'   # Running, processing in progress
    SUCCEEDED = 'S'  # Successfully finished without errors
    ABORTED = 'T'   # Terminated on user request (reserved for future use)
    FAILED = 'F'    # Failed, an error occurred
    UNDEFINED = 'U'  # Unknown undefined state

    STATUS_CHOICES = (
        (ACCEPTED, "ACCEPTED"),
        (STARTED, "STARTED"),
        (SUCCEEDED, "SUCCEEDED"),
        (ABORTED, "ABORTED"),
        (FAILED, "FAILED"),
        (UNDEFINED, "UNDEFINED"),
    )

    owner = ForeignKey(User, on_delete=CASCADE, related_name='jobs', null=True, blank=True)
    identifier = CharField(max_length=256, null=False, blank=False)
    process_id = CharField(max_length=256, null=False, blank=False)
    response_url = CharField(max_length=512, null=False, blank=False)
    created = DateTimeField(auto_now_add=True)
    started = DateTimeField(null=True)
    stopped = DateTimeField(null=True)
    status = CharField(max_length=1, choices=STATUS_CHOICES, default=UNDEFINED)

    class Meta:
        verbose_name = "WPS Job"
        verbose_name_plural = "WPS Jobs"

    def __unicode__(self):
        return "%s:%s:%s" % (self.process_id, self.identifier, self.status)


class UserCollectionLink(Model):
    """ Model class to link a django.contrib.auth.models.User to an
        eoxserver.resources.coverages.models.Collection.

        For VirES Aeolus, each user can only be linked to exactly one collection
        and vice-versa
    """
    user = OneToOneField(User, on_delete=CASCADE, related_name="user_collection")
    collection = OneToOneField(Collection, on_delete=CASCADE, related_name="user_collection")


class OptimizedProductDataItem(DataItem):
    product = OneToOneField(Product, on_delete=CASCADE, related_name='optimized_data_item')


#
# Helpers
#


def get_or_create_user_collection(user):
    identifier = "user_collection_%s" % user.username

    try:
        collection = Collection.objects.get(identifier=identifier)
    except Collection.DoesNotExist:
        collection_type, _ = CollectionType.objects.get_or_create(
            name="user_collection_type"
        )

        collection = Collection()
        collection.identifier = identifier
        collection.collection_type = collection_type

        collection.full_clean()
        collection.save()

        UserCollectionLink.objects.create(user=user, collection=collection)

    return collection


#
# Signal receivers
#

@receiver(post_migrate)
def post_migrate_receiver(*args, **kwargs):
    for user in User.objects.all():
        get_or_create_user_collection(user)

    # make sure we create the permissions for that collection
    content_type = ContentType.objects.get_for_model(Collection)
    for collection in Collection.objects.all():
        Permission.objects.get_or_create(
            codename='access_%s' % collection.identifier,
            name='Can access collection %s' % collection.identifier,
            content_type=content_type,
        )

    # default group does not have access to AUX collections
    group, created = Group.objects.get_or_create(
        name='aeolus_default'
    )

    # get permissions for "open" collections, but exclude user collections
    # and restricted collections
    permissions = Permission.objects.filter(
        codename__startswith='access_'
    ).exclude(
        codename__in=[
            'access_AUX_MRC_1B',
            'access_AUX_RRC_1B',
            'access_AUX_ISR_1B',
            'access_AUX_ZWC_1B',
        ]
    ).exclude(
        codename__startswith='access_user_collection'
    )
    group.permissions.set(permissions)
    group.save()

    for user in User.objects.all():
        user.groups.add(group)

    # privileged group has access to all collections
    group, _ = Group.objects.get_or_create(
        name='aeolus_privileged'
    )

    # get permissions for "open" collections, but exclude user collections
    permissions = Permission.objects.filter(
        codename__startswith='access_'
    ).exclude(
        codename__startswith='access_user_collection'
    )
    group.permissions.set(permissions)
    group.save()

    # give each user access to his own user collection
    for user in User.objects.all():
        user.user_permissions.add(
            Permission.objects.get(
                codename='access_user_collection_%s' % user.username
            )
        )


@receiver(post_save)
def post_save_receiver(sender, instance, created, *args, **kwargs):
    if issubclass(sender, User) and created:
        get_or_create_user_collection(instance)
        group = Group.objects.get(name='aeolus_default')
        instance.groups.add(group)

    elif issubclass(sender, Collection) and created:
        # make sure we create the permissions for that collection
        content_type = ContentType.objects.get_for_model(Collection)
        perm, _ = Permission.objects.get_or_create(
            codename='access_%s' % instance.identifier,
            name='Can access collection %s' % instance.identifier,
            content_type=content_type,
        )

        # if it is a user collection give that user the permission to view it
        link = UserCollectionLink.objects.filter(collection=instance).first()
        if link:
            link.user.user_permissions.add(perm)

        # otherwise add it to the according groups
        else:
            if 'AUX' in instance.identifier:
                group = Group.objects.get(name='aeolus_privileged')
                group.permissions.add(perm)

            group = Group.objects.get(name='aeolus_default')
            group.permissions.add(perm)


@receiver(pre_delete)
def pre_delete_receiver(sender, instance, *args, **kwargs):
    if issubclass(sender, User):
        get_or_create_user_collection(instance).delete()

    # make sure we clean up the permissions for that collection
    elif issubclass(sender, Collection):
        Permission.objects.get(
            codename='access_%s' % instance.identifier,
            name='Can access collection %s' % instance.identifier,
        ).delete()
