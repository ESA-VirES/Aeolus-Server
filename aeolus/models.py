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


from logging import getLogger
from django.dispatch import receiver
from django.db.models import (
    Model, ForeignKey, OneToOneField, CharField, DateTimeField, CASCADE,
)
from django.db.models.signals import post_save, post_migrate, pre_delete
from django.contrib.auth.models import User, Permission, Group
from django.contrib.contenttypes.models import ContentType
from allauth.socialaccount.models import SocialAccount

from eoxserver.backends.models import DataItem
from eoxserver.resources.coverages.models import (
    Collection, CollectionType, Product, ProductType
)
from .vires_permissions import update_user_groups


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


def get_or_create_user_collection_type(logger=None):
    if not logger:
        logger = getLogger(__name__)
    collection_type, created = CollectionType.objects.get_or_create(
        name="user_collection_type"
    )
    if created:
        collection_type.allowed_product_types.set(
            ProductType.objects.all()
        )
        logger.info("collection type %s created", collection_type.name)
    else:
        logger.debug("collection type %s exists", collection_type.name)
    return collection_type


def get_or_create_user_collection(user, logger=None):
    if not logger:
        logger = getLogger(__name__)
    identifier = f"user_collection_{user.username}"
    try:
        collection = Collection.objects.get(identifier=identifier)
        logger.debug("collection %s exists", identifier)
    except Collection.DoesNotExist:
        collection_type = get_or_create_user_collection_type()

        collection = Collection()
        collection.identifier = identifier
        collection.collection_type = collection_type

        collection.full_clean()
        collection.save()

        logger.info("collection %s created", identifier)

        UserCollectionLink.objects.create(user=user, collection=collection)

    return collection


def get_user_for_user_collection(collection, prefix="user_collection_"):
    if collection.identifier.startswith(prefix):
        username = collection.identifier[len(prefix):]
        return User.objects.get(username=username)
    return None


def delete_user_collection(user, logger=None):
    if not logger:
        logger = getLogger(__name__)
    identifier = f"user_collection_{user.username}"
    try:
        Collection.objects.get(identifier=identifier).delete()
        logger.info("collection %s removed", identifier)
    except Collection.DoesNotExist:
        logger.debug("collection %s does not exist", identifier)


def get_or_create_collection_permission(collection, content_type=None, logger=None):
    if not logger:
        logger = getLogger(__name__)
    if not content_type:
        content_type = ContentType.objects.get_for_model(Collection)
    permission, created = Permission.objects.get_or_create(
        codename=f"access_{collection.identifier}",
        name=f"Can access collection {collection.identifier}",
        content_type=content_type
    )
    if created:
        logger.info("permission %s created", permission.codename)
    else:
        logger.debug("permission %s exists", permission.codename)
    return permission


def delete_collection_permission(collection, logger=None):
    condename = f"access_{collection.identifier}"
    try:
        Permission.objects.get(codename=condename).delete()
        logger.info("permission %s removed", condename)
    except Permission.DoesNotExist:
        logger.debug("permission %s does not exist", condename)


def init_user_collections(logger=None):
    if not logger:
        logger = getLogger(__name__)
    content_type = ContentType.objects.get_for_model(Collection)
    for user in User.objects.all():
        collection = get_or_create_user_collection(user, logger=logger)
        user.user_permissions.add(
            get_or_create_collection_permission(
                collection, content_type=content_type, logger=logger
            )
        )


def create_collection_permissions(logger=None):
    if not logger:
        logger = getLogger(__name__)
    content_type = ContentType.objects.get_for_model(Collection)
    for collection in Collection.objects.all():
        get_or_create_collection_permission(
            collection, content_type=content_type, logger=logger
        )

#
# Signal receivers
#

@receiver(post_migrate)
def post_migrate_receiver(*args, **kwargs):
    create_collection_permissions()
    init_user_collections()


@receiver(post_save)
def post_save_receiver(sender, instance, created, *args, **kwargs):
    if issubclass(sender, User) and created:
        get_or_create_user_collection(instance)

    elif issubclass(sender, SocialAccount):
        update_user_groups(instance)

    elif issubclass(sender, Collection) and created:
        permission = get_or_create_collection_permission(instance)

        # if a user collection give that user the permission to view it
        user = get_user_for_user_collection(instance)
        if user:
            user.user_permissions.add(permission)


@receiver(pre_delete)
def pre_delete_receiver(sender, instance, *args, **kwargs):
    if issubclass(sender, User):
        delete_user_collection(instance)

    elif issubclass(sender, Collection):
        delete_collection_permission(instance)
