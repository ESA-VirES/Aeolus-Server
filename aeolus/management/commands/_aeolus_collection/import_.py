#-------------------------------------------------------------------------------
#
# Import Aeolus collections
#
# Authors: Martin Paces <martin.paces@eox.at>
#-------------------------------------------------------------------------------
# Copyright (C) 2026 EOX IT Services GmbH
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
# pylint: disable=missing-docstring, too-few-public-methods

import sys
import json
from traceback import print_exc
from django.db import transaction
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import User, Group, Permission
from eoxserver.resources.coverages.models import Collection, CollectionType
from .._common import Subcommand


class ImportCollectionSubcommand(Subcommand):
    name = "import"
    help = "Import Aeolus collection from a JSON file."

    def add_arguments(self, parser):
        parser.add_argument(
            "-f", "--file", dest="filename", default="-", help=(
                "Optional input JSON file-name. "
            )
        )

    def handle(self, **kwargs):
        filename = kwargs["filename"]
        with sys.stdin if filename == "-" else open(filename, "rb") as file_:
            self.save_collections(json.load(file_), **kwargs)

    def save_collections(self, data, **kwargs):
        total_count = 0
        failed_count = 0
        created_count = 0
        updated_count = 0

        for item in data:
            identifier = item.get("identifier")
            try:
                is_updated = save_collection(item, logger=self.logger)
            except Exception as error:
                failed_count += 1
                if kwargs.get("traceback"):
                    print_exc(file=sys.stderr)
                self.error(
                    "Failed to create or update collection %s! %s",
                    identifier, error
                )
            else:
                updated_count += is_updated
                created_count += not is_updated
                self.logger.info(
                    "collection %s updated" if is_updated else
                    "collection %s created", identifier
                )
            finally:
                total_count += 1

        if created_count or total_count == 0:
            self.info(
                "%d of %d collection%s created.", created_count, len(data),
                "s" if created_count != 1 else ""
            )

        if updated_count:
            self.info(
                "%d of %d collection%s updated.", updated_count, len(data),
                "s" if updated_count != 1 else ""
            )

        if failed_count:
            self.info(
                "%d of %d collection%s failed ", failed_count, len(data),
                "s" if failed_count != 1 else ""
            )
        sys.exit(failed_count)


@transaction.atomic
def save_collection(data, logger):

    identifier = data.get("identifier")
    if not identifier:
        raise ValueError("Missing collection identifier!")

    allowed_users = data.get("allowedUsers") or []
    allowed_groups = data.get("allowedGroups") or []

    collection_type_name = data.get("collectionType")
    if not collection_type_name:
        raise ValueError("Missing collection type name!")

    collection_type = get_collection_type(collection_type_name)
    if not collection_type:
        raise ValueError(f"Invalid collection type {collection_type_name}!")

    is_updated, collection = get_collection(identifier)
    collection.collection_type = collection_type
    collection.full_clean()
    collection.save()

    permission = get_access_permission(collection, logger=logger)
    _update_m2m_items(permission.user_set, get_users(allowed_users))
    _update_m2m_items(permission.group_set, get_groups(allowed_groups))

    return is_updated


def _update_m2m_items(target, items):
    for item in target.exclude(pk__in=[item.pk for item in items]):
        target.remove(item)
    for item in items:
        target.add(item)


def get_collection_type(identifier):
    try:
        return CollectionType.objects.get(name=identifier)
    except CollectionType.DoesNotExist:
        return None


def get_collection(identifier):
    try:
        return True, Collection.objects.get(identifier=identifier)
    except Collection.DoesNotExist:
        return False, Collection(identifier=identifier)


def get_users(identifiers):
    if not identifiers:
        return []
    items = {
        item.username: item
        for item in User.objects.filter(username__in=identifiers)
    }
    for identifier in identifiers:
        if identifier not in items:
            raise ValueError(f"Unknown user {identifier}!")
    return list(items.values())


def get_groups(identifiers):
    if not identifiers:
        return []
    items = {
        item.name: item
        for item in Group.objects.filter(name__in=identifiers)
    }
    for identifier in identifiers:
        if identifier not in items:
            raise ValueError(f"Unknown group {identifier}!")
    return list(items.values())


def get_access_permission(collection, logger):
    permission, created = Permission.objects.get_or_create(
        codename=f"access_{collection.identifier}",
        name=f"Can access collection {collection.identifier}",
        content_type=ContentType.objects.get_for_model(Collection),
    )
    if created:
        logger.info("permission %s created", permission.codename)
    else:
        logger.debug("permission %s exist", permission.codename)
    return permission
