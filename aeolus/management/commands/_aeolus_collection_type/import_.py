#-------------------------------------------------------------------------------
#
# Import Aeolus collection types
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
from eoxserver.resources.coverages.models import (
    CollectionType,
    ProductType,
    CoverageType,
)
from .._common import Subcommand


class ImportCollectionTypeSubcommand(Subcommand):
    name = "import"
    help = "Import Aeolus collection types from a JSON file."

    def add_arguments(self, parser):
        parser.add_argument(
            "-f", "--file", dest="filename", default="-", help=(
                "Optional input JSON file-name. "
            )
        )

    def handle(self, **kwargs):
        filename = kwargs['filename']
        with sys.stdin if filename == "-" else open(filename, "rb") as file_:
            self.save_collection_types(json.load(file_), **kwargs)

    def save_collection_types(self, data, **kwargs):
        total_count = 0
        failed_count = 0
        created_count = 0
        updated_count = 0

        for item in data:
            identifier = item.get("name")
            try:
                is_updated = save_collection_type(item)
            except Exception as error:
                failed_count += 1
                if kwargs.get('traceback'):
                    print_exc(file=sys.stderr)
                self.error(
                    "Failed to create or update collection type %s! %s",
                    identifier, error
                )
            else:
                updated_count += is_updated
                created_count += not is_updated
                self.logger.info(
                    "collection type %s updated" if is_updated else
                    "collection type %s created", identifier
                )
            finally:
                total_count += 1

        if created_count or total_count == 0:
            self.info(
                "%d of %d collection type%s created.", created_count, len(data),
                "s" if created_count != 1 else ""
            )

        if updated_count:
            self.info(
                "%d of %d collection type%s updated.", updated_count, len(data),
                "s" if updated_count != 1 else ""
            )

        if failed_count:
            self.info(
                "%d of %d collection type%s failed ", failed_count, len(data),
                "s" if failed_count != 1 else ""
            )
        sys.exit(failed_count)


@transaction.atomic
def save_collection_type(data):

    identifier = data.get("name")
    if not identifier:
        raise ValueError("Missing collection type name!")
    allowed_coverage_types = set(data.get("allowedCoverageTypes") or ())
    allowed_product_types = set(data.get("allowedProductTypes") or ())

    is_updated, collection_type = get_collection_type(identifier)

    collection_type.save()

    _update_m2m_items(
        collection_type.allowed_coverage_types,
        get_coverage_types(allowed_coverage_types)
    )

    _update_m2m_items(
        collection_type.allowed_product_types,
        get_product_types(allowed_product_types)
    )

    return is_updated


def _update_m2m_items(target, items):
    for item in target.exclude(pk__in=[item.pk for item in items]):
        target.remove(item)
    for item in items:
        target.add(item)


def get_coverage_types(identifiers):
    items = {
        item.name: item
        for item in CoverageType.objects.filter(name__in=identifiers)
    }
    for identifier in identifiers:
        if identifier not in items:
            raise ValueError(f"Unknown coverage type {identifier}!")
    return list(items.values())


def get_product_types(identifiers):
    items = {
        item.name: item
        for item in ProductType.objects.filter(name__in=identifiers)
    }
    for identifier in identifiers:
        if identifier not in items:
            raise ValueError(f"Unknown product type {identifier}!")
    return list(items.values())


def get_collection_type(identifier):
    try:
        return True, CollectionType.objects.get(name=identifier)
    except CollectionType.DoesNotExist:
        return False, CollectionType(name=identifier)
