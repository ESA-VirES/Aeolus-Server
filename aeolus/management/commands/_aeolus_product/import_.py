#-------------------------------------------------------------------------------
#
# Import Aeolus products
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
# pylint: disable=missing-docstring, too-few-public-methods, too-many-arguments
# pylint: disable=too-many-arguments, too-many-positional-arguments

import sys
import json
from traceback import print_exc
from dataclasses import dataclass
from eoxserver.resources.coverages.models import Product
from aeolus.management.api.product import (
    DEF_SIMPLIFICATION_TOLERANCE,
    register_product,
    deregister_product,
    unlink_product_from_collection,
    update_product_collection,
    get_product_collection,
    get_allowed_product_types,
)
from .._aeolus_product.common import ObjectSelectionSubcommand


class ImportProductSubcommand(ObjectSelectionSubcommand):
    name = "import"
    help = "Import Aeolus products from a JSON file."

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-f", "--file", dest="filename", default="-", help=(
                "Optional input JSON file-name. "
            )
        )
        parser.add_argument(
            "--sync", "--remove-missing", dest="remove_missing",
            action="store_true", default=False, help=(
                "Synchronize the product records with the input, i.e., "
                "de-register products not present in the input JSON."
            )
        )
        parser.add_argument(
            "--update", "--re-register", dest="update_existing",
            action="store_true", default=False, help=(
                "Update existing records. By default the existing records "
                "are not updated."
            )
        )
        parser.add_argument(
            "--simplify",
            type=float, default=DEF_SIMPLIFICATION_TOLERANCE,
            dest="simplification_tolerance",
            help=(
                "Simplify footprint with the requested tolerance. See "
                "https://docs.djangoproject.com/en/2.2/ref/contrib/gis/geos/"
                "#django.contrib.gis.geos.GEOSGeometry.simplify for details. "
            )
        )
        parser.add_argument(
            "--do-not-simplify", dest="simplify",
            action="store_false", default=True,
            help="Do not simplify footprint geometry."
        )

    def handle(self, **kwargs):
        products = self.select_objects((
                Product.objects.all()
                .select_related("product_type", "optimized_data_item")
                .prefetch_related("product_data_items")
            ), **kwargs
        )
        remove_missing = kwargs["remove_missing"]
        update_existing = kwargs["update_existing"]
        simplification_tolerance = (
            kwargs["simplification_tolerance"] if kwargs["simplify"] else None
        )

        filename = kwargs["filename"]
        with sys.stdin if filename == "-" else open(filename, "rb") as file_:
            self.import_products(
                json.load(file_), products,
                remove_missing=remove_missing,
                update_existing=update_existing,
                traceback=kwargs.get("traceback"),
                simplification_tolerance=simplification_tolerance,
            )

    def import_products(self, records, products, remove_missing=False,
                        update_existing=False, **options):
        collections = {}
        counter = Counter()

        if remove_missing:
            collections.update(
                self._remove_products(
                    records, products, counter, **options
                )
            )

        collections.update(
            self._import_products(
                records, counter, update_existing=update_existing, **options,
            )
        )

        self._update_collections(collections.values(), **options)

        counter.print_summary(self.info)

        sys.exit(counter.failed)

    def _import_products(self, records, counter, update_existing=False,
                         traceback=False, simplification_tolerance=None, **_):
        """ Update or insert product records. """
        collections = {}

        def _import_product(record):

            identifier = record.get("identifier")
            if not identifier:
                raise ValueError("Missing mandatory product identifier.")

            collection_id = record.get("collection")
            if not collection_id:
                raise ValueError("Missing mandatory collection identifier.")

            collection = collections.get(collection_id)
            if not collection:
                collection = get_product_collection(collection_id)

            allowed_product_types = get_allowed_product_types(collection)

            data_items = {
                data_item.get("type"): {
                    "location": data_item.get("location"),
                    "format": data_item.get("format"),
                }
                for data_item in record.get("dataItems")
            }

            filename = (data_items.get("source") or {}).get("location")
            if not filename:
                raise ValueError("Missing mandatory source product location.")

            result = register_product(
                collection, identifier, filename,
                update_existing=update_existing,
                simplification_tolerance=simplification_tolerance,
                defer_collection_update=True,
                allowed_product_types=allowed_product_types,
                logger=self.logger,
            )

            if collection_id in result.linked_to_collection:
                collections[collection_id] = collection

            # TODO: optimised products

            return result

        for record in records:
            collection_id = record.get("collection")
            identifier = record.get("identifier")
            try:
                result = _import_product(record)
            except Exception as error:
                counter.imported_failed += 1
                if traceback:
                    print_exc(file=sys.stderr)
                self.error(
                    "Failed to import product %s/%s! %s",
                    collection_id, identifier, error
                )
            else:
                if result.created:
                    counter.imported += 1
                else:
                    counter.imported_skipped += 1
                counter.removed += len(result.removed)
            finally:
                counter.total += 1

        return collections


    def _remove_products(self, records, products, counter, traceback=None, **_):
        """ Remove products not present in the imported product records. """

        preserved_products = set()
        preserved_links = set()
        for item in records:
            product_id = item.get("identifier")
            collection_id = item.get("collection")
            if product_id:
                preserved_products.add(product_id)
                if collection_id:
                    preserved_links.add((collection_id, product_id))

        collections = {}

        for product in products:
            if product.identifier not in preserved_products:
                try:
                    collections.update(
                        deregister_product(
                            product,
                            logger=self.logger,
                            update_collections=False
                        )
                    )
                except Exception as error:
                    if traceback:
                        print_exc(file=sys.stderr)
                    self.error(
                        "Failed to de-register product %s! %s",
                        product.identifier, error
                    )
                    counter.removed_failed += 1
                else:
                    counter.removed += 1
                continue

            for collection in product.collections.all():
                if (collection.identifier, product.identifier) not in preserved_links:
                    try:
                        unlink_product_from_collection(
                            collection, product, logger=self.logger,
                            update_collection=False
                        )
                    except Exception as error:
                        if traceback:
                            print_exc(file=sys.stderr)
                        self.error(
                            "Failed to product %s from collection %s! %s",
                            product.identifier, collection.identifier, error
                        )
                        counter.unlinked_failed += 1
                    else:
                        counter.unlinked += 1

        return collections

    def _update_collections(self, collections, traceback=False, **_):
        for collection in collections:
            try:
                update_product_collection(collection, logger=self.logger)
            except Exception as error:
                if traceback:
                    print_exc(file=sys.stderr)
                self.error(
                    "Failed to update collection %s! %s",
                    collection.identifier, error
                )

@dataclass
class Counter():

    total: int = 0
    imported: int = 0
    imported_skipped: int = 0
    imported_failed: int = 0
    removed: int = 0
    removed_failed: int = 0
    unlinked: int = 0
    unlinked_failed: int = 0

    @property
    def failed(self):
        return self.imported_failed + self.removed_failed + self.unlinked_failed

    def print_summary(self, print_function):

        if self.imported or self.total == 0:
            print_function(
                "%d of %d product%s registered.",
                self.imported, self.total,
                "s" if self.imported != 1 else ""
            )

        if self.imported_skipped:
            print_function(
                "%d of %d product%s skipped.",
                self.imported_skipped, self.total,
                "s" if self.imported_skipped != 1 else ""
            )

        if self.imported_failed:
            print_function(
                "%d of %d product%s failed to be registered",
                self.imported_failed, self.total,
                "s" if self.imported_failed != 1 else ""
            )

        if self.removed:
            print_function(
                "%d product%s de-registered.",
                self.removed, "s" if self.removed != 1 else ""
            )

        if self.removed_failed:
            print_function(
                "%d product%s failed to be de-registered.",
                self.removed_failed, "s" if self.removed_failed != 1 else ""
            )

        if self.unlinked:
            print_function(
                "%d product%s unlinked from collection.",
                self.unlinked, "s" if self.unlinked != 1 else ""
            )

        if self.unlinked_failed:
            print_function(
                "%d product%s failed to be unlinked from collection.",
                self.unlinked_failed, "s" if self.unlinked_failed != 1 else ""
            )
