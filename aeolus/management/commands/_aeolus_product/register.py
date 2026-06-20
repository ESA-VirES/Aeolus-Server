#-------------------------------------------------------------------------------
#
# Register Aeolus products
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
# pylint: disable=missing-docstring

import sys
from traceback import print_exc
from dataclasses import dataclass
from aeolus.management.api.product import (
    DEF_SIMPLIFICATION_TOLERANCE,
    get_product_id,
    register_product,
    update_product_collection,
    get_product_collection,
    get_allowed_product_types,
)
from .._common import Subcommand


class RegisterProductSubcommand(Subcommand):
    name = "register"
    help = (
        "Register one or more products to a collection. "
    )

    def add_arguments(self, parser):
        parser.add_argument("product-file", nargs="*")
        parser.add_argument(
            "-f", "--file", dest="input_file", default=None,
            help=(
                "Optional file from which the inputs are read rather "
                "than form the command line arguments. Use dash to read "
                "filenames from standard input."
            )
        )
        parser.add_argument(
            "-c", "--collection",
            dest="collection_id", required=True, help=(
                "Mandatory name of the product collection the product(s) "
                "should be placed in."
            )
        )
        parser.add_argument(
            "--update", "--re-register", dest="ignore_registered",
            action="store_false", default=True, help=(
                "Update product record when the product is already registered.  "
                "By default, the registration is skipped."
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
        parser.add_argument(
            "--defer-collection-update", dest="defer_collection_update",
            action="store_true", default=True,
            help="Defer collection updates once all objects are inserted."
        )
        parser.add_argument(
            "--instant-collection-update", dest="defer_collection_update",
            action="store_false",
            help="Perform collection when the objects are inserted."
        )

    def handle(self, **kwargs):
        data_files = kwargs["product-file"]
        update_existing = not kwargs["ignore_registered"]
        collection_id = kwargs["collection_id"]
        simplification_tolerance = (
            kwargs["simplification_tolerance"] if kwargs["simplify"] else None
        )
        defer_collection_update = kwargs["defer_collection_update"]

        collection = get_product_collection(collection_id)
        allowed_product_types = get_allowed_product_types(collection)

        counter = Counter()

        for data_file in read_products(kwargs["input_file"], data_files):
            product_id = get_product_id(data_file)
            try:
                result = register_product(
                    collection, product_id, data_file,
                    update_existing=update_existing,
                    simplification_tolerance=simplification_tolerance,
                    defer_collection_update=defer_collection_update,
                    allowed_product_types=allowed_product_types,
                    logger=self.logger
                )
            except Exception as error:
                if kwargs.get("traceback"):
                    print_exc(file=sys.stderr)
                self.error(
                    "Registration of product %s/%s failed! %s",
                    collection.identifier, product_id, error
                )
                collection.refresh_from_db()
                counter.failed += 1
                result = None

            else:
                counter.removed += len(result.removed)
                if result.created:
                    counter.inserted += 1
                else:
                    counter.skipped += 1
            finally:
                counter.total += 1

        if defer_collection_update:
            try:
                update_product_collection(collection, logger=self.logger)
            except Exception as error:
                if kwargs.get("traceback"):
                    print_exc(file=sys.stderr)
                self.error(
                    "Failed to update collection %s! %s",
                    collection.identifier, error
                )

        counter.print_report(lambda msg: print(msg, file=sys.stderr))

        sys.exit(counter.failed > 0)



def _get_allowed_product_types(collection):
    collection_type = collection.collection_type
    if collection_type:
        return set(
            product_type.identifier
            for product_type in collection_type.allowed_product_types
        )
    return None


@dataclass
class Counter:
    total: int = 0
    inserted: int = 0
    #updated: int = 0
    removed: int = 0
    skipped: int = 0
    failed: int = 0

    def print_report(self, print_fcn):

        def _plural(value):
            return "s" if value != 1 else ""

        if self.inserted > 0 or self.total == 0:
            print_fcn(f"{self.inserted} of {self.total} product{_plural(self.total)} registered.")

        #if self.updated > 0:
        #    print_fcn(f"{self.updated} of {self.total} product{_plural(self.total)} updated.")

        if self.skipped > 0:
            print_fcn(f"{self.skipped} of {self.total} product{_plural(self.total)} skipped.")

        if self.removed > 0:
            print_fcn(f"{self.removed} product{_plural(self.removed)} de-registered.")

        if self.failed > 0:
            print_fcn(
                f"Failed to register {self.failed} of {self.total} product{_plural(self.total)}."
            )


def read_products(filename, args):
    """ Get products iterator. """

    def _read_lines(lines):
        for line in lines:
            line = line.partition("#")[0] # strip comments
            line = line.strip() # strip white-space padding
            if line: # empty lines ignored
                yield line

    if filename is None:
        return iter(args)

    if filename == "-":
        return _read_lines(sys.stdin)

    with open(filename, encoding="utf-8") as file_:
        return _read_lines(file_)
