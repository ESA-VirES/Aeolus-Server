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
import logging
from os.path import splitext, basename
from traceback import print_exc
from dataclasses import dataclass
from typing import List
from django.db import transaction
from eoxserver.resources.coverages.models import (
    Collection, Product, collection_insert_eo_object,
)
from aeolus.registration import register_product as register_aeolus_product
from .._common import Subcommand

DEF_SIMPLIFICATION_TOLERANCE = 0.2


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

    def handle(self, **kwargs):
        data_files = kwargs["product-file"]
        update_existing = not kwargs["ignore_registered"]
        collection_id = kwargs["collection_id"]
        simplification_tolerance = (
            kwargs["simplification_tolerance"] if kwargs["simplify"] else None
        )

        collection = get_collection(collection_id)

        counter = Counter()

        for data_file in read_products(kwargs["input_file"], data_files):
            product_id = get_product_id(data_file)
            try:
                result = register_product(
                    collection, product_id, data_file,
                    update_existing=update_existing,
                    simplification_tolerance=simplification_tolerance,
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

        counter.print_report(lambda msg: print(msg, file=sys.stderr))

        sys.exit(counter.failed > 0)


@dataclass
class Result:
    product: Product
    created: bool
    removed: List[str] # < Python 3.9
    linked_to_collection: List[str] # < Python 3.9


def register_product(
    collection, identifier, filename,
    update_existing=False,
    simplification_tolerance=DEF_SIMPLIFICATION_TOLERANCE,
    logger=None
):
    if not logger:
        logger = logging.getLogger(__name__)

    result = _register_product(
        collection=collection,
        identifier=identifier,
        filename=filename,
        update_existing=update_existing,
        simplification_tolerance=simplification_tolerance,
    )

    for product_id in result.removed:
        logger.info("product %s deregistered", product_id)

    if result.created:
        logger.info("product %s registered", identifier)
    else:
        logger.debug("product %s exists", identifier)

    for collection_id in result.linked_to_collection:
        logger.info(
            "product %s linked to collection %s",
            identifier, collection_id,
        )

    return result


@transaction.atomic
def _register_product(
    collection, identifier, filename,
    update_existing=False,
    simplification_tolerance=DEF_SIMPLIFICATION_TOLERANCE,
):
    created = False
    removed = []
    linked_to_collection = []

    if update_existing:
        if _remove_existing_product(identifier):
            removed.append(identifier)
        product = None
    else:
        product = _get_existing_product(identifier)

    if not product:
        product = register_aeolus_product(
            filename, overrides={"identifier": identifier},
            footprint_simplification_tolerance=simplification_tolerance
        )
        created = True

    if not product.collections.filter(pk=collection.pk).exists():
        collection_insert_eo_object(collection, product)
        linked_to_collection.append(collection.identifier)

    return Result(
        product=product,
        created=created,
        removed=removed,
        linked_to_collection=linked_to_collection,
    )


def _remove_existing_product(identifier):
    count, _ = Product.objects.filter(identifier=identifier).delete()
    return count > 0


def _get_existing_product(identifier):
    try:
        return Product.objects.get(identifier=identifier)
    except Product.DoesNotExist:
        return None


def get_collection(identifier):
    try:
        return Collection.objects.get(identifier=identifier)
    except Collection.DoesNotExist:
        raise ValueError("Invalid collection identifier!") from None


def get_product_id(filename):
    """ Get the product identifier. """
    return splitext(basename(filename))[0]


class Counter():

    def __init__(self):
        self.total = 0
        self.inserted = 0
        #self.updated = 0
        self.removed = 0
        self.skipped = 0
        self.failed = 0

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
