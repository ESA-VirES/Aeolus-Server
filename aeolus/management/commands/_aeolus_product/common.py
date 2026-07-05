#-------------------------------------------------------------------------------
#
# Aeolus product management - common utilities
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
# pylint: disable=abstract-method

from os.path import isfile
from django.db.models import Q
from django.core.exceptions import ObjectDoesNotExist
from .._common import Subcommand, time_spec


class ProductSelectionSubcommand(Subcommand):
    """ Aeolus product selection subcommand. """
    SELECT_RELATED = []
    PREFETCH_RELATED = []

    def add_arguments(self, parser):
        parser.add_argument("identifier", nargs="*")
        parser.add_argument(
            "-l", "--location",
            dest="location", action="append",
            help=(
                "Select product by the given location."
                "Multiple locations are allowed."
            )
        )
        parser.add_argument(
            "-c", "--collection",
            dest="collection", action="append",
            help=(
                "Optional filter on the collection. "
                "Multiple collections are allowed."
            )
        )
        parser.add_argument(
            "-t", "--type", "--product-type",
            dest="type", action="append",
            help=(
                "Optional filter on the product type. "
                "Multiple product types are allowed."
            )
        )
        parser.add_argument(
            "--after", type=time_spec, required=False,
            help="Select objects after the given date."
        )
        parser.add_argument(
            "--before", type=time_spec, required=False,
            help="Select objects before the given date."
        )
        parser.add_argument(
            "--created-after", type=time_spec, required=False,
            help="Select objects whose record has been created after the given date."
        )
        parser.add_argument(
            "--created-before", type=time_spec, required=False,
            help="Select objects whose record has been created before the given date."
        )
        parser.add_argument(
            "--updated-after", type=time_spec, required=False,
            help="Select objects whose record has been updated after the given date."
        )
        parser.add_argument(
            "--updated-before", type=time_spec, required=False,
            help="Select objects whose record has been updated before the given date."
        )
        parser.add_argument(
            "--invalid-only", dest="invalid_only", action="store_true",
            default=False, help="Select invalid products missing a data-file."
        )
        parser.add_argument(
            "--optimized", dest="list_optimized", action="store_true",
            default=False, help="Select optimized products."
        )
        parser.add_argument(
            "--not-optimized", dest="list_not_optimized", action="store_true",
            default=False, help="Select not optimized products."
        )
        parser.add_argument(
            "--optimized-invalid", dest="list_optimized_invalid",
            action="store_true", default=False,
            help="Select optimized products without an actual optimized file."
        )

    def select_products(self, query, **kwargs):
        """ Get matched products. """

        if self.SELECT_RELATED:
            query = query.select_related(*self.SELECT_RELATED)

        if self.PREFETCH_RELATED:
            query = query.prefetch_related(*self.PREFETCH_RELATED)

        query = self._select_products_by_id(query, **kwargs)

        collections = set(kwargs["collection"] or [])
        if collections:
            query = query.filter(collections__identifier__in=collections)

        product_types = set(kwargs["type"] or [])
        if product_types:
            query = query.filter(product_type__name__in=product_types)

        if kwargs["after"]:
            query = query.filter(begin_time__gte=kwargs["after"])

        if kwargs["before"]:
            query = query.filter(end_time__lt=kwargs["before"])

        if kwargs["created_after"]:
            query = query.filter(inserted__gte=kwargs["created_after"])

        if kwargs["created_before"]:
            query = query.filter(inserted__lt=kwargs["created_before"])

        if kwargs["updated_after"]:
            query = query.filter(updated__gte=kwargs["updated_after"])

        if kwargs["updated_before"]:
            query = query.filter(updated__lt=kwargs["updated_before"])

        if kwargs["location"]:
            query = query.filter(
                Q(product_data_items__location__in=kwargs["location"]) |
                Q(optimized_data_item__location_in=kwargs["location"])
            )

        if kwargs["list_optimized"] or kwargs["list_optimized_invalid"]:
            query = query.filter(optimized_data_item__isnull=False)

        if kwargs["list_not_optimized"]:
            query = query.filter(optimized_data_item__isnull=True)

        if kwargs["list_optimized_invalid"]:
            query = filter_invalid_optimized(query, self.logger)

        if kwargs["invalid_only"]:
            query = filter_invalid(query, self.logger)

        return query

    def _select_products_by_id(self, query, **kwargs):
        identifiers = set(kwargs["identifier"])
        if identifiers:
            query = query.filter(identifier__in=identifiers)
        return query


class ProductSelectionSubcommandProtected(ProductSelectionSubcommand):
    """ Aeolus product selection subcommand requiring --all if no id given. """

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-a", "--all", dest="select_all", action="store_true", default=False,
            help="Select all objects."
        )

    def _select_products_by_id(self, query, **kwargs):
        identifiers = set(kwargs['identifier'])
        if identifiers or not kwargs['select_all']:
            query = query.filter(identifier__in=identifiers)
            if not identifiers:
                self.warning(
                    "No identifier is specified and no object will be removed. "
                    "Use the --all option to remove all matched items."
                )
        return query


def filter_invalid(products, logger=None):
    """ Filter invalid products. """
    for product in products:
        if is_invalid(product, logger):
            yield product


def is_invalid(product, logger=None):
    """ Return true is products is invalid. """
    count = 0
    for data_item in product.product_data_items.all():
        count += 1
        location = data_item.location
        if not (location and isfile(location)):
            logger.warning(
                "Invalid product %s detected! File %s does not exist!",
                product.identifier, location
            )
            return True

    if count == 0:
        logger.warning(
            "Invalid product %s detected! No data item!", product.identifier
        )
        return True

    if count > 1:
        logger.warning(
            "Invalid product %s detected! Multiple data items!",
            product.identifier
        )
        return True

    return False


def filter_invalid_optimized(products, logger=None):
    """ Filter invalid optimized data files. """
    for product in products:
        if is_invalid_optimized(product, logger):
            yield product


def is_invalid_optimized(product, logger):
    """ Return true is optimized data file is invalid. """
    try:
        data_item = product.optimized_data_item
    except ObjectDoesNotExist:
        return False

    location = data_item.location
    if not (location and isfile(location)):
        logger.warning(
            "Invalid product %s optimization detected! File %s does not exist!",
            product.identifier, location
        )
        return True
    return False
