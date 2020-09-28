# ------------------------------------------------------------------------------
#
# Products management - create an optimized file for a given product
#
# Project: VirES-Aeolus
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2018 EOX IT Services GmbH
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
# ------------------------------------------------------------------------------

import os.path
import errno

from django.core.management.base import CommandError, BaseCommand
from django.conf import settings
from django.db import transaction
from eoxserver.resources.coverages.management.commands import (
    CommandOutputMixIn
)
from eoxserver.backends import models as backends
from eoxserver.resources.coverages.models import Product

from aeolus.models import OptimizedProductDataItem
from aeolus.optimize import create_optimized_file, OptimizationError


class Command(CommandOutputMixIn, BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "-i", "--identifier", "--coverage-id", dest="identifier",
            action="store", default=None,
            help=(
                "The identifier to create the optimized file for."
            )
        )
        parser.add_argument(
            "-r", "--refresh",
            action="store_const", const="refresh", dest="mode", default="create",
            help="Refresh the optimized file if it already exists."
        )
        parser.add_argument(
            "-d", "--delete",
            action="store_const", const="delete", dest="mode", default="create",
            help="Only delete the optimized file, if it exists"
        )
        parser.add_argument(
            "-l", "--link",
            action="store_const", const="link", dest="mode", default="create",
            help="Link an already existing optimized file to the product."
        )
        parser.add_argument(
            "-u", "--unlink",
            action="store_const", const="unlink", dest="mode", default="create",
            help=(
                "Unlink an optimized file: remove the reference in the "
                "database, but leave the optimized file on the disk."
            )
        )
        parser.add_argument(
            "-U", "--update",
            action="store_true", dest="update", default=False,
            help=(
                "Update an optimized file, if it already exists, instead of "
                "recreate it wholly anew. Only used for `--create`."
            )
        )
        parser.add_argument(
            "-o", "--output", "--output-file", dest="output_file", default=None,
            help=(
                "Specify an output file. By default, the `AEOLUS_OPTIMIZED_DIR` "
                "setting is used to generate the filename in the following way: "
                "$AEOLUS_OPTIMIZED_DIR/{product_type.name}/"
                "{product.identifier}.nc"
            )
        )

    help = """
    Create (or delete) an optimized file for a specific product file.
    """

    @transaction.atomic()
    def handle(self, identifier, mode, output_file, update, **kwargs):
        if not identifier:
            raise CommandError("Missing manadatory --identifier")
        try:
            product = Product.objects.get(identifier=identifier)
        except Product.DoesNotExist:
            raise CommandError("No such product '%s'" % identifier)

        self.verbosity = kwargs.get('verbosity', 1)

        try:
            data_item = product.optimized_data_item
        except OptimizedProductDataItem.DoesNotExist:
            data_item = None

        # get an output filename

        optimized_dir = getattr(settings, 'AEOLUS_OPTIMIZED_DIR', None)
        if not output_file and optimized_dir:
            output_file = os.path.join(
                optimized_dir, product.product_type.name, identifier + '.nc'
            )

        if not output_file:
            raise CommandError(
                "No output path specified and no AEOLUS_OPTIMIZED_DIR "
                "setting provided."
            )

        if mode in ("delete", "refresh"):
            if data_item:
                self.print_msg(
                    "Removing optimized file for product '%s'" % identifier
                )
                try:
                    os.remove(data_item.location)
                except OSError as e:
                    self.print_wrn(
                        "Failed to delete optimized file '%s', error was: %s"
                        % (data_item.location, e)
                    )
                data_item.delete()

            else:
                self.print_wrn(
                    "Product '%s' did not have an optimized file"
                    % identifier
                )

            if os.path.exists(output_file):
                self.print_msg(
                    "Deleting orphaned optimized file %s" % output_file
                )
                try:
                    os.remove(output_file)
                except OSError as e:
                    raise CommandError(
                        "Failed to delete optimized file '%s', error was: %s"
                        % (output_file, e)
                    )

        elif mode == "create":
            if not update:
                if data_item:
                    raise CommandError(
                        "Product '%s' already has an optimized file. Use "
                        "'--delete' to remove it or '--refresh' to re-generate "
                        "it." % identifier
                    )
                elif os.path.exists(output_file):
                    raise CommandError(
                        "Orphaned optimized file '%s' already exists file. Use "
                        "'--delete' to remove it or '--refresh' to re-generate "
                        "it." % output_file
                    )

        elif mode == "link":
            if not os.path.exists(output_file):
                raise CommandError(
                    "Cannot link optimized file to product '%s' as the file "
                    "'%s' does not exist" % (identifier, output_file)
                )

            # check if there is already a data item associated with the product
            if data_item:
                if data_item.location == output_file:
                    self.print_wrn(
                        "File '%s' is already registered for the product '%s'"
                        % (output_file, identifier)
                    )
                else:
                    raise CommandError(
                        "Cannot link optimized file to product '%s' as another "
                        "optimized file ('%s') is already registered for the "
                        "product" % (identifier, data_item.location)
                    )
            else:
                self.print_msg(
                    "Linking optimized file '%s' to product '%s'"
                    % (output_file, identifier)
                )
                self._link_optimized_file(product, output_file)

        elif mode == "unlink":
            if not data_item:
                raise CommandError(
                    "Cannot unlink optimized file: no optimized file was "
                    "registered for the product '%s'" % identifier
                )
            else:
                if os.path.exists(data_item.location):
                    self.print_msg(
                        "Unlinking optimized file '%s' from product '%s'"
                        % (data_item.location, identifier)
                    )
                else:
                    self.print_wrn(
                        "Removing reference to non-existing optimized file '%s' "
                        "from product '%s'" % (data_item.location, identifier)
                    )
                data_item.delete()

        if mode in ("create", "refresh"):
            # create the output directory structure

            try:
                os.makedirs(os.path.dirname(output_file))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise CommandError(
                        "Failed to create the output directory, error was: %s"
                        % e
                    )

            self._create_optimized_file(product, output_file, update)

    def _create_optimized_file(self, product, output_file, update):
        identifier = product.identifier
        product_type = product.product_type

        # get the filename for the data file

        try:
            input_file = product.product_data_items.get().location
        except backends.DataItem.DoesNotExist:
            raise CommandError(
                "No data file for product '%s' found" % product.identifier
            )

        self._link_optimized_file(product, output_file)

        try:
            group_fields = create_optimized_file(
                input_file, product_type.name, output_file, update
            )
            for group, field_name in group_fields:
                self.print_msg("Optimizing %s/%s" % (group, field_name), 2)

            self.print_msg(
                "Successfully generated optimized file '%s'" % output_file
            )
        except OptimizationError as e:
            raise CommandError(
                "Failed to create the optimized file for product '%s'. "
                "Error was: %s" % (identifier, e)
            )

    def _link_optimized_file(self, product, output_file):
        self.print_msg(
            "Creating data item for optimized file '%s'" % output_file,
            2
        )

        # create a data item for the optimized file
        data_item, _ = OptimizedProductDataItem.objects.get_or_create(
            product=product,
            location=output_file, format="application/netcdf"
        )
        data_item.full_clean()
        data_item.save()
