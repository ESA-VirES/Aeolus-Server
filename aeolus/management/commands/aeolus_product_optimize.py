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
from optparse import make_option

from django.core.management.base import CommandError, BaseCommand
from django.conf import settings
from django.db import transaction
from eoxserver.resources.coverages.management.commands import (
    CommandOutputMixIn
)
from eoxserver.backends import models as backends

from aeolus.models import Product
from aeolus.optimize import create_optimized_file, OptimizationError


class Command(CommandOutputMixIn, BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option(
            "-i", "--identifier", "--coverage-id", dest="identifier",
            action="store", default=None,
            help=(
                "The identifier to create the optimised cache for."
            )
        ),
        make_option(
            "-r", "--refresh",
            action="store_const", const="refresh", dest="mode", default="create",
            help="Refresh the optimised image if it already exists."
        ),
        make_option(
            "-d", "--delete",
            action="store_const", const="delete", dest="mode", default="create",
            help="Only delete the optimised image, if it exists"
        ),
        make_option(
            "-o", "--output", default=None,
            help="Speficy an output file."
        )
    )

    help = """
    Create (or delete) an optimized file for a specific product file.
    """

    @transaction.atomic
    def handle(self, identifier, mode, output, **kwargs):
        if not identifier:
            raise CommandError("Missing manadatory --identifier")
        try:
            product = Product.objects.get(identifier=identifier)
        except Product.DoesNotExist:
            raise CommandError("No such product '%s'" % identifier)

        try:
            data_item = product.data_items.get(
                semantic__startswith="optimized"
            )
        except backends.DataItem.DoesNotExist:
            data_item = None

        if mode in ("delete", "refresh"):
            if data_item:
                self.print_msg(
                    "Removing optimized file for product '%s'" % identifier
                )
                try:
                    os.remove(data_item.location)
                except OSError as e:
                    raise CommandError(
                        "Failed to delete optimized file '%s', error was: %s"
                        % (data_item.location, e)
                    )
                data_item.delete()
            else:
                self.print_wrn(
                    "Product '%s' did not have an optimized file"
                    % identifier
                )

        elif mode == "create":
            if data_item:
                raise CommandError(
                    "Product '%s' already has an optimized file. Used "
                    "'--delete' to remove it or '--refresh' to re-generate it."
                    % identifier
                )

        if mode in ("create", "refresh"):
            self._create_optimized_file(product, output)

    def _create_optimized_file(self, product, output_file):
        identifier = product.identifier
        range_type = product.range_type

        # get an output filename

        optimized_dir = getattr(settings, 'AEOLUS_OPTIMIZED_DIR', None)
        if not output_file and optimized_dir:
            output_file = os.path.join(
                optimized_dir, range_type.name, identifier + '.nc'
            )

        if not output_file:
            raise CommandError(
                "No output path specified and no AEOLUS_OPTIMIZED_DIR "
                "setting provided."
            )

        # create the output directory structure

        try:
            os.makedirs(os.path.dirname(output_file))
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise CommandError(
                    "Failed to create the output directory, error was: %s" % e
                )

        # get the filename for the data file

        try:
            input_file = product.data_items.get(
                semantic__startswith="bands"
            ).location
        except backends.DataItem.DoesNotExist:
            raise CommandError(
                "No data file for product '%s' found" % product.identifier
            )

        # create a data item for the optimized file

        data_item = backends.DataItem(
            location=output_file, format="application/netcdf",
            semantic="optimized[1:%d]" % len(range_type),
            storage=None, package=None
        )
        data_item.dataset = product
        data_item.full_clean()
        data_item.save()

        try:
            create_optimized_file(input_file, range_type.name, output_file)
            self.print_msg(
                "Successfully generated optimized file '%s'" % output_file
            )
        except OptimizationError as e:
            raise CommandError(
                "Failed to create the optimized file for product '%s'. "
                "Error was: %s" % (identifier, e)
            )
