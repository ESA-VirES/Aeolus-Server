# ------------------------------------------------------------------------------
#
# Registration of Albedo
#
# Project: VirES
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

from django.core.management.base import CommandError, BaseCommand
from django.db import transaction
from eoxserver.resources.coverages import models
from eoxserver.resources.coverages.management.commands import CommandOutputMixIn

from aeolus.registration import register_albedo


class Command(CommandOutputMixIn, BaseCommand):

    help = (
        "Register a single Albedo file for a specific year/month."
    )
    args = "<filename>"

    def add_arguments(self, parser):
        parser.add_argument(
            "-f", "--file", dest="input_file", default=None,
            help=(
                "Path to the input filename."
            )
        )

        parser.add_argument(
            '-y', '--year', type=int, default=None,
            help='The year to register the Albedo map for.'
        )

        parser.add_argument(
            '-r', '--year-range', default=None, dest='year_range',
            help='The year-range to register the Albedo map for. E.g: 2012-2018'
        )

        parser.add_argument(
            '-m', '--month', type=int, default=None,
            help='The month to register the Albedo map for.'
        )

        parser.add_argument(
            "-c", "--collection", dest="collection_id", default=None,
            help="Optional collection the product should be linked to."
        )
        parser.add_argument(
            "--conflict", dest="conflict", choices=("IGNORE", "REPLACE"),
            default="IGNORE", help=(
                "Define how to resolve conflict when the product is already "
                "registered. By default the registration is skipped and the "
                "the passed product IGNORED. An alternative is to REPLACE the "
                "old product (remove the old one and insert the new one). "
                "In case of the REPLACE the collection links are NOT preserved."
            )
        )

    @transaction.atomic()
    def handle(self, input_file, month, year, year_range,
               collection_id, conflict, **kwargs):

        if year is not None:
            year_range = [year]
        elif year_range is not None:
            low, high = [int(v) for v in year_range.split('-')]
            year_range = range(low, high + 1)
        else:
            raise CommandError('Neither --year nor --year-range specified')

        for year in year_range:
            try:
                ds_model = register_albedo(
                    input_file, year, month, conflict == 'REPLACE'
                )
            except Exception as e:
                raise CommandError(str(e))

            self.print_msg(
                "Succesfully registered Albedo file %s for %d/%d"
                % (input_file, year, month)
            )

            if collection_id:
                try:
                    collection = models.Collection.objects.get(
                        identifier=collection_id
                    )

                    models.collection_insert_eo_object(collection, ds_model)
                    self.print_msg(
                        "Succesfully inserted dataset in collection '%s'"
                        % collection_id
                    )
                except Exception as e:
                    self.print_err(
                        "Failed to insert dataset in collection '%s'. "
                        "Error was: %s"
                        % (collection_id, e)
                    )
