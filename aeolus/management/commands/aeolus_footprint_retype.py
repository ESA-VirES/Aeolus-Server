# ------------------------------------------------------------------------------
#
# Project: EOxServer <http://eoxserver.org>
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2020 EOX IT Services GmbH
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
from django.contrib.gis.geos import (
    MultiPolygon, MultiLineString
)

from eoxserver.resources.coverages.management.commands import CommandOutputMixIn
from eoxserver.resources.coverages.models import Collection, Product


class Command(CommandOutputMixIn, BaseCommand):
    help = (
        "Re-type the footprints of the Products to either a `Multi`-geometry "
        "or the reverse."
    )
    args = "[-c <collection> [-c <collection> ...]] [--all] [--single]"

    def add_arguments(self, parser):
        parser.add_argument("-c", "--collection", dest="collections",
            action="append", default=None,
            help=("Optional. Collection identifier.")
        )
        parser.add_argument("--all", dest="do_all",
            action="store_true", default=False,
            help=("Optional. Transform all Product footprints.")
        )

        parser.add_argument("--multi", dest="multi",
            action="store_true", default=True,
            help=(
                "Optional. Transform all single footprints to Multi-geometries."
            )
        )

        parser.add_argument("--single", dest="multi",
            action="store_false", default=True,
            help=(
                "Optional. Transform all Multi-geometries to single ones, if "
                "possible."
            )
        )

    @transaction.atomic()
    def handle(self, collections, do_all, multi,  *args, **kwargs):
        if not (do_all or collections):
            raise CommandError(
                'Either at least one collection or --all must be specified'
            )

        if collections:
            qs = Product.objects.filter(collections__in=[
                Collection.objects.get(identifier=collection)
                for collection in collections
            ])
        else:
            qs = Product.objects.all()

        # TODO: optimizations: only fetch products with the geometry types of
        # question

        if multi:
            for p in qs:
                if p.footprint.geom_type == 'Polygon':
                    p.footprint = MultiPolygon(p.footprint)
                    p.save()
                elif p.footprint.geom_type == 'LineString':
                    p.footprint = MultiLineString(p.footprint)
                    p.save()
        else:
            for p in qs:
                if (p.footprint.geom_type in ('MultiPolygon', 'MultiLineString')
                        and len(p.footprint) == 1):
                    p.footprint = p.footprint[0]
                    p.save()
