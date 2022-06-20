# ------------------------------------------------------------------------------
#
# Project: EOxServer <http://eoxserver.org>
# Authors: Daniel Santillan <daniel.santillan@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2022 EOX IT Services GmbH
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

from aeolus.registration import get_dbl_metadata, get_eef_metadata
from aeolus import models
from aeolus.coda_utils import CODAFile
from aeolus import aux

class Command(CommandOutputMixIn, BaseCommand):
    help = (
        "Refresh saved footprints from products of a collection."
    )
    args = "[-c <collection> [-c <collection> ...]]"

    def add_arguments(self, parser):
        parser.add_argument("-c", "--collection", dest="collections",
            action="append", default=None,
            help=("Collection identifier.")
        )
        parser.add_argument(
            "--simplify",
            type=float, default=0.2, dest="simplification_tolerance",
            help=(
                "Footprint simplification tolerance. See "
                "https://docs.djangoproject.com/en/2.2/ref/contrib/gis/geos/"
                "#django.contrib.gis.geos.GEOSGeometry.simplify for details. "
                "By default 0.2 simplification is performed."
            )
        )

    @transaction.atomic()
    def handle(self, collections, simplification_tolerance, *args, **kwargs):
        if collections:
            qs = Product.objects.filter(collections__in=[
                Collection.objects.get(identifier=collection)
                for collection in collections
            ])
       
            for p in qs:
                codafile = CODAFile(p.product_data_items.first().location)
                assert codafile.product_class == 'AEOLUS'
                product_type = codafile.product_type

                if product_type.startswith('ALD'):
                    metadata = get_dbl_metadata(codafile)
                elif product_type.startswith('AUX'):
                    metadata = get_eef_metadata(codafile)
                else:
                    raise AssertionError('Unsupported product type %r' % product_type)

                if simplification_tolerance is not None:
                    footprint = metadata.get('footprint')
                    if footprint:
                        simplified = footprint.simplify(
                            simplification_tolerance
                        )

                        # simplify reduces "Multi"-Geometries to simple ones.
                        # force the same geometry type as the original footprint
                        if simplified.geom_type != footprint.geom_type:
                            simplified = type(footprint)(simplified)

                        metadata['footprint'] = simplified
                
                p.footprint = metadata['footprint']
                p.save()

                self.print_msg(
                    "Succesfully updated footpint of %s"
                    % (p.identifier)
                )



