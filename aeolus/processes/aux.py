# ------------------------------------------------------------------------------
#
#  Data extraction from Level 1B ADM-Aeolus products
#
# Project: VirES-Aeolus
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2017 EOX IT Services GmbH
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

from datetime import datetime
from cStringIO import StringIO

from django.contrib.gis.geos import Polygon
from eoxserver.core import Component, implements
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from eoxserver.services.ows.wps.parameters import (
    RequestParameter, ComplexData, FormatJSON, CDObject,
    BoundingBoxData, LiteralData, FormatBinaryBase64, FormatBinaryRaw
)
import msgpack

from aeolus import models
from aeolus.aux import extract_data


class Level1BAUXExctract(Component):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level1B products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level1B:AUX"
    metadata = {}
    profiles = ["vires-util"]

    inputs = [
        ("collection_ids", ComplexData(
            'collection_ids', title="Collection identifiers", abstract=(
                ""
            ), formats=FormatJSON()
        )),
        ("begin_time", LiteralData(
            'begin_time', datetime, optional=False, title="Begin time",
            abstract="Start of the selection time interval",
        )),
        ("end_time", LiteralData(
            'end_time', datetime, optional=False, title="End time",
            abstract="End of the selection time interval",
        )),
        ("bbox", BoundingBoxData(
            "bbox", crss=(4326, 3857), optional=True, title="Bounding box",
            abstract="Optional selection bounding box.", default=None,
        )),
        ("filters", ComplexData(
            'filters', title="Filters", abstract=(
                "JSON Object to set specific data filters."
            ), formats=FormatJSON(), optional=True
        )),
        ("fields", LiteralData(
            'fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("aux_type", LiteralData(
            'aux_type', str, optional=True, default=None,
            title="AUX type to query",
            abstract="The AUX type to query data from."
        )),
    ]

    outputs = [
        ("output", ComplexData(
            "output", title="",
            formats=[FormatBinaryRaw()],
        )),
    ]

    def execute(self, collection_ids, begin_time, end_time, bbox, filters,
                fields, aux_type, output, **kwargs):
        # input parsing
        if fields:
            fields = fields.split(',')
        else:
            fields = []

        # TODO: optimize this to make this in a single query
        collections = [
            models.ProductCollection.objects.get(identifier=identifier)
            for identifier in collection_ids.data
        ]

        db_filters = dict(
            begin_time__lte=end_time,
            end_time__gte=begin_time,
        )

        if bbox:
            box = Polygon.from_bbox(
                (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            )

            db_filters['ground_path__intersects'] = box

        data_filters = filters.data or {}

        output = {}
        for collection in collections:
            dbl_files = [
                product.data_items.filter(semantic__startswith='bands')
                .first().location
                for product in models.Product.objects.filter(
                    collections=collection,
                    **db_filters
                ).order_by('begin_time')
            ]
            output[collection.identifier] = extract_data(
                dbl_files, data_filters, fields, aux_type
            )

        # encode as messagepack
        encoded = StringIO(msgpack.dumps(output))

        return CDObject(
            encoded, filename="aux_data.mp", **output
        )
