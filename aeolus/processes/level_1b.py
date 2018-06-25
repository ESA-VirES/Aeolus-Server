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
import tempfile
import os.path
from uuid import uuid4

from django.contrib.gis.geos import Polygon
from eoxserver.core import Component, implements
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from eoxserver.services.ows.wps.parameters import (
    ComplexData, FormatJSON, CDObject, BoundingBoxData, LiteralData,
    FormatBinaryRaw, CDFile
)
import msgpack
from netCDF4 import Dataset
import numpy as np

from aeolus import models
from aeolus.level_1b import extract_data
from aeolus.processes.util.bbox import translate_bbox


class Level1BExctract(Component):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level1B products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level1B"
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
        ("observation_fields", LiteralData(
            'observation_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("measurement_fields", LiteralData(
            'measurement_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
    ]

    outputs = [
        ("output", ComplexData(
            "output", title="",
            formats=[
                FormatBinaryRaw('application/msgpack'),
                FormatBinaryRaw('application/netcdf'),
            ],
        )),
    ]

    def execute(self, collection_ids, begin_time, end_time, bbox, filters,
                output, **kwargs):
        # input parsing
        if kwargs['observation_fields']:
            observation_fields = kwargs['observation_fields'].split(',')
        else:
            observation_fields = []

        if kwargs['measurement_fields']:
            measurement_fields = kwargs['measurement_fields'].split(',')
        else:
            measurement_fields = []

        # TODO: optimize this to make this in a single query
        collections = [
            models.ProductCollection.objects.get(identifier=identifier)
            for identifier in collection_ids.data
        ]

        db_filters = dict(
            begin_time__lte=end_time,
            end_time__gte=begin_time,
        )

        data_filters = dict(
            time={'min': begin_time, 'max': end_time},
            **(filters.data if filters else {})
        )

        if bbox:
            # TODO: assure that bbox is within -180,-90,180,90
            # TODO: when minlon > maxlon, make 2 bboxes
            tpl_box = (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            box = Polygon.from_bbox(tpl_box)

            db_filters['ground_path__intersects'] = box

            tbox = translate_bbox(tpl_box)
            data_filters['longitude_of_DEM_intersection'] = {
                'min': tbox[0],
                'max': tbox[2]
            }
            data_filters['latitude_of_DEM_intersection'] = {
                'min': tbox[1],
                'max': tbox[3]
            }

        mime_type = output['mime_type']

        out_data = {}
        for collection in collections:
            dbl_files = [
                product.data_items.filter(semantic__startswith='bands')
                .first().location
                for product in models.Product.objects.filter(
                    collections=collection,
                    **db_filters
                ).order_by('begin_time')
            ]

            observation_data, measurement_data, filenames = extract_data(
                dbl_files, data_filters,
                observation_fields, measurement_fields,
                simple_observation_filters=True,
                convert_arrays=(mime_type == 'application/msgpack')
            )

            out_data[collection.identifier] = (
                observation_data, measurement_data, filenames
            )

        # encode as messagepack
        if mime_type == 'application/msgpack':
            encoded = StringIO(
                msgpack.dumps({
                    key: value[0:2]
                    for key, value in out_data.items()
                })
            )

            return CDObject(
                encoded, filename="level_1B_data.mp", **output
            )
        elif mime_type == 'application/netcdf':
            outpath = os.path.join(tempfile.gettempdir(), uuid4().hex) + '.nc'
            with Dataset(outpath, "w", format="NETCDF4") as ds:
                for collection, data in out_data.items():
                    observation_data, measurement_data, files = data

                    if observation_data:
                        num_observations = len(observation_data.values()[0])
                    elif measurement_data:
                        num_observations = len(measurement_data.values()[0][0])

                    ds.createDimension('observation', num_observations)
                    ds.createDimension('measurements_per_observation', 30)
                    ds.createDimension('array', 25)

                    if observation_data:
                        ds.createGroup('observations')
                    if measurement_data:
                        ds.createGroup('measurements')

                    for field, data in observation_data.items():
                        isscalar = data[0].ndim == 0
                        data = np.hstack(data) if isscalar else np.vstack(data)
                        variable = ds.createVariable(
                            '/observations/%s' % field, '%s%i' % (
                                data.dtype.kind, data.dtype.itemsize
                            ), (
                                'observation'
                            ) if isscalar else (
                                'observation', 'array',
                            )
                        )
                        variable[:] = data

                    for field, data in measurement_data.items():
                        isscalar = data[0][0][0].ndim == 0

                        if isscalar:
                            data = np.vstack(np.hstack(data))
                        else:
                            data = [
                                np.vstack([
                                    np.vstack(o)
                                    for o in f
                                ])
                                for f in data
                            ]

                        variable = ds.createVariable(
                            '/measurements/%s' % field, '%s%i' % (
                                data[0].dtype.kind, data[0].dtype.itemsize
                            ), (
                                'observation', 'measurements_per_observation'
                            ) if isscalar else (
                                'observation',
                                'measurements_per_observation',
                                'array',
                            )
                        )
                        variable[:] = data

            return CDFile(
                outpath, filename='level_1B_data.nc',
                remove_file=True, **output
            )
