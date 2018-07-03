# ------------------------------------------------------------------------------
#
#  Data extraction from Level 2C ADM-Aeolus products
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

from aeolus import models
from aeolus.level_2c import extract_data
from aeolus.processes.util.bbox import translate_bbox


class Level2CExctract(Component):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level2C products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level2C"
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
        ("mie_grouping_fields", LiteralData(
            'mie_grouping_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("mie_profile_fields", LiteralData(
            'mie_profile_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("mie_wind_fields", LiteralData(
            'mie_wind_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("rayleigh_grouping_fields", LiteralData(
            'rayleigh_grouping_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("rayleigh_profile_fields", LiteralData(
            'rayleigh_profile_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("rayleigh_wind_fields", LiteralData(
            'rayleigh_wind_fields', str, optional=True, default=None,
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
        if kwargs['mie_grouping_fields']:
            mie_grouping_fields = kwargs['mie_grouping_fields'].split(',')
        else:
            mie_grouping_fields = []

        if kwargs['mie_profile_fields']:
            mie_profile_fields = kwargs['mie_profile_fields'].split(',')
        else:
            mie_profile_fields = []

        if kwargs['mie_wind_fields']:
            mie_wind_fields = kwargs['mie_wind_fields'].split(',')
        else:
            mie_wind_fields = []

        if kwargs['rayleigh_grouping_fields']:
            rayleigh_grouping_fields = kwargs['rayleigh_grouping_fields'].split(',')
        else:
            rayleigh_grouping_fields = []

        if kwargs['rayleigh_profile_fields']:
            rayleigh_profile_fields = kwargs['rayleigh_profile_fields'].split(',')
        else:
            rayleigh_profile_fields = []

        if kwargs['rayleigh_wind_fields']:
            rayleigh_wind_fields = kwargs['rayleigh_wind_fields'].split(',')
        else:
            rayleigh_wind_fields = []

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
            mie_profile_datetime_start={'min_value': begin_time},
            mie_profile_datetime_stop={'max_value': end_time},
            rayleigh_profile_datetime_start={'min_value': begin_time},
            rayleigh_profile_datetime_stop={'max_value': end_time},
            mie_wind_result_start_time={'min_value': begin_time},
            mie_wind_result_stop_time={'max_value': end_time},
            rayleigh_wind_result_start_time={'min_value': begin_time},
            rayleigh_wind_result_stop_time={'max_value': end_time},
            **(filters.data if filters else {})
        )

        if bbox:
            # TODO: assure that bbox is within -180,-90,180,90
            # TODO: when minlon > maxlon, make 2 bboxes
            tpl_box = (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            box = Polygon.from_bbox(tpl_box)

            db_filters['ground_path__intersects'] = box

            tpl_box = translate_bbox(tpl_box)
            data_filters['mie_profile_lon_of_DEM_intersection'] = {
                'min': tpl_box[0],
                'max': tpl_box[2],
            }
            data_filters['mie_profile_lat_of_DEM_intersection'] = {
                'min': tpl_box[1],
                'max': tpl_box[3],
            }
            data_filters['rayleigh_profile_lon_of_DEM_intersection'] = {
                'min': tpl_box[0],
                'max': tpl_box[2],
            }
            data_filters['rayleigh_profile_lat_of_DEM_intersection'] = {
                'min': tpl_box[1],
                'max': tpl_box[3],
            }
            data_filters['mie_wind_result_COG_longitude'] = {
                'min': tpl_box[0],
                'max': tpl_box[2],
            }
            data_filters['mie_wind_result_COG_latitude'] = {
                'min': tpl_box[1],
                'max': tpl_box[3],
            }
            data_filters['rayleigh_wind_result_COG_longitude'] = {
                'min': tpl_box[0],
                'max': tpl_box[2],
            }
            data_filters['rayleigh_wind_result_COG_latitude'] = {
                'min': tpl_box[1],
                'max': tpl_box[3],
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

            (
                mie_grouping_data,
                rayleigh_grouping_data,
                mie_profile_data,
                rayleigh_profile_data,
                mie_wind_data,
                rayleigh_wind_data,
                measurement_data,
            ) = extract_data(
                dbl_files, data_filters,
                mie_grouping_fields=mie_grouping_fields,
                mie_profile_fields=mie_profile_fields,
                mie_wind_fields=mie_wind_fields,
                rayleigh_grouping_fields=rayleigh_grouping_fields,
                rayleigh_profile_fields=rayleigh_profile_fields,
                rayleigh_wind_fields=rayleigh_wind_fields,
                measurement_fields=measurement_fields,
            )

            out_data[collection.identifier] = dict(
                mie_grouping_data=mie_grouping_data,
                rayleigh_grouping_data=rayleigh_grouping_data,
                mie_profile_data=mie_profile_data,
                rayleigh_profile_data=rayleigh_profile_data,
                mie_wind_data=mie_wind_data,
                rayleigh_wind_data=rayleigh_wind_data,
                measurement_data=measurement_data,
            )

        # encode as messagepack
        if mime_type == 'application/msgpack':
            encoded = StringIO(msgpack.dumps(out_data))

            return CDObject(
                encoded, filename="level_2C_data.mp", **output
            )

        elif mime_type == 'application/netcdf':
            outpath = os.path.join(tempfile.gettempdir(), uuid4().hex) + '.nc'

            try:
                with Dataset(outpath, "w", format="NETCDF4") as ds:
                    for collection, collection_data in out_data.items():
                        for kind_name, kind in collection_data.items():
                            if not kind:
                                continue

                            # get or create the group and a simple dimension
                            # for the data kind
                            group = ds.createGroup(kind_name)
                            group.createDimension(kind_name, None)

                            # iterate over the actual data from each kind
                            for name, values in kind.items():
                                group.createVariable(
                                    name, 'f8', kind_name
                                )[:] = values

            except:
                os.remove(outpath)
                raise

            return CDFile(
                outpath, filename="level_2C_data.nc",
                remove_file=True, **output
            )
