# ------------------------------------------------------------------------------
#
#  Base class for processes dealing with accumulated data (Level 2B/2C)
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

from collections import defaultdict
import logging

import numpy as np
from eoxserver.services.ows.wps.parameters import LiteralData

from aeolus.processes.util.bbox import translate_bbox
from aeolus.processes.util.base import ExtractionProcessBase
from aeolus.perf_util import ElapsedTimeLogger


logger = logging.getLogger(__name__)


class AccumulatedDataExctractProcessBase(ExtractionProcessBase):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level2C products of the specified collections.
    """

    # Override these values in the subclasses
    extraction_function = None
    level_name = None

    # common inputs/outputs to satisfy ProcessInterface
    inputs = ExtractionProcessBase.inputs + [
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

    def get_data_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        """ Overwritten function to get the exact data filters for L2B/C files
        """
        data_filters = dict(
            mie_profile_datetime_start={'min_value': begin_time},
            mie_profile_datetime_stop={'max_value': end_time},
            rayleigh_profile_datetime_start={'min_value': begin_time},
            rayleigh_profile_datetime_stop={'max_value': end_time},
            mie_wind_result_start_time={'min_value': begin_time},
            mie_wind_result_stop_time={'max_value': end_time},
            rayleigh_wind_result_start_time={'min_value': begin_time},
            rayleigh_wind_result_stop_time={'max_value': end_time},
            **(filters)
        )

        if bbox:
            tpl_box = translate_bbox(
                (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            )
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

        return data_filters

    def extract_data(self, collection_products, data_filters, mime_type, **kw):
        """ L2B/C extraction function
        """
        if kw['mie_grouping_fields']:
            mie_grouping_fields = kw['mie_grouping_fields'].split(',')
        else:
            mie_grouping_fields = []

        if kw['mie_profile_fields']:
            mie_profile_fields = kw['mie_profile_fields'].split(',')
        else:
            mie_profile_fields = []

        if kw['mie_wind_fields']:
            mie_wind_fields = kw['mie_wind_fields'].split(',')
        else:
            mie_wind_fields = []

        if kw['rayleigh_grouping_fields']:
            rayleigh_grouping_fields = kw['rayleigh_grouping_fields'].split(',')
        else:
            rayleigh_grouping_fields = []

        if kw['rayleigh_profile_fields']:
            rayleigh_profile_fields = kw['rayleigh_profile_fields'].split(',')
        else:
            rayleigh_profile_fields = []

        if kw['rayleigh_wind_fields']:
            rayleigh_wind_fields = kw['rayleigh_wind_fields'].split(',')
        else:
            rayleigh_wind_fields = []

        if kw['measurement_fields']:
            measurement_fields = kw['measurement_fields'].split(',')
        else:
            measurement_fields = []

        # create the iterator: yielding collection + sub iterators
        # the sub iterators iterate over all data files and yield the selected
        # and filtered fields
        return (
            (collection, self.extraction_function([
                (
                    band_data_item.location,
                    optimized_data_item.location if optimized_data_item else None
                )
                for band_data_item, optimized_data_item in (
                    (
                        product.data_items.filter(
                            semantic__startswith='bands'
                        ).first(),
                        product.data_items.filter(
                            semantic__startswith='optimized'
                        ).first(),

                    )
                    for product in products
                )
            ], data_filters,
                mie_grouping_fields=mie_grouping_fields,
                mie_profile_fields=mie_profile_fields,
                mie_wind_fields=mie_wind_fields,
                rayleigh_grouping_fields=rayleigh_grouping_fields,
                rayleigh_profile_fields=rayleigh_profile_fields,
                rayleigh_wind_fields=rayleigh_wind_fields,
                measurement_fields=measurement_fields,
                convert_arrays=(mime_type == 'application/msgpack'),
            ))
            for collection, products in collection_products
        )

    def accumulate_for_messagepack(self, out_data_iterator):
        out_data = {}
        for collection, data_iterator in out_data_iterator:
            accumulated_data = [
                defaultdict(list),
                defaultdict(list),
                defaultdict(list),
                defaultdict(list),
                defaultdict(list),
                defaultdict(list),
                defaultdict(list),
            ]

            for data_kinds in data_iterator:
                for data_kind, acc in zip(data_kinds, accumulated_data):
                    for field, values in data_kind.items():
                        acc[field].extend(values)

            collection_data = dict(
                mie_grouping_data=accumulated_data[0],
                rayleigh_grouping_data=accumulated_data[1],
                mie_profile_data=accumulated_data[2],
                rayleigh_profile_data=accumulated_data[3],
                mie_wind_data=accumulated_data[4],
                rayleigh_wind_data=accumulated_data[5],
                measurement_data=accumulated_data[6],
            )
            out_data[collection.identifier] = collection_data
        return out_data

    def write_product_data_to_netcdf(self, ds, file_data):
        file_data = dict(
            mie_grouping_data=file_data[0],
            rayleigh_grouping_data=file_data[1],
            mie_profile_data=file_data[2],
            rayleigh_profile_data=file_data[3],
            mie_wind_data=file_data[4],
            rayleigh_wind_data=file_data[5],
            measurement_data=file_data[6],
        )

        def _get_offset_for_kind(ds, kind_name):
            if kind_name in ds.groups:
                return ds.groups[kind_name].dimensions[kind_name].size
            return 0

        offsets = dict(
            (kind_name, _get_offset_for_kind(ds, kind_name))
            for kind_name in file_data.keys()
        )

        for kind_name, kind in file_data.items():
            if not kind:
                continue

            # get or create the group and a simple dimension
            # for the data kind
            group = ds.createGroup(kind_name)
            if kind_name not in group.dimensions:
                group.createDimension(kind_name, None)

            # iterate over the actual data from each kind
            for name, values in kind.items():
                # if the variable does not yet exist,
                # create it
                if name not in group.variables:
                    isscalar = (values[0].ndim == 0)
                    if not isscalar:
                        array_dim_size = values[0].shape[-1]
                        array_dim_name = "array_%d" % array_dim_size
                        if array_dim_name not in ds.dimensions:
                            ds.createDimension(array_dim_name, array_dim_size)
                        values = np.vstack(values)

                    with ElapsedTimeLogger("creating var %s" % name, logger):
                        var = group.createVariable(
                            name, '%s%i' % (
                                values.dtype.kind,
                                values.dtype.itemsize
                            ), (
                                kind_name,
                            ) if isscalar else (
                                kind_name, array_dim_name
                            )
                        )
                        var[:] = values
                # if the variable already exists, append
                # data to it
                else:
                    var = group[name]
                    end = offsets[kind_name] + values.shape[0]

                    with ElapsedTimeLogger("adding to var %s" % name, logger):
                        var[offsets[kind_name]:end] = values
