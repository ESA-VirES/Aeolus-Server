# ------------------------------------------------------------------------------
#
#  Data extraction from Level 1B AUX ADM-Aeolus products
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

from collections import defaultdict

from eoxserver.core import Component, implements
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from eoxserver.services.ows.wps.parameters import LiteralData
import numpy as np

from aeolus.aux import extract_data
from aeolus.processes.util.bbox import translate_bbox
from aeolus.processes.util.base import ExtractionProcessBase


class Level1BAUXExtractBase(ExtractionProcessBase):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level1B products of the specified collections.
    """

    inputs = ExtractionProcessBase.inputs + [
        ("fields", LiteralData(
            'fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
    ]

    aux_type = None

    def get_data_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        if bbox:
            tpl_box = translate_bbox(
                (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            )

        if self.aux_type in ("MRC", "RRC"):
            data_filters = dict(
                time_freq_step={
                    "min": begin_time,
                    "max": end_time,
                },
                **filters
            )
            if bbox:
                data_filters['lon_of_DEM_intersection'] = {
                    'min': tpl_box[0],
                    'max': tpl_box[2]
                }
                data_filters['lat_of_DEM_intersection'] = {
                    'min': tpl_box[1],
                    'max': tpl_box[3]
                }
            return data_filters

        elif self.aux_type in ("ISR", "ZWC"):
            data_filters = dict(
                time={
                    "min": begin_time,
                    "max": end_time,
                },
                **filters
            )
            if bbox and self.aux_type == "ZWC":
                data_filters['lon_of_DEM_intersection'] = {
                    'min': tpl_box[0],
                    'max': tpl_box[2]
                }
                data_filters['lat_of_DEM_intersection'] = {
                    'min': tpl_box[1],
                    'max': tpl_box[3]
                }
            return data_filters

        return filters

    def extract_data(self, collection_products, data_filters, fields, mime_type,
                     **kwargs):
        return (
            (collection, extract_data([
                product.data_items.filter(semantic__startswith='bands')
                .first().location
                for product in products
            ],
                data_filters,
                fields.split(',') if fields else [],
                self.aux_type,
                convert_arrays=(mime_type == 'application/msgpack'),
            ))
            for collection, products in collection_products
        )

    def accumulate_for_messagepack(self, out_data_iterator):
        out_data = {}
        for collection, data_iterator in out_data_iterator:
            accumulated_data = defaultdict(list)
            for calibration_data, frequency_data in data_iterator:
                file_data = dict(**calibration_data)
                file_data.update(frequency_data)
                for field_name, values in file_data.items():
                    accumulated_data[field_name].extend(values)

            out_data[collection.identifier] = accumulated_data

        return out_data

    def write_product_data_to_netcdf(self, ds, file_data):
        calibration_data, frequency_data = file_data
        if 'calibration' not in ds.dimensions:
            ds.createDimension('calibration', None)
            num_calibrations = 0
        else:
            num_calibrations = ds.dimensions['calibration'].size

        if 'frequency' not in ds.dimensions:
            ds.createDimension('frequency', None)
            num_frequencies = 0
        else:
            num_frequencies = ds.dimensions['frequency'].size

        for field_name, data in calibration_data.items():
            group = ds.createGroup('calibration_data')
            # TODO: better scalar check
            isscalar = (isinstance(data[0], str) or data[0].ndim == 0)
            arrsize = data[0].shape[0] if not isscalar else 0
            array_dim = 'array_%d' % arrsize
            data = np.hstack(data) if isscalar else np.vstack(data)

            if arrsize and array_dim not in ds.dimensions:
                ds.createDimension(array_dim, arrsize)

            # create new variable (+ dimensions)
            if field_name not in group.variables:
                group.createVariable(
                    field_name, '%s%i' % (
                        data.dtype.kind, data.dtype.itemsize
                    ),
                    ('calibration') if isscalar else ('calibration', array_dim)
                )[:] = data

            # append to existing variable
            else:
                var = group[field_name]
                end = num_calibrations + data.shape[0]
                var[num_calibrations:end] = data

        for field_name, data in frequency_data.items():
            group = ds.createGroup('frequency_data')
            # TODO: better scalar check
            isscalar = (isinstance(data[0][0], str) or data[0][0].ndim == 0)
            arrsize = data[0][0].shape[0] if not isscalar else 0
            array_dim = 'array_%d' % arrsize

            dtype = data[0][0].dtype
            if not isscalar:
                data = [
                    np.vstack(item) for item in data
                ]

            data = np.hstack(data)

            if arrsize and array_dim not in ds.dimensions:
                ds.createDimension(array_dim, arrsize)

            # create new variable (+ dimensions)
            if field_name not in group.variables:
                var = group.createVariable(
                    field_name, '%s%i' % (
                        dtype.kind, dtype.itemsize
                    ),
                    ('frequency') if isscalar else ('frequency', array_dim)
                )
                var[:] = data

            # append to existing variable
            else:
                var = group[field_name]
                end = num_frequencies + data.shape[0]
                var[num_frequencies:end] = data


class Level1BAUXISRExtract(Level1BAUXExtractBase, Component):
    """ This process extracts data from the ADM-Aeolus
        Level1B AUX_ISR products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level1B:AUX:ISR"
    metadata = {}
    profiles = ["vires-util"]

    range_type_name = "AUX_ISR"
    aux_type = "ISR"


class Level1BAUXMRCExtract(Level1BAUXExtractBase, Component):
    """ This process extracts data from the ADM-Aeolus
        Level1B AUX_MRC products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level1B:AUX:MRC"
    metadata = {}
    profiles = ["vires-util"]

    range_type_name = "AUX_MRC"
    aux_type = "MRC"


class Level1BAUXRRCExtract(Level1BAUXExtractBase, Component):
    """ This process extracts data from the ADM-Aeolus
        Level1B AUX_RRC products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level1B:AUX:RRC"
    metadata = {}
    profiles = ["vires-util"]

    range_type_name = "AUX_RRC"
    aux_type = "RRC"


class Level1BAUXZWCExtract(Level1BAUXExtractBase, Component):
    """ This process extracts data from the ADM-Aeolus
        Level1B AUX_ZWC products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level1B:AUX:ZWC"
    metadata = {}
    profiles = ["vires-util"]

    range_type_name = "AUX_ZWC"
    aux_type = "ZWC"
