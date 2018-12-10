# ------------------------------------------------------------------------------
#
#  Data extraction process for AUX MET products
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

from eoxserver.core import Component, implements
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from eoxserver.services.ows.wps.parameters import LiteralData
import numpy as np

from aeolus.aux_met import extract_data
from aeolus.processes.util.bbox import translate_bbox
from aeolus.processes.util.base import ExtractionProcessBase


class AUXMET12Extract(ExtractionProcessBase, Component):
    """ This process extracts data from the ADM-Aeolus
        Level1B AUX MET products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:AUX:MET"
    metadata = {}
    profiles = ["vires-util"]

    range_type_name = "AUX_MET_12"

    inputs = ExtractionProcessBase.inputs + [
        ("fields", LiteralData(
            'fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
    ]

    def get_data_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        data_filters = dict(
            time_off_nadir={
                "min": begin_time,
                "max": end_time,
            },
            time_nadir={
                "min": begin_time,
                "max": end_time,
            },
            **filters
        )

        if bbox:
            tpl_box = translate_bbox(
                (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            )

            data_filters['longitude_off_nadir'] = {
                'min': tpl_box[0],
                'max': tpl_box[2]
            }
            data_filters['longitude_nadir'] = {
                'min': tpl_box[0],
                'max': tpl_box[2]
            }
            data_filters['latitude_off_nadir'] = {
                'min': tpl_box[1],
                'max': tpl_box[3]
            }
            data_filters['latitude_nadir'] = {
                'min': tpl_box[1],
                'max': tpl_box[3]
            }

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
                convert_arrays=(mime_type == 'application/msgpack'),
            ))
            for collection, products in collection_products
        )

    def accumulate_for_messagepack(self, out_data_iterator):
        out_data = {}
        for collection, data_iterator in out_data_iterator:
            accumulated_data = defaultdict(list)
            for type_name, data in data_iterator:
                file_data = dict(**data)
                for field_name, values in file_data.items():
                    accumulated_data[field_name].extend(values)

            out_data[collection.identifier] = accumulated_data

        return out_data

    def write_product_data_to_netcdf(self, ds, file_data):
        type_name, full_data = file_data
        if type_name not in ds.dimensions:
            ds.createDimension(type_name, None)
            num_records = 0
        else:
            num_records = ds.dimensions[type_name].size

        for field_name, data in full_data.items():
            isscalar = (isinstance(data[0], str) or data[0].ndim == 0)
            arrsize = data[0].shape[0] if not isscalar else 0
            array_dim = 'array_%d' % arrsize
            data = np.hstack(data) if isscalar else np.vstack(data)

            if arrsize and array_dim not in ds.dimensions:
                ds.createDimension(array_dim, arrsize)

            # create new variable (+ dimensions)
            if field_name not in ds.variables:
                data_type_name = '%s%i' % (data.dtype.kind, data.dtype.itemsize)
                dims = (type_name) if isscalar else (type_name, array_dim)
                ds.createVariable(field_name, data_type_name, dims)[:] = data

            # append to existing variable
            else:
                var = ds[field_name]
                end = num_records + data.shape[0]
                var[num_records:end] = data
