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
from aeolus.processes.util.base import ExtractionProcessBase


class Level1BAUXExctract(ExtractionProcessBase, Component):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level1B products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level1B:AUX"
    metadata = {}
    profiles = ["vires-util"]

    inputs = ExtractionProcessBase.inputs + [
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

    def get_data_filters(self, begin_time, end_time, bbox, filters, aux_type,
                         **kwargs):
        # TODO: use bbox too?
        if aux_type in ("MRC", "RRC"):
            return dict(
                time_freq_step={
                    "min": begin_time,
                    "max": end_time,
                },
                **filters
            )
        elif aux_type in ("ISR", "ZWC"):
            return dict(
                time={
                    "min": begin_time,
                    "max": end_time,
                },
                **filters
            )
        return filters

    def extract_data(self, collection_products, data_filters, fields, aux_type,
                     mime_type, **kw):
        return (
            (collection, extract_data([
                product.data_items.filter(semantic__startswith='bands')
                .first().location
                for product in products
            ],
                data_filters, fields.split(','), aux_type,
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
        if 'frequency' not in ds.dimensions:
            ds.createDimension('frequency', None)

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
            if field_name not in group.dimensions:
                group.createVariable(
                    field_name, '%s%i' % (
                        data.dtype.kind, data.dtype.itemsize
                    ),
                    ('calibration') if isscalar else ('calibration', array_dim)
                )[:] = data

            # append to existing variable
            else:
                var = group[field_name]
                offset = var.shape[0]
                end = offset + data.shape[0]
                var[offset:end] = data

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

            # print data.shape, data.dtype

            if arrsize and array_dim not in ds.dimensions:
                ds.createDimension(array_dim, arrsize)

            # create new variable (+ dimensions)
            if field_name not in group.dimensions:
                group.createVariable(
                    field_name, '%s%i' % (
                        dtype.kind, dtype.itemsize
                    ),
                    (
                        'calibration', 'frequency'
                    ) if isscalar else (
                        'calibration', 'frequency', array_dim
                    )
                )[:] = data

            # append to existing variable
            else:
                var = group[field_name]
                offset = var.shape[0]
                end = offset + data.shape[0]
                var[offset:end] = data

    def get_out_filename(self, extension):
        return "aux_data.%s" % extension
