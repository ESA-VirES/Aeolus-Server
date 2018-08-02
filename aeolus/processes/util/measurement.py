# ------------------------------------------------------------------------------
#
#  Base class for processes dealing with measurement data (Level 1B/2A)
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

import numpy as np
from eoxserver.services.ows.wps.parameters import LiteralData

from aeolus.processes.util.bbox import translate_bbox
from aeolus.processes.util.base import ExtractionProcessBase


class MeasurementDataExtractProcessBase(ExtractionProcessBase):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level 1B/2A products of the specified collections.
    """

    # Override these values in the subclasses
    extraction_function = None
    level_name = None

    # common inputs/outputs to satisfy ProcessInterface
    inputs = ExtractionProcessBase.inputs + [
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

    def get_data_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        """ Overwritten function to get the exact data filters for L2B/C files
        """
        data_filters = dict(
            time={'min': begin_time, 'max': end_time},
            **filters
        )

        if bbox:
            tpl_box = translate_bbox(
                (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            )
            data_filters['longitude_of_DEM_intersection'] = {
                'min': tpl_box[0],
                'max': tpl_box[2]
            }
            data_filters['latitude_of_DEM_intersection'] = {
                'min': tpl_box[1],
                'max': tpl_box[3]
            }

        return data_filters

    def extract_data(self, collection_products, data_filters, mime_type, **kw):
        """ L2B/C extraction function
        """
        if kw.get('observation_fields'):
            observation_fields = kw['observation_fields'].split(',')
        else:
            observation_fields = []

        if kw.get('measurement_fields'):
            measurement_fields = kw['measurement_fields'].split(',')
        else:
            measurement_fields = []

        if kw.get('group_fields'):
            group_fields = kw['group_fields'].split(',')
        else:
            group_fields = []

        # create the iterator: yielding collection + sub iterators
        # the sub iterators iterate over all data files and yield the selected
        # and filtered fields
        return (
            (collection, self.extraction_function([
                product.data_items.filter(semantic__startswith='bands')
                .first().location
                for product in products
            ], data_filters,
                observation_fields=observation_fields,
                measurement_fields=measurement_fields,
                group_fields=group_fields,
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
            ]

            for data_kinds in data_iterator:
                for data_kind, acc in zip(data_kinds, accumulated_data):
                    for field, values in data_kind.items():
                        acc[field].extend(values)

            collection_data = dict(
                observation_data=accumulated_data[0],
                measurement_data=accumulated_data[1],
                group_data=accumulated_data[2],
            )
            out_data[collection.identifier] = collection_data
        return out_data

    def write_product_data_to_netcdf(self, ds, file_data):
        observation_data = file_data[0]
        measurement_data = file_data[1]
        group_data = file_data[2]

        if 'observation' not in ds.dimensions:
            ds.createDimension('observation', None)
            num_observations = 0
        else:
            num_observations = ds.dimensions['observation'].size

        if measurement_data and 'measurements_per_observation' not in ds.dimensions:
            ds.createDimension('measurements_per_observation', 30)
        if group_data and 'group' not in ds.dimensions:
            ds.createDimension('group', None)
            num_groups = 0
        elif group_data:
            num_groups = ds.dimensions['group'].size

        if observation_data:
            group = ds.createGroup('observations')

            for name, values in observation_data.items():
                isscalar = values[0].ndim == 0
                if name not in group.variables:
                    values = np.hstack(values) if isscalar else np.vstack(values)

                    # check if a dimension for that array was already created.
                    # Create one, if it not yet existed
                    array_dim_name = None
                    if not isscalar:
                        array_dim_size = values.shape[-1]
                        array_dim_name = "array_%d" % array_dim_size
                        if array_dim_name not in ds.dimensions:
                            ds.createDimension(array_dim_name, array_dim_size)

                    variable = ds.createVariable(
                        '/observations/%s' % name, '%s%i' % (
                            values.dtype.kind, values.dtype.itemsize
                        ), (
                            'observation'
                        ) if isscalar else (
                            'observation', array_dim_name
                        )
                    )
                    variable[:] = values
                else:
                    values = np.hstack(values) if isscalar else np.vstack(values)
                    var = group[name]
                    end = num_observations + values.shape[0]
                    var[num_observations:end] = values

        if measurement_data:
            group = ds.createGroup('measurements')

            for name, values in measurement_data.items():
                isscalar = values[0][0].ndim == 0

                if isscalar:
                    values = np.vstack(values)
                else:
                    values = np.rollaxis(
                        np.dstack([np.vstack(obs) for obs in values]),
                        2, 0
                    )

                if name not in group.variables:
                    # check if a dimension for that array was already created.
                    # Create one, if it not yet existed
                    array_dim_name = None
                    if not isscalar:
                        array_dim_size = values.shape[-1]
                        array_dim_name = "array_%d" % array_dim_size
                        if array_dim_name not in ds.dimensions:
                            ds.createDimension(array_dim_name, array_dim_size)

                    var = ds.createVariable(
                        '/measurements/%s' % name, '%s%i' % (
                            values[0].dtype.kind, values[0].dtype.itemsize
                        ), (
                            'observation', 'measurements_per_observation'
                        ) if isscalar else (
                            'observation',
                            'measurements_per_observation',
                            array_dim_name,
                        )
                    )
                    var[:] = values

                else:
                    var = group[name]
                    end = num_observations + values.shape[0]
                    var[num_observations:end] = values

        # TODO: group data?

    def get_out_filename(self, extension):
        return "level_%s_data.%s" % (self.level_name, extension)
