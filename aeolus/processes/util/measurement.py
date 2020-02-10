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
import netCDF4
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

        if kw.get('ica_fields'):
            ica_fields = kw['ica_fields'].split(',')
        else:
            ica_fields = []

        if kw.get('sca_fields'):
            sca_fields = kw['sca_fields'].split(',')
        else:
            sca_fields = []

        def get_optimized_data_item(product):
            try:
                return product.optimized_data_item
            except Exception:
                return None

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
                        product.product_data_items.all().first(),
                        get_optimized_data_item(product),

                    )
                    for product in products
                )
            ], data_filters,
                observation_fields=observation_fields,
                measurement_fields=measurement_fields,
                group_fields=group_fields,
                ica_fields=ica_fields,
                sca_fields=sca_fields,
                simple_observation_filters=True,
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
            ]

            for data_kinds in data_iterator:
                for data_kind, acc in zip(data_kinds, accumulated_data):
                    for field, values in data_kind.items():
                        if values is not None:
                            acc[field].extend([
                                value.tolist() if value is not None else []
                                for value in values
                            ])
                        else:
                            acc[field].extend([])

            collection_data = dict(
                observation_data=accumulated_data[0],
                measurement_data=accumulated_data[1],
                group_data=accumulated_data[2],
                ica_data=accumulated_data[3],
                sca_data=accumulated_data[4],
            )

            out_data[collection.identifier] = collection_data

        return out_data

    def write_product_data_to_netcdf(self, ds, file_data):
        observation_data = file_data[0]
        measurement_data = file_data[1]
        group_data = file_data[2]
        ica_data = file_data[3]
        sca_data = file_data[4]

        if observation_data and 'observation' not in ds.dimensions:
            ds.createDimension('observation', None)
            num_observations = 0
        elif observation_data:
            num_observations = ds.dimensions['observation'].size

        if measurement_data and 'measurement' not in ds.dimensions:
            ds.createDimension('measurement', None)
            num_measurements = 0
        elif measurement_data:
            num_measurements = ds.dimensions['measurement'].size

        if group_data and 'group' not in ds.dimensions:
            ds.createDimension('group', None)
            num_groups = 0
        elif group_data:
            num_groups = ds.dimensions['group'].size

        if ica_data and 'ica_dim' not in ds.dimensions:
            ds.createDimension('ica_dim', None)
            num_icas = 0
        elif ica_data:
            num_icas = ds.dimensions['ica_dim'].size

        if sca_data and 'sca_dim' not in ds.dimensions:
            ds.createDimension('sca_dim', None)
            num_scas = 0
        elif sca_data:
            num_scas = ds.dimensions['sca_dim'].size

        if observation_data:
            group = ds.createGroup('observations')

            for name, values in observation_data.items():
                if not values.shape[0]:
                    continue

                isscalar = values[0].ndim == 0

                if np.ma.is_masked(values):
                    values.set_fill_value(
                        netCDF4.default_fillvals.get(
                            netcdf_dtype(values.dtype)
                        )
                    )

                if name not in group.variables:
                    # check if a dimension for that array was already created.
                    # Create one, if it not yet existed

                    array_dim_name = None
                    if not isscalar:
                        values = np.vstack(values)
                        array_dim_size = values.shape[-1]
                        array_dim_name = "array_%d" % array_dim_size
                        if array_dim_name not in ds.dimensions:
                            ds.createDimension(array_dim_name, array_dim_size)

                    variable = ds.createVariable(
                        '/observations/%s' % name, netcdf_dtype(values.dtype), (
                            'observation'
                        ) if isscalar else (
                            'observation', array_dim_name
                        )
                    )
                    variable[:] = values
                else:
                    var = group[name]
                    end = num_observations + values.shape[0]
                    var[num_observations:end] = values

        if measurement_data:
            group = ds.createGroup('measurements')

            for name, values in measurement_data.items():
                if values is None or not values.shape[0]:
                    continue

                isscalar = values.ndim == 2

                if np.ma.is_masked(values):
                    values.set_fill_value(
                        netCDF4.default_fillvals.get(
                            netcdf_dtype(values.dtype)
                        )
                    )

                    if isscalar:
                        values = np.ma.hstack(values)
                    else:
                        values = np.ma.vstack(values)

                else:
                    if isscalar:
                        values = np.hstack(values)
                    else:
                        values = np.vstack(values)

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
                            values.dtype.kind, values.dtype.itemsize
                        ), (
                            'measurement',
                        ) if isscalar else (
                            'measurement',
                            array_dim_name,
                        )
                    )

                    var[:] = values

                else:
                    var = group[name]
                    end = num_measurements + values.shape[0]
                    var[num_measurements:end] = values

        if group_data:
            group = ds.createGroup('groups')

            for name, values in group_data.items():
                if not len(values):
                    continue

                isscalar = values[0].ndim == 0

                if np.ma.is_masked(values):
                    values.set_fill_value(
                        netCDF4.default_fillvals.get(
                            netcdf_dtype(values.dtype)
                        )
                    )

                if isscalar:
                    values = np.hstack(values)

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
                        '/groups/%s' % name, '%s%i' % (
                            values[0].dtype.kind, values[0].dtype.itemsize
                        ), (
                            'group',
                        ) if isscalar else (
                            'group',
                            array_dim_name,
                        )
                    )
                    var[:] = values

                else:
                    var = group[name]
                    end = num_groups + values.shape[0]
                    var[num_groups:end] = values

        if ica_data:
            group = ds.createGroup('ica')

            for name, values in ica_data.items():
                if not values.shape[0]:
                    continue

                isscalar = values[0].ndim == 0

                if np.ma.is_masked(values):
                    values.set_fill_value(
                        netCDF4.default_fillvals.get(
                            netcdf_dtype(values.dtype)
                        )
                    )

                if isscalar:
                    values = np.hstack(values)

                if name not in group.variables:
                    # check if a dimension for that array was already created.
                    # Create one, if it not yet existed
                    array_dim_name = None
                    if not isscalar:
                        array_dim_size = values.shape[-1]
                        array_dim_name = "array_%d" % array_dim_size
                        if array_dim_name not in ds.dimensions:
                            ds.createDimension(array_dim_name, array_dim_size)

                        if np.ma.is_masked(values):
                            values.set_fill_value(
                                netCDF4.default_fillvals.get(
                                    netcdf_dtype(values.dtype)
                                )
                            )

                    var = ds.createVariable(
                        '/ica/%s' % name, netcdf_dtype(values.dtype), (
                            'ica_dim',
                        ) if isscalar else (
                            'ica_dim',
                            array_dim_name,
                        )
                    )

                    var[:] = values

                else:
                    var = group[name]
                    end = num_icas + values.shape[0]
                    var[num_icas:end] = values

        if sca_data:
            group = ds.createGroup('sca')

            for name, values in sca_data.items():
                if not values.shape[0]:
                    continue

                isscalar = values[0].ndim == 0

                if np.ma.is_masked(values):
                    values.set_fill_value(
                        netCDF4.default_fillvals.get(
                            netcdf_dtype(values.dtype)
                        )
                    )

                if isscalar:
                    values = np.hstack(values)
                else:
                    values = np.vstack(values)

                if name not in group.variables:
                    # check if a dimension for that array was already created.
                    # Create one, if it not yet existed
                    array_dim_name = None
                    if not isscalar:
                        array_dim_size = values.shape[-1]
                        array_dim_name = "array_%d" % array_dim_size
                        if array_dim_name not in ds.dimensions:
                            ds.createDimension(array_dim_name, array_dim_size)

                        if np.ma.is_masked(values):
                            values.set_fill_value(
                                netCDF4.default_fillvals.get(
                                    netcdf_dtype(values.dtype)
                                )
                            )

                    var = ds.createVariable(
                        '/sca/%s' % name, netcdf_dtype(values.dtype), (
                            'sca_dim',
                        ) if isscalar else (
                            'sca_dim',
                            array_dim_name,
                        )
                    )

                    var[:] = values

                else:
                    var = group[name]
                    end = num_scas + values.shape[0]
                    var[num_scas:end] = values


def netcdf_dtype(numpy_dtype):
    return '%s%i' % (numpy_dtype.kind, numpy_dtype.itemsize)
