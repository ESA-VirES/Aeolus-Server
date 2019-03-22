# ------------------------------------------------------------------------------
#
#  Data extraction from ADM-Aeolus AUX MET products
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

import os.path
from collections import defaultdict
from copy import deepcopy

import numpy as np
from netCDF4 import Dataset
from scipy.interpolate import interp1d

from aeolus.coda_utils import CODAFile, access_location
from aeolus.filtering import make_mask, make_array_mask, combine_mask


# ------------------------------------------------------------------------------
# AUX MET
# ------------------------------------------------------------------------------

LOCATIONS = {
    'time_off_nadir':                               ['/geo_off_nadir', -1, 'amd_datetime'],
    'latitude_off_nadir':                           ['/geo_off_nadir', -1, 'amd_latitude'],
    'longitude_off_nadir':                          ['/geo_off_nadir', -1, 'amd_longitude'],
    'surface_wind_component_u_off_nadir':           ['/met_off_nadir', -1, 'amd_us'],
    'surface_wind_component_v_off_nadir':           ['/met_off_nadir', -1, 'amd_vs'],
    'surface_pressure_off_nadir':                   ['/met_off_nadir', -1, 'amd_ps'],
    'surface_altitude_off_nadir':                   ['/met_off_nadir', -1, 'amd_zs'],
    'layer_validity_flag_off_nadir':                ['/met_off_nadir', -1, 'profile_data', -1, 'amd_validity_flag'],
    'layer_pressure_off_nadir':                     ['/met_off_nadir', -1, 'profile_data', -1, 'amd_pnom'],
    'layer_altitude_off_nadir':                     ['/met_off_nadir', -1, 'profile_data', -1, 'amd_znom'],
    'layer_temperature_off_nadir':                  ['/met_off_nadir', -1, 'profile_data', -1, 'amd_t'],
    'layer_wind_component_u_off_nadir':             ['/met_off_nadir', -1, 'profile_data', -1, 'amd_u'],
    'layer_wind_component_v_off_nadir':             ['/met_off_nadir', -1, 'profile_data', -1, 'amd_v'],
    'layer_rel_humidity_off_nadir':                 ['/met_off_nadir', -1, 'profile_data', -1, 'amd_rh'],
    'layer_spec_humidity_off_nadir':                ['/met_off_nadir', -1, 'profile_data', -1, 'amd_q'],
    'layer_cloud_cover_off_nadir':                  ['/met_off_nadir', -1, 'profile_data', -1, 'amd_cc'],
    'layer_cloud_liquid_water_content_off_nadir':   ['/met_off_nadir', -1, 'profile_data', -1, 'amd_clwc'],
    'layer_cloud_ice_water_content_off_nadir':      ['/met_off_nadir', -1, 'profile_data', -1, 'amd_ciwc'],
    'time_nadir':                                   ['/geo_nadir', -1, 'amd_datetime'],
    'latitude_nadir':                               ['/geo_nadir', -1, 'amd_latitude'],
    'longitude_nadir':                              ['/geo_nadir', -1, 'amd_longitude'],
    'surface_wind_component_u_nadir':               ['/met_nadir', -1, 'amd_us'],
    'surface_wind_component_v_nadir':               ['/met_nadir', -1, 'amd_vs'],
    'surface_pressure_nadir':                       ['/met_nadir', -1, 'amd_ps'],
    'surface_altitude_nadir':                       ['/met_nadir', -1, 'amd_zs'],
    'layer_validity_flag_nadir':                    ['/met_nadir', -1, 'profile_data', -1, 'amd_validity_flag'],
    'layer_pressure_nadir':                         ['/met_nadir', -1, 'profile_data', -1, 'amd_pnom'],
    'layer_altitude_nadir':                         ['/met_nadir', -1, 'profile_data', -1, 'amd_znom'],
    'layer_temperature_nadir':                      ['/met_nadir', -1, 'profile_data', -1, 'amd_t'],
    'layer_wind_component_u_nadir':                 ['/met_nadir', -1, 'profile_data', -1, 'amd_u'],
    'layer_wind_component_v_nadir':                 ['/met_nadir', -1, 'profile_data', -1, 'amd_v'],
    'layer_rel_humidity_nadir':                     ['/met_nadir', -1, 'profile_data', -1, 'amd_rh'],
    'layer_spec_humidity_nadir':                    ['/met_nadir', -1, 'profile_data', -1, 'amd_q'],
    'layer_cloud_cover_nadir':                      ['/met_nadir', -1, 'profile_data', -1, 'amd_cc'],
    'layer_cloud_liquid_water_content_nadir':       ['/met_nadir', -1, 'profile_data', -1, 'amd_clwc'],
    'layer_cloud_ice_water_content_nadir':          ['/met_nadir', -1, 'profile_data', -1, 'amd_ciwc'],
}


OFF_NADIR_FIELDS = set([
    'time_off_nadir',
    'latitude_off_nadir',
    'longitude_off_nadir',
    'surface_wind_component_u_off_nadir',
    'surface_wind_component_v_off_nadir',
    'surface_pressure_off_nadir',
    'surface_altitude_off_nadir',
    'layer_validity_flag_off_nadir',
    'layer_pressure_off_nadir',
    'layer_altitude_off_nadir',
    'layer_temperature_off_nadir',
    'layer_wind_component_u_off_nadir',
    'layer_wind_component_v_off_nadir',
    'layer_rel_humidity_off_nadir',
    'layer_spec_humidity_off_nadir',
    'layer_cloud_cover_off_nadir',
    'layer_cloud_liquid_water_content_off_nadir',
    'layer_cloud_ice_water_content_off_nadir',
])

NADIR_FIELDS = set([
    'time_nadir',
    'latitude_nadir',
    'longitude_nadir',
    'surface_wind_component_u_nadir',
    'surface_wind_component_v_nadir',
    'surface_pressure_nadir',
    'surface_altitude_nadir',
    'layer_validity_flag_nadir',
    'layer_pressure_nadir',
    'layer_altitude_nadir',
    'layer_temperature_nadir',
    'layer_wind_component_u_nadir',
    'layer_wind_component_v_nadir',
    'layer_rel_humidity_nadir',
    'layer_spec_humidity_nadir',
    'layer_cloud_cover_nadir',
    'layer_cloud_liquid_water_content_nadir',
    'layer_cloud_ice_water_content_nadir',
])

CALIBRATION_FIELDS = set([
    'time_off_nadir',
    'time_nadir',
    'latitude_off_nadir',
    'latitude_nadir',
    'longitude_off_nadir',
    'longitude_nadir',
    'surface_wind_component_u_off_nadir',
    'surface_wind_component_u_nadir',
    'surface_wind_component_v_off_nadir',
    'surface_wind_component_v_nadir',
    'surface_pressure_off_nadir',
    'surface_pressure_nadir',
    'surface_altitude_off_nadir',
    'surface_altitude_nadir',
])

CALIBRATION_ARRAY_FIELDS = set([
    'layer_validity_flag_off_nadir',
    'layer_validity_flag_nadir',
    'layer_pressure_off_nadir',
    'layer_pressure_nadir',
    'layer_altitude_off_nadir',
    'layer_altitude_nadir',
    'layer_temperature_off_nadir',
    'layer_temperature_nadir',
    'layer_wind_component_u_off_nadir',
    'layer_wind_component_u_nadir',
    'layer_wind_component_v_off_nadir',
    'layer_wind_component_v_nadir',
    'layer_rel_humidity_off_nadir',
    'layer_rel_humidity_nadir',
    'layer_spec_humidity_off_nadir',
    'layer_spec_humidity_nadir',
    'layer_cloud_cover_off_nadir',
    'layer_cloud_cover_nadir',
    'layer_cloud_liquid_water_content_off_nadir',
    'layer_cloud_liquid_water_content_nadir',
    'layer_cloud_ice_water_content_off_nadir',
    'layer_cloud_ice_water_content_nadir',
])

AUX_FILE_TYPE_PATH = (
    'Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/File_Type'
)


def _array_to_list(data):
    if isinstance(data, np.ndarray):
        isobject = data.dtype == np.object
        data = data.tolist()
        if isobject:
            data = [
                _array_to_list(obj) for obj in data
            ]
    return data


def extract_data(filenames, filters, fields, scalefactor):
    """
    """

    orig_filters = filters

    files = [
        (
            CODAFile(coda_filename),
            Dataset(netcdf_filename)
            if netcdf_filename and os.path.exists(netcdf_filename)
            else None

        )
        for (coda_filename, netcdf_filename) in filenames
    ]

    for i, (cf, ds) in enumerate(files):
        with cf:

            next_cf = files[i + 1][0] if (i + 1) < len(files) else None

            # TODO: handle overlap
            if next_cf and overlaps(cf, next_cf):
                filters = adjust_overlap(
                    cf, next_cf, deepcopy(orig_filters)
                )
                # completely overlapped
                if filters is None:
                    continue

            else:
                filters = orig_filters

            typed_fields_and_filters = [(
                'off_nadir',
                [
                    field_name
                    for field_name in fields
                    if field_name in OFF_NADIR_FIELDS
                ], dict([
                    (field_name, value)
                    for field_name, value in filters.items()
                    if field_name in OFF_NADIR_FIELDS
                ]),
            ), (
                'nadir',
                [
                    field_name
                    for field_name in fields
                    if field_name in NADIR_FIELDS
                ], dict([
                    (field_name, value)
                    for field_name, value in filters.items()
                    if field_name in NADIR_FIELDS
                ]),
            )]

            for t_name, typed_fields, filters in typed_fields_and_filters:
                data = defaultdict(list)

                # make a mask of all calibrations to be included, by only
                # looking at the fields for whole calibrations
                mask = None
                array_mask = None

                for field_name, filter_value in filters.items():
                    path = LOCATIONS[field_name][:]

                    mask_data = access_optimized(cf, ds, field_name, path)
                    mask_data = scale_data(mask_data, scalefactor)

                    is_array = field_name in CALIBRATION_ARRAY_FIELDS

                    if is_array:
                        mask_data = np.vstack(mask_data)

                    new_mask = make_mask(
                        mask_data,
                        filter_value.get('min'), filter_value.get('max'),
                    )
                    mask = combine_mask(new_mask, mask)

                    if is_array:
                        new_array_mask = make_array_mask(
                            mask_data, **filter_value
                        )
                        array_mask = combine_mask(new_array_mask, array_mask)

                # when the mask is done, create an array of indices for
                # calibrations to be included
                nonzero_ids = None
                if mask is not None:
                    nonzero_ids = np.nonzero(mask)
                    if array_mask is not None:
                        array_mask = np.logical_not(array_mask[nonzero_ids])

                # load all desired values for the requested calibrations
                for field_name in typed_fields:
                    path = LOCATIONS[field_name]

                    field_data = access_optimized(cf, ds, field_name, path)
                    field_data = scale_data(field_data, scalefactor)

                    if nonzero_ids is not None:
                        # skip over empty patches of data
                        if nonzero_ids[0].shape[0] == 0:
                            continue

                        field_data = field_data[nonzero_ids]
                        if field_name in CALIBRATION_ARRAY_FIELDS:
                            if field_data.shape[0] > 0:
                                field_data = np.vstack(field_data)

                            if array_mask is not None:
                                field_data = np.ma.MaskedArray(
                                    field_data, array_mask
                                )

                    data[field_name] = field_data

                yield t_name, data


def overlaps(cf, next_cf):
    end_time = cf.fetch_date('mph/sensing_stop')
    begin_time = next_cf.fetch_date('mph/sensing_start')
    return end_time > begin_time


def adjust_overlap(cf, next_cf, filters):
    stop_time = next_cf.fetch_date('mph/sensing_start')

    for field in ['time_off_nadir', 'time_nadir']:

        if field not in filters:
            filters[field] = {'max': stop_time}

        else:
            if 'min' in filters[field] and filters[field]['min'] > stop_time:
                return None

            elif 'max' not in filters[field]:
                filters[field]['max'] = stop_time

            else:
                filters[field]['max'] = min(
                    stop_time, filters[field]['max']
                )

    return filters


def access_optimized(cf, ds, field_name, location):
    if ds:
        group = ds.groups.get('DATA')
        if group:
            variable = group.variables.get(field_name)
            if variable:
                return variable[:]
    return access_location(cf, location)


def scale_data(data, scalefactor):
    if scalefactor == 1:
        return data

    cur_size = int(data.shape[0])
    new_size = int(float(cur_size) * scalefactor)
    old_x = np.linspace(0, 1, cur_size)
    new_x = np.linspace(0, 1, new_size)

    return interp1d(old_x, data, kind='nearest', axis=0)(new_x)
