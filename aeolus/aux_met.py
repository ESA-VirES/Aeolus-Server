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

from collections import defaultdict

import numpy as np

from aeolus.coda_utils import CODAFile
from aeolus.filtering import make_mask, combine_mask


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


def extract_data(filenames, filters, fields, convert_arrays=False):
    """
    """

    typed_fields_and_filters = [(
            'off_nadir',
            [
                field_name
                for field_name in fields
                if field_name in OFF_NADIR_FIELDS
            ], dict([
                field_name
                for field_name, value in filters
                if field_name in OFF_NADIR_FIELDS
            ]),
        ), (
            'nadir',
            [
                field_name
                for field_name in fields
                if field_name in NADIR_FIELDS
            ], dict([
                field_name
                for field_name, value in filters
                if field_name in NADIR_FIELDS
            ]),
        )
    ]

    for filename in filenames:
        with CODAFile(filename) as cf:
            for t_name, typed_fields, filters in typed_fields_and_filters:
                data = defaultdict(list)

                # make a mask of all calibrations to be included, by only
                # looking at the fields for whole calibrations
                calibration_mask = None
                for field_name, filter_value in filters.items():
                    path = LOCATIONS[field_name][:]
                    new_mask = make_mask(
                        cf.fetch(*path),
                        filter_value.get('min'), filter_value.get('max'),
                        field_name in CALIBRATION_ARRAY_FIELDS
                    )
                    calibration_mask = combine_mask(new_mask, calibration_mask)

                # when the mask is done, create an array of indices for
                # calibrations to be included
                calibration_nonzero_ids = None
                if calibration_mask is not None:
                    calibration_nonzero_ids = np.nonzero(calibration_mask)

                # load all desired values for the requested calibrations
                for field_name in fields:
                    path = LOCATIONS[field_name]
                    field_data = cf.fetch(*path)

                    if calibration_nonzero_ids is not None:
                        field_data = field_data[calibration_nonzero_ids]

                    # write out data
                    if convert_arrays:
                        data[field_name] = _array_to_list(field_data)
                    else:
                        data[field_name] = field_data

                yield t_name, data
