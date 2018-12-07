# ------------------------------------------------------------------------------
#
#  Data extraction from Level 1B ADM-Aeolus AUX products
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


def extract_data(filenames, filters, fields):
    """
    """

    typed_fields_and_filters = [(
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
        data = defaultdict(list)

        with CODAFile(filename) as cf:
            for typed_fields, typed_filters in typed_fields_and_filters:

                calibration_filters = {
                    name: filter_value
                    for name, filter_value in typed_filters.items()
                    if name in CALIBRATION_FIELDS or
                    name in CALIBRATION_ARRAY_FIELDS
                }

                frequency_filters = {
                    name: filter_value
                    for name, filter_value in typed_filters.items()
                    if calibration_filters
                }

                requested_calibration_fields = [
                    field
                    for field in typed_fields
                    if field in CALIBRATION_FIELDS or
                    field in CALIBRATION_ARRAY_FIELDS
                ]

                requested_frequency_fields = [
                    field
                    for field in typed_fields
                    if field not in requested_calibration_fields
                ]

                # make a mask of all calibrations to be included, by only
                # looking at the fields for whole calibrations
                calibration_mask = None
                for field_name, filter_value in calibration_filters.items():
                    path = LOCATIONS[field_name][:]
                    new_mask = make_mask(
                        cf.fetch(*path),
                        filter_value.get('min'), filter_value.get('max'),
                        field in CALIBRATION_ARRAY_FIELDS
                    )
                    calibration_mask = combine_mask(new_mask, calibration_mask)

                # when the mask is done, create an array of indices for
                # calibrations
                # to be included
                calibration_nonzero_ids = None
                if calibration_mask is not None:
                    calibration_nonzero_ids = np.nonzero(calibration_mask)
                    calibration_ids = calibration_nonzero_ids[0]

                # load all desired values for the requested calibrations
                for field_name in requested_calibration_fields:
                    path = LOCATIONS[field_name]
                    field_data = cf.fetch(*path)

                    if calibration_nonzero_ids is not None:
                        field_data = field_data[calibration_nonzero_ids]

                    # write out data
                    data[field_name].extend(_array_to_list(field_data))

                # iterate over all calibrations
                for calibration_id in calibration_ids:
                    # build a mask of all frequencies within a specific
                    # calibration
                    frequency_mask = None
                    for field_name, filter_value in frequency_filters.items():
                        path = LOCATIONS[field_name]

                        new_mask = make_mask(
                            cf.fetch(path[0], calibration_id, *path[2:]),
                            filter_value.get('min'), filter_value.get('max'),
                            field_name in CALIBRATION_ARRAY_FIELDS
                        )
                        frequency_mask = combine_mask(new_mask, frequency_mask)

                    # make an array of all indices to be included
                    frequency_ids = None
                    if frequency_mask is not None:
                        frequency_ids = np.nonzero(frequency_mask)

                    # iterate over all requested frequency fields and write the
                    # possibly subset data to the output
                    for field_name in requested_frequency_fields:
                        path = LOCATIONS[field_name]
                        field_data = cf.fetch(path[0], calibration_id, *path[2:])

                        if frequency_ids is not None:
                            field_data = field_data[frequency_ids]

                        data[field_name].append(_array_to_list(field_data))

            yield data


# test_file = '/mnt/data/AE_OPER_AUX_ISR_1B_20071002T103629_20071002T110541_0002.EEF'
test_file = '/mnt/data/AE_OPER_AUX_MRC_1B_20071031T021229_20071031T022829_0002.EEF'
# test_file = '/mnt/data/AE_OPER_AUX_RRC_1B_20071031T021229_20071031T022829_0002.EEF'
# test_file = '/mnt/data/AE_OPER_AUX_ZWC_1B_20071101T202641_20071102T000841_0001.EEF'


def main():
    from pprint import pprint

    data = extract_data('/mnt/data/AE_OPER_AUX_ISR_1B_20071002T103629_20071002T110541_0002.EEF', {
        # 'freq_mie_USR_closest_to_rayleigh_filter_centre': {
        #     'max': 1,
        # },
        # 'mie_response': {
        #     'min': 10,
        #     'max': 12,
        # }
    }, [
        'mie_valid',
        # 'freq_mie_USR_closest_to_rayleigh_filter_centre',
    ], 'ISR')

    # data = extract_data(
    #     '/mnt/data/AE_OPER_AUX_MRC_1B_20071031T021229_20071031T022829_0002.EEF',
    #     {
    #         'lat_of_DEM_intersection': {
    #             'max': 0,
    #         },
    #         'altitude': {
    #             'min': 25000,
    #             # 'max': 12,
    #         }
    #     }, [
    #         'measurement_mean_sensitivity',
    #         'lat_of_DEM_intersection',
    #         'altitude',
    #     ], 'MRC'
    # )

    pprint(dict(data))

    # from pprint import pprint
    # import contextlib
    # import time
    # from aeolus.coda_utils import CODAFile, datetime_to_coda_time
    # import numpy as np

    # @contextlib.contextmanager
    # def timed(name):
    #     start = time.time()
    #     yield
    #     print str(int((time.time() - start) * 1000)) + 'ms', name

    # # locations = AUX_ISR_LOCATIONS
    # locations = AUX_MRC_LOCATIONS
    # # locations = AUX_RRC_LOCATIONS
    # # locations = AUX_ZWC_LOCATIONS

    # with CODAFile(test_file) as cf:
    #     for name, path in locations.items():
    #         #with timed(name):
    #         value = cf.fetch(*path)
    #         if isinstance(value, np.ndarray):
    #             shape = []
    #             while value.dtype == np.object:
    #                 value = np.stack(value)
    #                 shape.extend(value.shape)

    #             print "shape", shape, value.shape
    #         # print cf.get_size(*path)
    #         print name, value.shape if not isinstance(value, (int, float)) else 0



if __name__ == "__main__":
    main()
