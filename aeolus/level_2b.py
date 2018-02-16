# ------------------------------------------------------------------------------
#
#  Aeolus - Level 1B data extraction
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
from collections import defaultdict

import numpy as np

from aeolus.coda_utils import CODAFile, datetime_to_coda_time
from aeolus.filtering import make_mask, combine_mask


locations = {
    'mie_measurement_map':                              ['/meas_map', -1, 'mie_map_of_l1b_meas_used', -1, 'which_l2b_wind_id'],
    'rayleigh_measurement_map':                         ['/meas_map', -1, 'rayleigh_map_of_l1b_meas_used', -1, 'which_l2b_wind_id'],
    'mie_measurement_weight':                           ['/meas_map', -1, 'mie_map_of_l1b_meas_used', -1, 'weight'],
    'rayleigh_measurement_weight':                      ['/meas_map', -1, 'rayleigh_map_of_l1b_meas_used', -1, 'weight'],
    'mie_grouping_id':                                  ['/mie_grouping', -1, 'grouping_result_id'],
    'mie_grouping_time':                                ['/mie_grouping', -1, 'start_of_obs_datetime'],
    'mie_grouping_start_obs':                           ['/mie_grouping', -1, 'which_l1b_brc1'],
    'mie_grouping_start_meas_per_obs':                  ['/mie_grouping', -1, 'which_l1b_meas_within_this_brc1'],
    'mie_grouping_end_obs':                             ['/mie_grouping', -1, 'which_l1b_brc2'],
    'mie_grouping_end_meas_per_obs':                    ['/mie_grouping', -1, 'which_l1b_meas_within_this_brc2'],
    'rayleigh_grouping_id':                             ['/rayleigh_grouping', -1, 'grouping_result_id'],
    'rayleigh_grouping_time':                           ['/rayleigh_grouping', -1, 'start_of_obs_datetime'],
    'rayleigh_grouping_start_obs':                      ['/rayleigh_grouping', -1, 'which_l1b_brc1'],
    'rayleigh_grouping_start_meas_per_obs':             ['/rayleigh_grouping', -1, 'which_l1b_meas_within_this_brc1'],
    'rayleigh_grouping_end_obs':                        ['/rayleigh_grouping', -1, 'which_l1b_brc2'],
    'rayleigh_grouping_end_meas_per_obs':               ['/rayleigh_grouping', -1, 'which_l1b_meas_within_this_brc2'],
    'l1B_num_of_measurements_per_obs':                  ['/meas_product_confid_data', -1, 'l1b_meas_number'],
    'l1B_obs_number':                                   ['/meas_product_confid_data', -1, 'l1b_brc_number'],
    'mie_wind_result_id':                               ['/mie_geolocation', -1, 'wind_result_id'],
    'mie_wind_profile_wind_result_id':                  ['/mie_profile', -1, 'l2b_wind_profiles/wind_result_id_number', -1],
    'mie_wind_result_range_bin_number':                 ['/mie_hloswind', -1, 'windresult/which_range_bin'],
    'mie_wind_result_start_time':                       ['/mie_geolocation', -1, 'windresult_geolocation/datetime_start'],
    'mie_wind_result_COG_time':                         ['/mie_geolocation', -1, 'windresult_geolocation/datetime_cog'],
    'mie_wind_result_stop_time':                        ['/mie_geolocation', -1, 'windresult_geolocation/datetime_stop'],
    'mie_wind_result_bottom_altitude':                  ['/mie_geolocation', -1, 'windresult_geolocation/altitude_bottom'],
    'mie_wind_result_COG_altitude':                     ['/mie_geolocation', -1, 'windresult_geolocation/altitude_vcog'],
    'mie_wind_result_top_altitude':                     ['/mie_geolocation', -1, 'windresult_geolocation/altitude_top'],
    'mie_wind_result_bottom_range':                     ['/mie_geolocation', -1, 'windresult_geolocation/satrange_bottom'],
    'mie_wind_result_COG_range':                        ['/mie_geolocation', -1, 'windresult_geolocation/satrange_vcog'],
    'mie_wind_result_top_range':                        ['/mie_geolocation', -1, 'windresult_geolocation/satrange_top'],
    'mie_wind_result_start_latitude':                   ['/mie_geolocation', -1, 'windresult_geolocation/latitude_start'],
    'mie_wind_result_COG_latitude':                     ['/mie_geolocation', -1, 'windresult_geolocation/latitude_cog'],
    'mie_wind_result_stop_latitude':                    ['/mie_geolocation', -1, 'windresult_geolocation/latitude_stop'],
    'mie_wind_result_start_longitude':                  ['/mie_geolocation', -1, 'windresult_geolocation/longitude_start'],
    'mie_wind_result_COG_longitude':                    ['/mie_geolocation', -1, 'windresult_geolocation/longitude_cog'],
    'mie_wind_result_stop_longitude':                   ['/mie_geolocation', -1, 'windresult_geolocation/longitude_stop'],
    'mie_wind_result_lat_of_DEM_intersection':          ['/mie_geolocation', -1, 'windresult_geolocation/lat_of_dem_intersection'],
    # 'mie_profile_lat_of_DEM_intersection':              [''],
    'mie_wind_result_lon_of_DEM_intersection':          ['/mie_geolocation', -1, 'windresult_geolocation/lon_of_dem_intersection'],
    # 'mie_profile_lon_of_DEM_intersection':              [''],
    'mie_wind_result_geoid_separation':                 ['/mie_geolocation', -1, 'windresult_geolocation/wgs84_to_geoid_altitude'],
    # 'mie_profile_geoid_separation':                     [''],
    'mie_wind_result_alt_of_DEM_intersection':          ['/mie_geolocation', -1, 'windresult_geolocation/alt_of_dem_intersection'],
    # 'mie_profile_alt_of_DEM_intersection':              [''],
    'rayleigh_wind_result_id':                          ['/rayleigh_geolocation', -1, 'wind_result_id'],
    'rayleigh_wind_profile_wind_result_id':             ['/rayleigh_profile', -1, 'l2b_wind_profiles/wind_result_id_number', -1],
    'rayleigh_wind_result_range_bin_number':            ['/rayleigh_hloswind', -1, 'windresult/which_range_bin'], # TODO ??
    'rayleigh_wind_result_start_time':                  ['/rayleigh_geolocation', -1, 'windresult_geolocation/datetime_start'],
    'rayleigh_wind_result_COG_time':                    ['/rayleigh_geolocation', -1, 'windresult_geolocation/datetime_cog'],
    'rayleigh_wind_result_stop_time':                   ['/rayleigh_geolocation', -1, 'windresult_geolocation/datetime_stop'],
    'rayleigh_wind_result_bottom_altitude':             ['/rayleigh_geolocation', -1, 'windresult_geolocation/altitude_bottom'],
    'rayleigh_wind_result_COG_altitude':                ['/rayleigh_geolocation', -1, 'windresult_geolocation/altitude_vcog'],
    'rayleigh_wind_result_top_altitude':                ['/rayleigh_geolocation', -1, 'windresult_geolocation/altitude_top'],
    'rayleigh_wind_result_bottom_range':                ['/rayleigh_geolocation', -1, 'windresult_geolocation/satrange_bottom'],
    'rayleigh_wind_result_COG_range':                   ['/rayleigh_geolocation', -1, 'windresult_geolocation/satrange_vcog'],
    'rayleigh_wind_result_top_range':                   ['/rayleigh_geolocation', -1, 'windresult_geolocation/satrange_top'],
    'rayleigh_wind_result_start_latitude':              ['/rayleigh_geolocation', -1, 'windresult_geolocation/latitude_start'],
    'rayleigh_wind_result_COG_latitude':                ['/rayleigh_geolocation', -1, 'windresult_geolocation/latitude_cog'],
    'rayleigh_wind_result_stop_latitude':               ['/rayleigh_geolocation', -1, 'windresult_geolocation/latitude_stop'],
    'rayleigh_wind_result_start_longitude':             ['/rayleigh_geolocation', -1, 'windresult_geolocation/longitude_start'],
    'rayleigh_wind_result_COG_longitude':               ['/rayleigh_geolocation', -1, 'windresult_geolocation/longitude_cog'],
    'rayleigh_wind_result_stop_longitude':              ['/rayleigh_geolocation', -1, 'windresult_geolocation/longitude_stop'],
    'rayleigh_wind_result_lat_of_DEM_intersection':     ['/rayleigh_geolocation', -1, 'windresult_geolocation/lat_of_dem_intersection'],
    # 'rayleigh_profile_lat_of_DEM_intersection':         [''],
    'rayleigh_wind_result_lon_of_DEM_intersection':     ['/rayleigh_geolocation', -1, 'windresult_geolocation/lon_of_dem_intersection'],
    # 'rayleigh_profile_lon_of_DEM_intersection':         [''],
    'rayleigh_wind_result_geoid_separation':            ['/rayleigh_geolocation', -1, 'windresult_geolocation/wgs84_to_geoid_altitude'],
    # 'rayleigh_profile_geoid_separation':                [''],
    'rayleigh_wind_result_alt_of_DEM_intersection':     ['/rayleigh_geolocation', -1, 'windresult_geolocation/alt_of_dem_intersection'],
    # 'rayleigh_profile_alt_of_DEM_intersection':         [''],
    # 'l1B_measurement_time':                             [''],  # TODO: not available
    # 'mie_bin_classification':                           [''],  # TODO: not described in XLS sheet
    # 'rayleigh_bin_classification':                      [''],  # TODO: not described in XLS sheet
    'optical_prop_algo_extinction':                     ['/meas_product_confid_data', -1, 'opt_prop_result/extinction_iterative'],
    'optical_prop_algo_scattering_ratio':               ['/meas_product_confid_data', -1, 'opt_prop_result/scattering_ratio_iterative'],
    'optical_prop_crosstalk_detected':                  ['/meas_product_confid_data', -1, 'opt_prop_result/xtalk_detected'],
    'mie_wind_result_HLOS_error':                       ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/hlos_error_estimate'],
    'mie_wind_result_QC_flags_1':                       ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/flags1'],
    'mie_wind_result_QC_flags_2':                       ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/flags2'],
    'mie_wind_result_QC_flags_3':                       ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/flags3'],
    'mie_wind_result_SNR':                              ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/mie_snr'],
    'mie_wind_result_scattering_ratio':                 ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/scattering_ratio'],
    'rayleigh_wind_result_HLOS_error':                  ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/hlos_error_estimate'],
    'rayleigh_wind_result_QC_flags_1':                  ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/flags1'],
    'rayleigh_wind_result_QC_flags_2':                  ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/flags2'],
    'rayleigh_wind_result_QC_flags_3':                  ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/flags3'],
    'rayleigh_wind_result_scattering_ratio':            ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/scattering_ratio'],
    'mie_wind_result_observation_type':                 ['/mie_hloswind', -1, 'windresult/observation_type'],
    'mie_wind_result_validity_flag':                    ['/mie_hloswind', -1, 'windresult/validity_flag'],
    'mie_wind_result_wind_velocity':                    ['/mie_hloswind', -1, 'windresult/mie_wind_velocity'],
    'mie_wind_result_integration_length':               ['/mie_hloswind', -1, 'windresult/integration_length'],
    'mie_wind_result_num_of_measurements':              ['/mie_hloswind', -1, 'windresult/n_meas_in_class'],
    'rayleigh_wind_result_observation_type':            ['/rayleigh_hloswind', -1, 'windresult/observation_type'],
    'rayleigh_wind_result_validity_flag':               ['/rayleigh_hloswind', -1, 'windresult/validity_flag'],
    'rayleigh_wind_result_wind_velocity':               ['/rayleigh_hloswind', -1, 'windresult/rayleigh_wind_velocity'],
    'rayleigh_wind_result_integration_length':          ['/rayleigh_hloswind', -1, 'windresult/integration_length'],
    'rayleigh_wind_result_num_of_measurements':         ['/rayleigh_hloswind', -1, 'windresult/n_meas_in_class'],
    'rayleigh_wind_result_reference_pressure':          ['/rayleigh_hloswind', -1, 'windresult/reference_pressure'],
    'rayleigh_wind_result_reference_temperature':       ['/rayleigh_hloswind', -1, 'windresult/reference_temperature'],
    'rayleigh_wind_result_reference_backscatter_ratio': ['/rayleigh_hloswind', -1, 'windresult/reference_backscatter_ratio'],
    'mie_wind_profile_observation_type':                ['/mie_profile', -1, 'l2b_wind_profiles/obs_type'],
    'rayleigh_wind_profile_observation_type':           ['/rayleigh_profile', -1, 'l2b_wind_profiles/obs_type'],
}


MIE_GROUPING_FIELDS = set([
    'mie_grouping_id',
    'mie_grouping_time',
    'mie_grouping_start_obs',
    'mie_grouping_start_meas_per_obs',
    'mie_grouping_end_obs',
    'mie_grouping_end_meas_per_obs',
])


RAYLEIGH_GROUPING_FIELDS = set([
    'rayleigh_grouping_id',
    'rayleigh_grouping_time',
    'rayleigh_grouping_start_obs',
    'rayleigh_grouping_start_meas_per_obs',
    'rayleigh_grouping_end_obs',
    'rayleigh_grouping_end_meas_per_obs',
])

MIE_PROFILE_FIELDS = set([
    'mie_wind_profile_wind_result_id',
    'mie_wind_profile_observation_type',
    'mie_profile_lat_of_DEM_intersection',
    'mie_profile_lon_of_DEM_intersection',
    'mie_profile_geoid_separation',
    'mie_profile_alt_of_DEM_intersection',
])

RAYLEIGH_PROFILE_FIELDS = set([
    'rayleigh_wind_profile_wind_result_id',
    'rayleigh_wind_profile_observation_type',
    'rayleigh_profile_lat_of_DEM_intersection',
    'rayleigh_profile_lon_of_DEM_intersection',
    'rayleigh_profile_geoid_separation',
    'rayleigh_profile_alt_of_DEM_intersection',
])

MIE_WIND_FIELDS = set([
    'mie_wind_result_id',
    'mie_wind_result_range_bin_number',
    'mie_wind_result_start_time',
    'mie_wind_result_COG_time',
    'mie_wind_result_stop_time',
    'mie_wind_result_bottom_altitude',
    'mie_wind_result_COG_altitude',
    'mie_wind_result_top_altitude',
    'mie_wind_result_bottom_range',
    'mie_wind_result_COG_range',
    'mie_wind_result_top_range',
    'mie_wind_result_start_latitude',
    'mie_wind_result_COG_latitude',
    'mie_wind_result_stop_latitude',
    'mie_wind_result_start_longitude',
    'mie_wind_result_COG_longitude',
    'mie_wind_result_stop_longitude',
    'mie_wind_result_lat_of_DEM_intersection',
    'mie_wind_result_lon_of_DEM_intersection',
    'mie_wind_result_geoid_separation',
    'mie_wind_result_alt_of_DEM_intersection',
    'mie_wind_result_HLOS_error',
    'mie_wind_result_QC_flags_1',
    'mie_wind_result_QC_flags_2',
    'mie_wind_result_QC_flags_3',
    'mie_wind_result_SNR',
    'mie_wind_result_scattering_ratio',
    'mie_wind_result_observation_type',
    'mie_wind_result_validity_flag',
    'mie_wind_result_wind_velocity',
    'mie_wind_result_integration_length',
    'mie_wind_result_num_of_measurements',
])

RAYLEIGH_WIND_FIELDS = set([
    'rayleigh_wind_result_id',
    'rayleigh_wind_result_range_bin_number',
    'rayleigh_wind_result_start_time',
    'rayleigh_wind_result_COG_time',
    'rayleigh_wind_result_stop_time',
    'rayleigh_wind_result_bottom_altitude',
    'rayleigh_wind_result_COG_altitude',
    'rayleigh_wind_result_top_altitude',
    'rayleigh_wind_result_bottom_range',
    'rayleigh_wind_result_COG_range',
    'rayleigh_wind_result_top_range',
    'rayleigh_wind_result_start_latitude',
    'rayleigh_wind_result_COG_latitude',
    'rayleigh_wind_result_stop_latitude',
    'rayleigh_wind_result_start_longitude',
    'rayleigh_wind_result_COG_longitude',
    'rayleigh_wind_result_stop_longitude',
    'rayleigh_wind_result_lat_of_DEM_intersection',
    'rayleigh_wind_result_lon_of_DEM_intersection',
    'rayleigh_wind_result_geoid_separation',
    'rayleigh_wind_result_alt_of_DEM_intersection',
    'rayleigh_wind_result_HLOS_error',
    'rayleigh_wind_result_QC_flags_1',
    'rayleigh_wind_result_QC_flags_2',
    'rayleigh_wind_result_QC_flags_3',
    'rayleigh_wind_result_scattering_ratio',
    'rayleigh_wind_result_observation_type',
    'rayleigh_wind_result_validity_flag',
    'rayleigh_wind_result_wind_velocity',
    'rayleigh_wind_result_integration_length',
    'rayleigh_wind_result_num_of_measurements',
    'rayleigh_wind_result_reference_pressure',
    'rayleigh_wind_result_reference_temperature',
    'rayleigh_wind_result_reference_backscatter_ratio',
])

MEASUREMENT_FIELDS = set([
    'mie_measurement_map',
    'rayleigh_measurement_map',
    'mie_measurement_weight',
    'rayleigh_measurement_weight',
    'l1B_num_of_measurements_per_obs',
    'l1B_obs_number',
    'optical_prop_algo_extinction',
    'optical_prop_algo_scattering_ratio',
    'optical_prop_crosstalk_detected',
])


def _array_to_list(data):
    if isinstance(data, np.ndarray):
        isobject = data.dtype == np.object
        data = data.tolist()
        if isobject:
            data = [
                _array_to_list(obj) for obj in data
            ]
    return data


def join_mask(cf, wind_mask, profile_mask, grouping_mask, measurement_mask,
              is_mie):
    if is_mie:
        profile_to_wind_field = 'mie_wind_profile_wind_result_id'
        measurement_to_wind_field = 'mie_measurement_map'
    else:
        profile_to_wind_field = 'rayleigh_wind_profile_wind_result_id'
        measurement_to_wind_field = 'rayleigh_measurement_map'

    pass


def extract_data(filenames, filters,
                 mie_grouping_fields, rayleigh_grouping_fields,
                 mie_profile_fields, rayleigh_profile_fields,
                 mie_wind_fields, rayleigh_wind_fields, measurement_fields,
                 convert_arrays=False):

    filenames = [filenames] if isinstance(filenames, basestring) else filenames

    mie_grouping_data = defaultdict(list)
    mie_grouping_filters = {
        name: value
        for name, value in filters.items() if name in MIE_GROUPING_FIELDS
    }

    rayleigh_grouping_data = defaultdict(list)
    rayleigh_grouping_filters = {
        name: value
        for name, value in filters.items() if name in RAYLEIGH_GROUPING_FIELDS
    }

    mie_profile_data = defaultdict(list)
    mie_profile_filters = {
        name: value
        for name, value in filters.items() if name in MIE_PROFILE_FIELDS
    }

    rayleigh_profile_data = defaultdict(list)
    rayleigh_profile_filters = {
        name: value
        for name, value in filters.items() if name in RAYLEIGH_PROFILE_FIELDS
    }

    mie_wind_data = defaultdict(list)
    mie_wind_filters = {
        name: value
        for name, value in filters.items() if name in MIE_WIND_FIELDS
    }

    rayleigh_wind_data = defaultdict(list)
    rayleigh_wind_filters = {
        name: value
        for name, value in filters.items() if name in RAYLEIGH_WIND_FIELDS
    }

    measurement_data = defaultdict(list)
    measurement_filters = {
        name: value
        for name, value in filters.items() if name in MEASUREMENT_FIELDS
    }

    filters_and_fields_and_output = (
        (mie_grouping_filters, mie_grouping_fields, mie_grouping_data),
        (rayleigh_grouping_filters, rayleigh_grouping_fields, rayleigh_grouping_data),
        (mie_profile_filters, mie_profile_fields, mie_profile_data),
        (rayleigh_profile_filters, rayleigh_profile_fields, rayleigh_profile_data),
        (mie_wind_filters, mie_wind_fields, mie_wind_data),
        (rayleigh_wind_filters, rayleigh_wind_fields, rayleigh_wind_data),
        (measurement_filters, measurement_fields, measurement_data),
    )

    for cf in [CODAFile(filename) for filename in filenames]:
        with cf:
            mie_grouping_mask = create_type_mask(cf, mie_grouping_filters)
            mie_profile_mask = create_type_mask(cf, mie_profile_filters)
            mie_wind_mask = create_type_mask(cf, mie_wind_filters)
            rayleigh_grouping_mask = create_type_mask(cf, rayleigh_grouping_filters)
            rayleigh_profile_mask = create_type_mask(cf, rayleigh_profile_filters)
            rayleigh_wind_mask = create_type_mask(cf, rayleigh_wind_filters)
            measurement_mask = create_type_mask(cf, measurement_filters)

            # mie profile to mie wind mask
            if mie_profile_mask is not None:
                wind_ids = cf.fetch(
                    *locations['mie_wind_profile_wind_result_id']
                )
                filtered = wind_ids[np.nonzero(mie_profile_mask)]
                stacked = np.hstack(filtered)

                new_mask = np.zeros(
                    (cf.fetch('/sph/NumMieWindResults'),), np.bool
                )
                new_mask[stacked[stacked != 0] - 1] = True

                mie_wind_mask = combine_mask(new_mask, mie_wind_mask)

            make_outputs(
                cf, mie_grouping_fields,
                mie_grouping_data, mie_grouping_mask
            )
            make_outputs(
                cf, mie_profile_fields,
                mie_profile_data, mie_profile_mask
            )
            make_outputs(
                cf, mie_wind_fields,
                mie_wind_data, mie_wind_mask
            )
            make_outputs(
                cf, rayleigh_grouping_fields,
                rayleigh_grouping_data, rayleigh_grouping_mask
            )
            make_outputs(
                cf, rayleigh_profile_fields,
                rayleigh_profile_data, rayleigh_profile_mask
            )
            make_outputs(
                cf, rayleigh_wind_fields,
                rayleigh_wind_data, rayleigh_wind_mask
            )
            make_outputs(
                cf, measurement_fields,
                measurement_data, measurement_mask
            )






            # for filters, fields, output in filters_and_fields_and_output:
            #     mask = None
            #     for field, filter_value in filters.items():
            #         new_mask = make_mask(
            #             data=cf.fetch(*locations[field]),
            #             **filter_value
            #         )
            #         mask = combine_mask(new_mask, mask)

            #     ids = np.nonzero(mask)

            #     for field in fields:
            #         data = cf.fetch(*locations[field])
            #         if mask is not None:
            #             data = data[ids]

            #         output[field].extend(_array_to_list(data))



            # filters, fields, output = (
            #     mie_profile_filters, mie_profile_fields, mie_profile_data
            # )
            # profile_mask = None
            # for field, filter_value in filters.items():
            #     new_mask = make_mask(
            #         data=cf.fetch(*locations[field]),
            #         **filter_value
            #     )
            #     profile_mask = combine_mask(new_mask, profile_mask)

            # ids = np.nonzero(profile_mask)

            # for field in fields:
            #     data = cf.fetch(*locations[field])
            #     if profile_mask is not None:
            #         data = data[ids]

            #     output[field].extend(_array_to_list(data))



            # filters, fields, output = (
            #     mie_wind_filters, mie_wind_fields, mie_wind_data
            # )
            # wind_mask = None
            # for field, filter_value in filters.items():
            #     new_mask = make_mask(
            #         data=cf.fetch(*locations[field]),
            #         **filter_value
            #     )
            #     wind_mask = combine_mask(new_mask, wind_mask)

            # ids = np.nonzero(wind_mask)

            # for field in fields:
            #     data = cf.fetch(*locations[field])
            #     if wind_mask is not None:
            #         data = data[ids]

            #     output[field].extend(_array_to_list(data))


    return (
        mie_grouping_data,
        rayleigh_grouping_data,
        mie_profile_data,
        rayleigh_profile_data,
        mie_wind_data,
        rayleigh_wind_data,
        measurement_data,
    )



def create_type_mask(cf, filters):
    mask = None
    for field, filter_value in filters.items():
        new_mask = make_mask(
            data=cf.fetch(*locations[field]),
            **filter_value
        )
        mask = combine_mask(new_mask, mask)

    return mask


def make_outputs(cf, fields, output, mask=None):
    ids = np.nonzero(mask) if mask is not None else None
    for field in fields:
        data = cf.fetch(*locations[field])

        print data.shape
        if mask is not None:
            data = data[ids]

        print data.shape

        print field, mask

        output[field].extend(_array_to_list(data))






test_file = '/mnt/data/AE_OPER_ALD_U_N_2B_20151001T104454_20151001T121445_0001/AE_OPER_ALD_U_N_2B_20151001T104454_20151001T121445_0001.DBL'


# test_file = '/mnt/data/AE_OPER_ALD_U_N_2A_20101002T000000059_000083999_017071_0001.DBL'

# test_file = '/mnt/data/AE_OPER_ALD_U_N_2A_20101002T000000059_001188000_017071_0001.DBL'

def main():
    extract_data(
        test_file, {
            # 'mie_wind_result_id': {
            #     'min_value': 1,
            #     'max_value': 10
            # },


            'mie_wind_profile_observation_type': {
                'min_value': 1,
                'max_value': 1
            }
        },
        mie_grouping_fields=[],
        rayleigh_grouping_fields=[],
        mie_profile_fields=[
            # 'mie_wind_profile_observation_type',
            # 'mie_wind_profile_wind_result_id'
        ],
        rayleigh_profile_fields=[],
        mie_wind_fields=['mie_wind_result_id'],
        rayleigh_wind_fields=[],
        measurement_fields=[],
    )

if __name__ == '__main__':
    main()
