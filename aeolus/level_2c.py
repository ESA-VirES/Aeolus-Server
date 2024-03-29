# ------------------------------------------------------------------------------
#
#  Aeolus - Level 2C data extraction
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

import math

import numpy as np

from aeolus.coda_utils import access_location
from aeolus.albedo import sample_offnadir
from aeolus.extraction.accumulated import AccumulatedDataExtractor


def _make_profile_from_wind_calc(id_field, value_field):
    def _inner(cf):
        profile_ids = cf.fetch(*locations[id_field])
        values = cf.fetch(*locations[value_field])

        out = np.empty(profile_ids.shape[0])
        for i, ids in enumerate(profile_ids):
            out[i] = values[ids[np.nonzero(ids)][0] - 1]

        return out
    return _inner


calc_mie_profile_lat_of_DEM_intersection = _make_profile_from_wind_calc(
    'mie_wind_profile_wind_result_id',
    'mie_wind_result_lat_of_DEM_intersection'
)

calc_mie_profile_lon_of_DEM_intersection = _make_profile_from_wind_calc(
    'mie_wind_profile_wind_result_id',
    'mie_wind_result_lon_of_DEM_intersection'
)

calc_mie_profile_geoid_separation = _make_profile_from_wind_calc(
    'mie_wind_profile_wind_result_id',
    'mie_wind_result_geoid_separation'
)

calc_mie_profile_alt_of_DEM_intersection = _make_profile_from_wind_calc(
    'mie_wind_profile_wind_result_id',
    'mie_wind_result_alt_of_DEM_intersection'
)

calc_rayleigh_profile_lat_of_DEM_intersection = _make_profile_from_wind_calc(
    'rayleigh_wind_profile_wind_result_id',
    'rayleigh_wind_result_lat_of_DEM_intersection'
)

calc_rayleigh_profile_lon_of_DEM_intersection = _make_profile_from_wind_calc(
    'rayleigh_wind_profile_wind_result_id',
    'rayleigh_wind_result_lon_of_DEM_intersection'
)

calc_rayleigh_profile_geoid_separation = _make_profile_from_wind_calc(
    'rayleigh_wind_profile_wind_result_id',
    'rayleigh_wind_result_geoid_separation'
)

calc_rayleigh_profile_alt_of_DEM_intersection = _make_profile_from_wind_calc(
    'rayleigh_wind_profile_wind_result_id',
    'rayleigh_wind_result_alt_of_DEM_intersection'
)


def _calc_velocity(u, v):
    u = u.astype(np.float32)
    v = v.astype(np.float32)
    return np.sqrt(np.square(u) + np.square(v))


def _calc_direction(u, v):
    u = u.astype(np.float32)
    v = v.astype(np.float32)
    return (180 / math.pi) * np.arctan2(u, v) + 180


def calc_mie_assimilation_background_horizontal_wind_velocity(cf):
    u = cf.fetch(*locations['mie_assimilation_background_u_wind_velocity'])
    v = cf.fetch(*locations['mie_assimilation_background_v_wind_velocity'])
    return _calc_velocity(u, v)


def calc_mie_assimilation_background_wind_direction(cf):
    u = cf.fetch(*locations['mie_assimilation_background_u_wind_velocity'])
    v = cf.fetch(*locations['mie_assimilation_background_v_wind_velocity'])
    return _calc_direction(u, v)


def calc_mie_assimilation_analysis_horizontal_wind_velocity(cf):
    u = cf.fetch(*locations['mie_assimilation_analysis_u_wind_velocity'])
    v = cf.fetch(*locations['mie_assimilation_analysis_v_wind_velocity'])
    return _calc_velocity(u, v)


def calc_mie_assimilation_analysis_wind_direction(cf):
    u = cf.fetch(*locations['mie_assimilation_analysis_u_wind_velocity'])
    v = cf.fetch(*locations['mie_assimilation_analysis_v_wind_velocity'])
    return _calc_direction(u, v)


def calc_rayleigh_assimilation_background_horizontal_wind_velocity(cf):
    u = cf.fetch(*locations['rayleigh_assimilation_background_u_wind_velocity'])
    v = cf.fetch(*locations['rayleigh_assimilation_background_v_wind_velocity'])
    return _calc_velocity(u, v)


def calc_rayleigh_assimilation_background_wind_direction(cf):
    u = cf.fetch(*locations['rayleigh_assimilation_background_u_wind_velocity'])
    v = cf.fetch(*locations['rayleigh_assimilation_background_v_wind_velocity'])
    return _calc_direction(u, v)


def calc_rayleigh_assimilation_analysis_horizontal_wind_velocity(cf):
    u = cf.fetch(*locations['rayleigh_assimilation_analysis_u_wind_velocity'])
    v = cf.fetch(*locations['rayleigh_assimilation_analysis_v_wind_velocity'])
    return _calc_velocity(u, v)


def calc_rayleigh_assimilation_analysis_wind_direction(cf):
    u = cf.fetch(*locations['rayleigh_assimilation_analysis_u_wind_velocity'])
    v = cf.fetch(*locations['rayleigh_assimilation_analysis_v_wind_velocity'])
    return _calc_direction(u, v)


def _make_calc_albedo_off_nadir(lon_location, lat_location):
    def _inner(cf):
        start = cf.fetch_date('/mph/sensing_start')
        stop = cf.fetch_date('/mph/sensing_stop')

        mean = start + (stop - start) / 2
        lons = access_location(cf,
            locations[lon_location],
        )
        lats = access_location(cf,
            locations[lat_location],
        )

        lons[lons > 180] -= 360
        shape = lons.shape

        if len(shape) > 1:
            lons = lons.flatten()
            lats = lats.flatten()

        data = sample_offnadir(mean.year, mean.month, lons, lats)

        if len(shape) > 1:
            data = data.reshape(shape)

        return data

    return _inner

def _create_temp_granularity(temp_location, ref_location):
    def _inner(cf):
        temperature_data = access_location(cf, temp_location)
        which_cog_data = access_location(cf, ref_location)
        data = temperature_data[which_cog_data-1]
        return data
    return _inner

calc_mie_wind_result_albedo_off_nadir = _make_calc_albedo_off_nadir(
    'mie_wind_result_lon_of_DEM_intersection',
    'mie_wind_result_lat_of_DEM_intersection',
)

calc_rayleigh_wind_result_albedo_off_nadir = _make_calc_albedo_off_nadir(
    'rayleigh_wind_result_lon_of_DEM_intersection',
    'rayleigh_wind_result_lat_of_DEM_intersection',
)


calc_mie_profile_albedo_off_nadir = _make_calc_albedo_off_nadir(
    'mie_profile_lon_of_DEM_intersection',
    'mie_profile_lat_of_DEM_intersection',
)

calc_rayleigh_profile_albedo_off_nadir = _make_calc_albedo_off_nadir(
    'rayleigh_profile_lon_of_DEM_intersection',
    'rayleigh_profile_lat_of_DEM_intersection',
)

def _checkCorrectIdentifier(location, alternative_location):
    def _inner(cf):
        try:
            values = cf.fetch(*location)
        except Exception as e:
            try:
                values = cf.fetch(*alternative_location)
            except Exception as e:
                raise e

        return values
    return _inner



locations = {
    'mie_measurement_map':                                          ['/meas_map', -1, 'mie_map_of_l1b_meas_used', -1, 'which_l2b_wind_id'],
    'rayleigh_measurement_map':                                     ['/meas_map', -1, 'rayleigh_map_of_l1b_meas_used', -1, 'which_l2b_wind_id'],
    'mie_measurement_weight':                                       ['/meas_map', -1, 'mie_map_of_l1b_meas_used', -1, 'weight'],
    'rayleigh_measurement_weight':                                  ['/meas_map', -1, 'rayleigh_map_of_l1b_meas_used', -1, 'weight'],
    'mie_grouping_id':                                              ['/mie_grouping', -1, 'grouping_result_id'],
    'mie_grouping_time':                                            ['/mie_grouping', -1, 'start_of_obs_datetime'],
    'mie_grouping_start_obs':                                       ['/mie_grouping', -1, 'which_l1b_brc1'],
    'mie_grouping_start_meas_obs':                                  ['/mie_grouping', -1, 'which_l1b_meas_within_this_brc1'],
    'mie_grouping_end_obs':                                         ['/mie_grouping', -1, 'which_l1b_brc2'],
    'mie_grouping_end_meas_obs':                                    ['/mie_grouping', -1, 'which_l1b_meas_within_this_brc2'],
    'rayleigh_grouping_id':                                         ['/rayleigh_grouping', -1, 'grouping_result_id'],
    'rayleigh_grouping_time':                                       ['/rayleigh_grouping', -1, 'start_of_obs_datetime'],
    'rayleigh_grouping_start_obs':                                  ['/rayleigh_grouping', -1, 'which_l1b_brc1'],
    'rayleigh_grouping_start_meas_obs':                             ['/rayleigh_grouping', -1, 'which_l1b_meas_within_this_brc1'],
    'rayleigh_grouping_end_obs':                                    ['/rayleigh_grouping', -1, 'which_l1b_brc2'],
    'rayleigh_grouping_end_meas_obs':                               ['/rayleigh_grouping', -1, 'which_l1b_meas_within_this_brc2'],
    'l1B_num_of_measurements_per_obs':                              ['/meas_product_confid_data', -1, 'l1b_meas_number'],
    'l1B_obs_number':                                               ['/meas_product_confid_data', -1, 'l1b_brc_number'],
    'mie_wind_result_id':                                           ['/mie_geolocation', -1, 'wind_result_id'],
    'mie_wind_profile_wind_result_id':                              ['/mie_profile', -1, 'l2b_wind_profiles/wind_result_id_number', -1],
    'mie_wind_result_range_bin_number':                             ['/mie_hloswind', -1, 'windresult/which_range_bin'],
    'mie_wind_result_start_time':                                   ['/mie_geolocation', -1, 'windresult_geolocation/datetime_start'],
    'mie_wind_result_COG_time':                                     ['/mie_geolocation', -1, 'windresult_geolocation/datetime_cog'],
    'mie_wind_result_stop_time':                                    ['/mie_geolocation', -1, 'windresult_geolocation/datetime_stop'],
    'mie_wind_result_bottom_altitude':                              ['/mie_geolocation', -1, 'windresult_geolocation/altitude_bottom'],
    'mie_wind_result_COG_altitude':                                 ['/mie_geolocation', -1, 'windresult_geolocation/altitude_vcog'],
    'mie_wind_result_top_altitude':                                 ['/mie_geolocation', -1, 'windresult_geolocation/altitude_top'],
    'mie_wind_result_bottom_range':                                 ['/mie_geolocation', -1, 'windresult_geolocation/satrange_bottom'],
    'mie_wind_result_COG_range':                                    ['/mie_geolocation', -1, 'windresult_geolocation/satrange_vcog'],
    'mie_wind_result_top_range':                                    ['/mie_geolocation', -1, 'windresult_geolocation/satrange_top'],
    'mie_wind_result_start_latitude':                               ['/mie_geolocation', -1, 'windresult_geolocation/latitude_start'],
    'mie_wind_result_COG_latitude':                                 ['/mie_geolocation', -1, 'windresult_geolocation/latitude_cog'],
    'mie_wind_result_stop_latitude':                                ['/mie_geolocation', -1, 'windresult_geolocation/latitude_stop'],
    'mie_wind_result_start_longitude':                              ['/mie_geolocation', -1, 'windresult_geolocation/longitude_start'],
    'mie_wind_result_COG_longitude':                                ['/mie_geolocation', -1, 'windresult_geolocation/longitude_cog'],
    'mie_wind_result_stop_longitude':                               ['/mie_geolocation', -1, 'windresult_geolocation/longitude_stop'],
    'mie_wind_result_lat_of_DEM_intersection':                      ['/mie_geolocation', -1, 'windresult_geolocation/lat_of_dem_intersection'],
    'mie_wind_result_lon_of_DEM_intersection':                      ['/mie_geolocation', -1, 'windresult_geolocation/lon_of_dem_intersection'],
    'mie_wind_result_geoid_separation':                             ['/mie_geolocation', -1, 'windresult_geolocation/wgs84_to_geoid_altitude'],
    'mie_wind_result_alt_of_DEM_intersection':                      ['/mie_geolocation', -1, 'windresult_geolocation/alt_of_dem_intersection'],
    'mie_wind_result_arg_of_lat_of_DEM_intersection':               ['/mie_geolocation', -1, 'windresult_geolocation/arg_of_lat_of_dem_intersection'],
    'mie_wind_result_los_azimuth':                                  ['/mie_geolocation', -1, 'windresult_geolocation/los_azimuth'],
    'mie_wind_result_los_elevation_top':                            ['/mie_geolocation', -1, 'windresult_geolocation/los_elevation_top'],
    'mie_wind_result_los_elevation_bottom':                         ['/mie_geolocation', -1, 'windresult_geolocation/los_elevation_bottom'],
    'mie_wind_result_los_elevation_vcog':                           ['/mie_geolocation', -1, 'windresult_geolocation/los_elevation_vcog'],
    'mie_wind_result_which_cog_l1b_brc':                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
    'mie_wind_result_satellite_los_velocity':                       ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_meas_in_this_brc'],
    'mie_profile_lat_of_DEM_intersection':                          calc_mie_profile_lat_of_DEM_intersection,
    'mie_profile_lon_of_DEM_intersection':                          calc_mie_profile_lon_of_DEM_intersection,
    'mie_profile_geoid_separation':                                 calc_mie_profile_geoid_separation,
    'mie_profile_alt_of_DEM_intersection':                          calc_mie_profile_alt_of_DEM_intersection,
    'rayleigh_wind_result_id':                                      ['/rayleigh_geolocation', -1, 'wind_result_id'],
    'rayleigh_wind_profile_wind_result_id':                         ['/rayleigh_profile', -1, 'l2b_wind_profiles/wind_result_id_number', -1],
    'rayleigh_wind_result_range_bin_number':                        ['/rayleigh_hloswind', -1, 'windresult/which_range_bin'],
    'rayleigh_wind_result_start_time':                              ['/rayleigh_geolocation', -1, 'windresult_geolocation/datetime_start'],
    'rayleigh_wind_result_COG_time':                                ['/rayleigh_geolocation', -1, 'windresult_geolocation/datetime_cog'],
    'rayleigh_wind_result_stop_time':                               ['/rayleigh_geolocation', -1, 'windresult_geolocation/datetime_stop'],
    'rayleigh_wind_result_bottom_altitude':                         ['/rayleigh_geolocation', -1, 'windresult_geolocation/altitude_bottom'],
    'rayleigh_wind_result_COG_altitude':                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/altitude_vcog'],
    'rayleigh_wind_result_top_altitude':                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/altitude_top'],
    'rayleigh_wind_result_bottom_range':                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/satrange_bottom'],
    'rayleigh_wind_result_COG_range':                               ['/rayleigh_geolocation', -1, 'windresult_geolocation/satrange_vcog'],
    'rayleigh_wind_result_top_range':                               ['/rayleigh_geolocation', -1, 'windresult_geolocation/satrange_top'],
    'rayleigh_wind_result_start_latitude':                          ['/rayleigh_geolocation', -1, 'windresult_geolocation/latitude_start'],
    'rayleigh_wind_result_COG_latitude':                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/latitude_cog'],
    'rayleigh_wind_result_stop_latitude':                           ['/rayleigh_geolocation', -1, 'windresult_geolocation/latitude_stop'],
    'rayleigh_wind_result_start_longitude':                         ['/rayleigh_geolocation', -1, 'windresult_geolocation/longitude_start'],
    'rayleigh_wind_result_COG_longitude':                           ['/rayleigh_geolocation', -1, 'windresult_geolocation/longitude_cog'],
    'rayleigh_wind_result_stop_longitude':                          ['/rayleigh_geolocation', -1, 'windresult_geolocation/longitude_stop'],
    'rayleigh_wind_result_lat_of_DEM_intersection':                 ['/rayleigh_geolocation', -1, 'windresult_geolocation/lat_of_dem_intersection'],
    'rayleigh_wind_result_lon_of_DEM_intersection':                 ['/rayleigh_geolocation', -1, 'windresult_geolocation/lon_of_dem_intersection'],
    'rayleigh_wind_result_geoid_separation':                        ['/rayleigh_geolocation', -1, 'windresult_geolocation/wgs84_to_geoid_altitude'],
    'rayleigh_wind_result_alt_of_DEM_intersection':                 ['/rayleigh_geolocation', -1, 'windresult_geolocation/alt_of_dem_intersection'],
    'rayleigh_wind_result_arg_of_lat_of_DEM_intersection':          ['/rayleigh_geolocation', -1, 'windresult_geolocation/arg_of_lat_of_dem_intersection'],
    'rayleigh_wind_result_los_azimuth':                             ['/rayleigh_geolocation', -1, 'windresult_geolocation/los_azimuth'],
    'rayleigh_wind_result_los_elevation_top':                       ['/rayleigh_geolocation', -1, 'windresult_geolocation/los_elevation_top'],
    'rayleigh_wind_result_los_elevation_bottom':                    ['/rayleigh_geolocation', -1, 'windresult_geolocation/los_elevation_bottom'],
    'rayleigh_wind_result_los_elevation_vcog':                      ['/rayleigh_geolocation', -1, 'windresult_geolocation/los_elevation_vcog'],
    'rayleigh_wind_result_which_cog_l1b_brc':                       ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
    'rayleigh_wind_result_satellite_los_velocity':                  ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_meas_in_this_brc'],
    'rayleigh_profile_alt_of_DEM_intersection':                     calc_rayleigh_profile_alt_of_DEM_intersection,
    'rayleigh_profile_lat_of_DEM_intersection':                     calc_rayleigh_profile_lat_of_DEM_intersection,
    'rayleigh_profile_lon_of_DEM_intersection':                     calc_rayleigh_profile_lon_of_DEM_intersection,
    'rayleigh_profile_geoid_separation':                            calc_rayleigh_profile_geoid_separation,
    'l1B_measurement_time':                                         ['/meas_product_confid_data', -1, 'start_of_obs_datetime'],
    # TODO: mie_bin_classification and rayleigh_bin_classification not specified
    # 'mie_bin_classification':                                       ['/meas_product_confid_data', -1, 'l2b_mie_classification_qc/l2b_mie_meas_bin_classification', -1, 'l2b_mie_meas_bin_class_flags1'],
    # 'rayleigh_bin_classification':                                  ['/meas_product_confid_data', -1, 'l2b_rayleigh_classification_qc/l2b_rayleigh_meas_bin_classification', -1, 'l2b_rayleigh_meas_bin_class_flags1'],
    'optical_prop_algo_extinction':                                 _checkCorrectIdentifier(
                                                                        ['/meas_product_confid_data', -1, 'opt_prop_result/extinction_iterative'],
                                                                        ['/meas_product_confid_data', -1, 'opt_prop_result/opt_prop_meas_result', -1, 'extinction_iterative']
                                                                    ),
    'optical_prop_algo_scattering_ratio':                           _checkCorrectIdentifier(
                                                                        ['/meas_product_confid_data', -1, 'opt_prop_result/scattering_ratio_iterative'],
                                                                        ['/meas_product_confid_data', -1, 'opt_prop_result/opt_prop_meas_result', -1, 'scattering_ratio_iterative']
                                                                    ),
    'optical_prop_crosstalk_detected':                              _checkCorrectIdentifier(
                                                                        ['/meas_product_confid_data', -1, 'opt_prop_result/xtalk_detected'],
                                                                        ['/meas_product_confid_data', -1, 'opt_prop_result/opt_prop_meas_result', -1, 'xtalk_detected']
                                                                    ),
    'mie_wind_result_HLOS_error':                                   ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/hlos_error_estimate'],
    'mie_wind_result_reference_hlos':                               ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/reference_hlos'],
    'mie_wind_result_QC_flags_1':                                   ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/flags1'],
    'mie_wind_result_QC_flags_2':                                   ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/flags2'],
    'mie_wind_result_QC_flags_3':                                   ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/flags3'],
    'mie_wind_result_SNR':                                          _checkCorrectIdentifier(
                                                                        ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/mie_snr'],
                                                                        ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/intref_fitting_mie_snr']
                                                                    ),
    'mie_wind_result_scattering_ratio':                             _checkCorrectIdentifier(
                                                                        ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/scattering_ratio'],
                                                                        ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/intref_fitting_mie_sr']
                                                                    ),
    'mie_wind_result_extinction':                                   ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/extinction'],
    'mie_wind_result_background_high':                              ['/mie_wind_prod_conf_data', -1, 'mie_wind_qc/mie_background_high'],
    'rayleigh_wind_result_HLOS_error':                              ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/hlos_error_estimate'],
    'rayleigh_wind_result_reference_hlos':                          ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/reference_hlos'],
    'rayleigh_wind_result_QC_flags_1':                              ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/flags1'],
    'rayleigh_wind_result_QC_flags_2':                              ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/flags2'],
    'rayleigh_wind_result_QC_flags_3':                              ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/flags3'],
    'rayleigh_wind_result_scattering_ratio':                        ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/scattering_ratio'],
    'rayleigh_wind_result_background_high':                         ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/rayleigh_background_high'],
    'rayleigh_wind_result_scattering_ratio_method':                 ['/rayleigh_wind_prod_conf_data', -1, 'rayleigh_wind_qc/scattering_ratio_method'],
    'mie_assimilation_L2B_QC':                                      ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/l2b_mie_obs_screening/l2b_mie_obs_qc'],
    'mie_assimilation_persistence_error':                           ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/hlos_observation_errors/persistence_error'],
    'mie_assimilation_representativity_error':                      ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/hlos_observation_errors/representativity_error'],
    'mie_assimilation_final_error':                                 ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/hlos_observation_errors/final_error'],
    'mie_assimilation_est_L2B_bias':                                ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/hlos_observation_errors/estimated_obs_bias'],
    'mie_assimilation_background_HLOS_error':                       ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/background_hlos_error'],
    'mie_assimilation_L2B_HLOS_reliability':                        ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/l2b_hlos_reliability'],
    'mie_assimilation_u_wind_background_error':                     ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/zonal_wind_background_error'],
    'mie_assimilation_v_wind_background_error':                     ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/meridional_wind_background_error'],
    'rayleigh_assimilation_L2B_QC':                                 ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/l2b_rayleigh_obs_screening/l2b_rayleigh_obs_qc'],
    'rayleigh_assimilation_persistence_error':                      ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/hlos_observation_errors/persistence_error'],
    'rayleigh_assimilation_representativity_error':                 ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/hlos_observation_errors/representativity_error'],
    'rayleigh_assimilation_final_error':                            ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/hlos_observation_errors/final_error'],
    'rayleigh_assimilation_est_L2B_bias':                           ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/hlos_observation_errors/estimated_obs_bias'],
    'rayleigh_assimilation_background_HLOS_error':                  ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/background_hlos_error'],
    'rayleigh_assimilation_L2B_HLOS_reliability':                   ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/l2b_hlos_reliability'],
    'rayleigh_assimilation_u_wind_background_error':                ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/zonal_wind_background_error'],
    'rayleigh_assimilation_v_wind_background_error':                ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/meridional_wind_background_error'],
    'mie_wind_result_observation_type':                             ['/mie_hloswind', -1, 'windresult/observation_type'],
    'mie_wind_result_validity_flag':                                ['/mie_hloswind', -1, 'windresult/validity_flag'],
    'mie_wind_result_wind_velocity':                                ['/mie_hloswind', -1, 'windresult/mie_wind_velocity'],
    'mie_wind_result_applied_spacecraft_los_corr_velocity':         ['/mie_hloswind', -1, 'windresult/applied_spacecraft_los_corr_velocity'],
    'mie_wind_result_applied_m1_temperature_corr_velocity':         ['/mie_hloswind', -1, 'windresult/applied_m1_temperature_corr_velocity'],
    'mie_wind_result_integration_length':                           ['/mie_hloswind', -1, 'windresult/integration_length'],
    'mie_wind_result_num_of_measurements':                          ['/mie_hloswind', -1, 'windresult/n_meas_in_class'],
    'rayleigh_wind_result_observation_type':                        ['/rayleigh_hloswind', -1, 'windresult/observation_type'],
    'rayleigh_wind_result_validity_flag':                           ['/rayleigh_hloswind', -1, 'windresult/validity_flag'],
    'rayleigh_wind_result_wind_velocity':                           ['/rayleigh_hloswind', -1, 'windresult/rayleigh_wind_velocity'],
    'rayleigh_wind_result_wind_to_pressure':                        ['/rayleigh_hloswind', -1, 'windresult/rayleigh_wind_to_pressure'],
    'rayleigh_wind_result_wind_to_temperature':                     ['/rayleigh_hloswind', -1, 'windresult/rayleigh_wind_to_temperature'],
    'rayleigh_wind_result_wind_to_backscatter_ratio':               ['/rayleigh_hloswind', -1, 'windresult/rayleigh_wind_to_backscatter_ratio'],
    'rayleigh_wind_result_applied_spacecraft_los_corr_velocity':    ['/rayleigh_hloswind', -1, 'windresult/applied_spacecraft_los_corr_velocity'],
    'rayleigh_wind_result_applied_rdb_corr_velocity':               ['/rayleigh_hloswind', -1, 'windresult/applied_rdb_corr_velocity'],
    'rayleigh_wind_result_applied_ground_corr_velocity':            ['/rayleigh_hloswind', -1, 'windresult/applied_ground_corr_velocity'],
    'rayleigh_wind_result_applied_m1_temperature_corr_velocity':    ['/rayleigh_hloswind', -1, 'windresult/applied_m1_temperature_corr_velocity'],
    'rayleigh_wind_result_integration_length':                      ['/rayleigh_hloswind', -1, 'windresult/integration_length'],
    'rayleigh_wind_result_num_of_measurements':                     ['/rayleigh_hloswind', -1, 'windresult/n_meas_in_class'],
    'rayleigh_wind_result_reference_pressure':                      ['/rayleigh_hloswind', -1, 'windresult/reference_pressure'],
    'rayleigh_wind_result_reference_temperature':                   ['/rayleigh_hloswind', -1, 'windresult/reference_temperature'],
    'rayleigh_wind_result_reference_backscatter_ratio':             ['/rayleigh_hloswind', -1, 'windresult/reference_backscatter_ratio'],
    'mie_wind_profile_observation_type':                            ['/mie_profile', -1, 'l2b_wind_profiles/obs_type'],
    'rayleigh_wind_profile_observation_type':                       ['/rayleigh_profile', -1, 'l2b_wind_profiles/obs_type'],
    'mie_assimilation_validity_flag':                               ['/mie_vecwind', -1, 'height_bin_vecwind/validity_flag'],
    'mie_assimilation_background_HLOS':                             ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/background_hlos'],
    'mie_assimilation_background_u_wind_velocity':                  ['/mie_vecwind', -1, 'height_bin_vecwind/background_zonal_wind_velocity'],
    'mie_assimilation_background_v_wind_velocity':                  ['/mie_vecwind', -1, 'height_bin_vecwind/background_meridional_wind_velocity'],
    'mie_assimilation_background_horizontal_wind_velocity':         calc_mie_assimilation_background_horizontal_wind_velocity,
    'mie_assimilation_background_wind_direction':                   calc_mie_assimilation_background_wind_direction,
    'mie_assimilation_analysis_HLOS':                               ['/mie_assim_pcd', -1, 'l2c_mie_quality_param/l2c_mie_height_bin_quality_param/assimilation_model_pcd/Analysis_hlos'],
    'mie_assimilation_analysis_u_wind_velocity':                    ['/mie_vecwind', -1, 'height_bin_vecwind/analysis_zonal_wind_velocity'],
    'mie_assimilation_analysis_v_wind_velocity':                    ['/mie_vecwind', -1, 'height_bin_vecwind/analysis_meridional_wind_velocity'],
    'mie_assimilation_analysis_horizontal_wind_velocity':           calc_mie_assimilation_analysis_horizontal_wind_velocity,
    'mie_assimilation_analysis_wind_direction':                     calc_mie_assimilation_analysis_wind_direction,
    'rayleigh_assimilation_validity_flag':                          ['/rayleigh_vecwind', -1, 'height_bin_vecwind/validity_flag'],
    'rayleigh_assimilation_background_HLOS':                        ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/background_hlos'],
    'rayleigh_assimilation_background_u_wind_velocity':             ['/rayleigh_vecwind', -1, 'height_bin_vecwind/background_zonal_wind_velocity'],
    'rayleigh_assimilation_background_v_wind_velocity':             ['/rayleigh_vecwind', -1, 'height_bin_vecwind/background_meridional_wind_velocity'],
    'rayleigh_assimilation_background_horizontal_wind_velocity':    calc_rayleigh_assimilation_background_horizontal_wind_velocity,
    'rayleigh_assimilation_background_wind_direction':              calc_rayleigh_assimilation_background_wind_direction,
    'rayleigh_assimilation_analysis_HLOS':                          ['/rayleigh_assim_pcd', -1, 'l2c_rayleigh_quality_param/l2c_rayleigh_height_bin_quality_param/assimilation_model_pcd/Analysis_hlos'],
    'rayleigh_assimilation_analysis_u_wind_velocity':               ['/rayleigh_vecwind', -1, 'height_bin_vecwind/analysis_zonal_wind_velocity'],
    'rayleigh_assimilation_analysis_v_wind_velocity':               ['/rayleigh_vecwind', -1, 'height_bin_vecwind/analysis_meridional_wind_velocity'],
    'rayleigh_assimilation_analysis_horizontal_wind_velocity':      calc_rayleigh_assimilation_analysis_horizontal_wind_velocity,
    'rayleigh_assimilation_analysis_wind_direction':                calc_rayleigh_assimilation_analysis_wind_direction,

    # custom fields:
    'mie_profile_datetime_start':                                   ['/mie_profile', -1, 'profile_datetime_start'],
    'mie_profile_datetime_average':                                 ['/mie_profile', -1, 'profile_datetime_average'],
    'mie_profile_datetime_stop':                                    ['/mie_profile', -1, 'profile_datetime_stop'],
    'rayleigh_profile_datetime_start':                              ['/rayleigh_profile', -1, 'profile_datetime_start'],
    'rayleigh_profile_datetime_average':                            ['/rayleigh_profile', -1, 'profile_datetime_average'],
    'rayleigh_profile_datetime_stop':                               ['/rayleigh_profile', -1, 'profile_datetime_stop'],

    # Albedo
    'mie_wind_result_albedo_off_nadir':                             calc_mie_wind_result_albedo_off_nadir,
    'rayleigh_wind_result_albedo_off_nadir':                        calc_rayleigh_wind_result_albedo_off_nadir,
    'mie_profile_albedo_off_nadir':                                 calc_mie_profile_albedo_off_nadir,
    'rayleigh_profile_albedo_off_nadir':                            calc_rayleigh_profile_albedo_off_nadir,

        # Temperature masked by mie and rayleigh
    'mie_aht_22':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_22'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_aht_23':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_23'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_aht_24':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_24'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_aht_25':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_25'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_aht_26':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_26'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_aht_27':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_27'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_tc_18':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_18'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_tc_19':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_19'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_tc_20':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_20'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_tc_21':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_21'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_tc_23':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_23'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_tc_25':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_25'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_tc_27':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_27'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_tc_29':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_29'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'mie_tc_32':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_32'],
                            ['/mie_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_aht_22':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_22'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_aht_23':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_23'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_aht_24':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_24'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_aht_25':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_25'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_aht_26':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_26'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_aht_27':       _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/aht_27'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_tc_18':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_18'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_tc_19':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_19'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_tc_20':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_20'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_tc_21':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_21'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_tc_23':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_23'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_tc_25':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_25'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_tc_27':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_27'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_tc_29':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_29'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
    'rayleigh_tc_32':        _create_temp_granularity(
                            ['/copied_brc_data', -1, 'm1_temperature/tc_32'],
                            ['/rayleigh_geolocation', -1, 'windresult_geolocation/which_cog_l1b_brc'],
                        ),
}

MIE_GROUPING_FIELDS = set([
    'mie_grouping_id',
    'mie_grouping_time',
    'mie_grouping_start_obs',
    'mie_grouping_start_meas_obs',
    'mie_grouping_end_obs',
    'mie_grouping_end_meas_obs',
])


RAYLEIGH_GROUPING_FIELDS = set([
    'rayleigh_grouping_id',
    'rayleigh_grouping_time',
    'rayleigh_grouping_start_obs',
    'rayleigh_grouping_start_meas_obs',
    'rayleigh_grouping_end_obs',
    'rayleigh_grouping_end_meas_obs',
])

MIE_PROFILE_FIELDS = set([
    'mie_wind_profile_wind_result_id',
    'mie_profile_lat_of_DEM_intersection',
    'mie_profile_lon_of_DEM_intersection',
    'mie_profile_geoid_separation',
    'mie_profile_alt_of_DEM_intersection',
    'mie_wind_profile_observation_type',
    'mie_profile_datetime_start',
    'mie_profile_datetime_average',
    'mie_profile_datetime_stop',
    'mie_profile_albedo_off_nadir',
])

RAYLEIGH_PROFILE_FIELDS = set([
    'rayleigh_wind_profile_wind_result_id',
    'rayleigh_wind_profile_observation_type',
    'rayleigh_profile_lat_of_DEM_intersection',
    'rayleigh_profile_lon_of_DEM_intersection',
    'rayleigh_profile_geoid_separation',
    'rayleigh_profile_alt_of_DEM_intersection',
    'rayleigh_profile_datetime_start',
    'rayleigh_profile_datetime_average',
    'rayleigh_profile_datetime_stop',
    'rayleigh_profile_albedo_off_nadir',
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
    'mie_wind_result_arg_of_lat_of_DEM_intersection',
    'mie_wind_result_HLOS_error',
    'mie_wind_result_reference_hlos',
    'mie_wind_result_QC_flags_1',
    'mie_wind_result_QC_flags_2',
    'mie_wind_result_QC_flags_3',
    'mie_wind_result_SNR',
    'mie_wind_result_scattering_ratio',
    'mie_wind_result_extinction',
    'mie_wind_result_background_high',
    'mie_assimilation_L2B_QC',
    'mie_assimilation_persistence_error',
    'mie_assimilation_representativity_error',
    'mie_assimilation_final_error',
    'mie_assimilation_est_L2B_bias',
    'mie_assimilation_background_HLOS_error',
    'mie_assimilation_L2B_HLOS_reliability',
    'mie_assimilation_u_wind_background_error',
    'mie_assimilation_v_wind_background_error',
    'mie_wind_result_observation_type',
    'mie_wind_result_validity_flag',
    'mie_wind_result_wind_velocity',
    'mie_wind_result_applied_spacecraft_los_corr_velocity',
    'mie_wind_result_applied_m1_temperature_corr_velocity',
    'mie_wind_result_integration_length',
    'mie_wind_result_num_of_measurements',
    'mie_assimilation_validity_flag',
    'mie_assimilation_background_HLOS',
    'mie_assimilation_background_u_wind_velocity',
    'mie_assimilation_background_v_wind_velocity',
    'mie_assimilation_background_horizontal_wind_velocity',
    'mie_assimilation_background_wind_direction',
    'mie_assimilation_analysis_HLOS',
    'mie_assimilation_analysis_u_wind_velocity',
    'mie_assimilation_analysis_v_wind_velocity',
    'mie_assimilation_analysis_horizontal_wind_velocity',
    'mie_assimilation_analysis_wind_direction',
    'mie_wind_result_albedo_off_nadir',
    'mie_aht_22',
    'mie_aht_23',
    'mie_aht_24',
    'mie_aht_25',
    'mie_aht_26',
    'mie_aht_27',
    'mie_tc_18',
    'mie_tc_19',
    'mie_tc_20',
    'mie_tc_21',
    'mie_tc_23',
    'mie_tc_25',
    'mie_tc_27',
    'mie_tc_29',
    'mie_tc_32',
    'mie_wind_result_los_azimuth',
    'mie_wind_result_los_elevation_top',
    'mie_wind_result_los_elevation_bottom',
    'mie_wind_result_los_elevation_vcog',
    'mie_wind_result_which_cog_l1b_brc',
    'mie_wind_result_satellite_los_velocity',
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
    'rayleigh_wind_result_arg_of_lat_of_DEM_intersection',
    'rayleigh_wind_result_HLOS_error',
    'rayleigh_wind_result_reference_hlos',
    'rayleigh_wind_result_background_high',
    'rayleigh_wind_result_QC_flags_1',
    'rayleigh_wind_result_QC_flags_2',
    'rayleigh_wind_result_QC_flags_3',
    'rayleigh_wind_result_scattering_ratio',
    'rayleigh_assimilation_L2B_QC',
    'rayleigh_assimilation_persistence_error',
    'rayleigh_assimilation_representativity_error',
    'rayleigh_assimilation_final_error',
    'rayleigh_assimilation_est_L2B_bias',
    'rayleigh_assimilation_background_HLOS_error',
    'rayleigh_assimilation_L2B_HLOS_reliability',
    'rayleigh_assimilation_u_wind_background_error',
    'rayleigh_assimilation_v_wind_background_error',
    'rayleigh_wind_result_observation_type',
    'rayleigh_wind_result_validity_flag',
    'rayleigh_wind_result_wind_velocity',
    'rayleigh_wind_result_wind_to_pressure',
    'rayleigh_wind_result_wind_to_temperature',
    'rayleigh_wind_result_wind_to_backscatter_ratio',
    'rayleigh_wind_result_applied_spacecraft_los_corr_velocity',
    'rayleigh_wind_result_applied_rdb_corr_velocity',
    'rayleigh_wind_result_applied_ground_corr_velocity',
    'rayleigh_wind_result_applied_m1_temperature_corr_velocity',
    'rayleigh_wind_result_integration_length',
    'rayleigh_wind_result_num_of_measurements',
    'rayleigh_wind_result_reference_pressure',
    'rayleigh_wind_result_reference_temperature',
    'rayleigh_wind_result_reference_backscatter_ratio',
    'rayleigh_assimilation_validity_flag',
    'rayleigh_assimilation_background_HLOS',
    'rayleigh_assimilation_background_u_wind_velocity',
    'rayleigh_assimilation_background_v_wind_velocity',
    'rayleigh_assimilation_background_horizontal_wind_velocity',
    'rayleigh_assimilation_background_wind_direction',
    'rayleigh_assimilation_analysis_HLOS',
    'rayleigh_assimilation_analysis_u_wind_velocity',
    'rayleigh_assimilation_analysis_v_wind_velocity',
    'rayleigh_assimilation_analysis_horizontal_wind_velocity',
    'rayleigh_assimilation_analysis_wind_direction',
    'rayleigh_wind_result_albedo_off_nadir',
    'rayleigh_aht_22',
    'rayleigh_aht_23',
    'rayleigh_aht_24',
    'rayleigh_aht_25',
    'rayleigh_aht_26',
    'rayleigh_aht_27',
    'rayleigh_tc_18',
    'rayleigh_tc_19',
    'rayleigh_tc_20',
    'rayleigh_tc_21',
    'rayleigh_tc_23',
    'rayleigh_tc_25',
    'rayleigh_tc_27',
    'rayleigh_tc_29',
    'rayleigh_tc_32',
    'rayleigh_wind_result_los_azimuth',
    'rayleigh_wind_result_los_elevation_top',
    'rayleigh_wind_result_los_elevation_bottom',
    'rayleigh_wind_result_los_elevation_vcog',
    'rayleigh_wind_result_which_cog_l1b_brc',
    'rayleigh_wind_result_satellite_los_velocity',
    'rayleigh_wind_result_scattering_ratio_method',
])

MEASUREMENT_FIELDS = set([
    'mie_measurement_map',
    'rayleigh_measurement_map',
    'mie_measurement_weight',
    'rayleigh_measurement_weight',
    'l1B_num_of_measurements_per_obs',
    'l1B_obs_number',
    'l1B_measurement_time',
    # 'mie_bin_classification',
    # 'rayleigh_bin_classification',
    'optical_prop_algo_extinction',
    'optical_prop_algo_scattering_ratio',
    'optical_prop_crosstalk_detected',
])

ARRAY_FIELDS = set([
    'mie_measurement_map',
    'rayleigh_measurement_map',
    'mie_wind_profile_wind_result_id',
    'rayleigh_wind_profile_wind_result_id',
    'mie_measurement_weight',
    'rayleigh_measurement_weight',
])


class L2CMeasurementDataExtractor(AccumulatedDataExtractor):
    locations = locations
    mie_grouping_fields_defs = MIE_GROUPING_FIELDS
    rayleigh_grouping_fields_defs = RAYLEIGH_GROUPING_FIELDS
    mie_profile_fields_defs = MIE_PROFILE_FIELDS
    rayleigh_profile_fields_defs = RAYLEIGH_PROFILE_FIELDS
    mie_wind_fields_defs = MIE_WIND_FIELDS
    rayleigh_wind_fields_defs = RAYLEIGH_WIND_FIELDS
    measurement_fields_defs = MEASUREMENT_FIELDS
    array_fields = ARRAY_FIELDS

    overlap_fields = [
        'mie_profile_datetime_stop',
        'rayleigh_profile_datetime_stop',
        'mie_wind_result_stop_time',
        'rayleigh_wind_result_stop_time',
    ]

extractor = L2CMeasurementDataExtractor()

# main extraction function
extract_data = extractor.extract_data


test_file = '/mnt/data/AE_OPER_ALD_U_N_2C_20151001T001124_20151001T014439_0002/AE_OPER_ALD_U_N_2C_20151001T001124_20151001T014439_0002.DBL'


def main():
    print(extract_data(
        test_file, {
            'mie_wind_result_id': {
                'min_value': 1000,
                'max_value': 1010
            },


            # 'l1B_obs_number': {
            #     'min_value': 3,
            #     'max_value': 10
            # }

            # 'l1B_obs_number': {
            #     'min_value': 300,
            #     'max_value': 310,
            # }
        },
        mie_grouping_fields=[

        ],
        rayleigh_grouping_fields=[

        ],
        mie_profile_fields=[
            # 'mie_profile_lat_of_DEM_intersection'
            # 'mie_wind_profile_observation_type',
            # 'mie_wind_profile_wind_result_id'
        ],
        rayleigh_profile_fields=[

        ],
        mie_wind_fields=[
            'mie_assimilation_background_horizontal_wind_velocity',
            'mie_assimilation_background_wind_direction',
            # 'mie_wind_result_id'
        ],
        rayleigh_wind_fields=[
            # 'rayleigh_wind_result_id'
        ],
        measurement_fields=[
            # 'l1B_obs_number'
        ],
    ))

if __name__ == '__main__':
    main()
