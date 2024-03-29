# ------------------------------------------------------------------------------
#
#  Aeolus - Level 1B data extraction
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

import numpy as np

from aeolus.coda_utils import access_location
from aeolus.albedo import sample_offnadir
from aeolus.extraction.measurement import (
    MeasurementDataExtractor, stack_measurement_array
)


def location_for_observation(location, observation_id):
    if location[1] == observation_id:
        return location
    return [location[0], observation_id] + location[2:]


def calc_rayleigh_signal_intensity(cf, observation_id=None):
    if observation_id is not None:
        channel_A_intensity = access_location(cf,
            location_for_observation(
                MEASUREMENT_LOCATIONS['rayleigh_signal_channel_A_intensity'],
                observation_id,
            )
        )

        channel_B_intensity = access_location(cf,
            location_for_observation(
                MEASUREMENT_LOCATIONS['rayleigh_signal_channel_B_intensity'],
                observation_id,
            )
        )

        if observation_id == -1:
            channel_A_intensity = stack_measurement_array(channel_A_intensity)
            channel_B_intensity = stack_measurement_array(channel_B_intensity)
    else:
        channel_A_intensity = access_location(cf,
            OBSERVATION_LOCATIONS['rayleigh_signal_channel_A_intensity'],
        )
        channel_B_intensity = access_location(cf,
            OBSERVATION_LOCATIONS['rayleigh_signal_channel_B_intensity'],
        )

    return channel_A_intensity + channel_B_intensity


def calc_rayleigh_signal_intensity_measurement(cf, observation_id=-1):
    return calc_rayleigh_signal_intensity(cf, observation_id)


def calc_rayleigh_signal_intensity_range_corrected(cf):
    signal_intensity = np.vstack(
        calc_rayleigh_signal_intensity(cf)
    )[:, :24]
    rayleigh_range = np.vstack(
        access_location(cf,
            OBSERVATION_LOCATIONS['rayleigh_range'],
        )
    )[:, 1:]

    return signal_intensity * (rayleigh_range ** 2)


def calc_mie_signal_intensity_range_corrected(cf):
    signal_intensity = np.vstack(
        access_location(cf,
            OBSERVATION_LOCATIONS['mie_signal_intensity'],
        )
    )[:, :24]
    mie_range = np.vstack(
        access_location(cf,
            OBSERVATION_LOCATIONS['mie_range'],
        )
    )[:, 1:]

    return signal_intensity * (mie_range ** 2)


def calc_rayleigh_signal_intensity_normalised(cf):
    signal_intensity = np.vstack(
        calc_rayleigh_signal_intensity(cf)
    )[:, :24]
    rayleigh_range = np.vstack(
            access_location(cf,
            OBSERVATION_LOCATIONS['rayleigh_range'],
        )
    )[:, 1:]

    integration_time_location = [
        'measurement', -1, 'rayleigh_time_delays', 'bin_layer_integration_time'
    ]
    integration_times = np.vstack(
        access_location(cf, integration_time_location)
    )

    return signal_intensity * (rayleigh_range ** 2) * (101.0 / integration_times)


def calc_mie_signal_intensity_normalised(cf, observation_id=None):
    signal_intensity = np.vstack(
        access_location(cf,
            OBSERVATION_LOCATIONS['mie_signal_intensity'],
        )
    )[:, :24]
    mie_range = np.vstack(
        access_location(cf,
            OBSERVATION_LOCATIONS['mie_range'],
        )
    )[:, 1:]

    integration_time_location = [
        'measurement', -1, 'mie_time_delays', 'bin_layer_integration_time'
    ]
    integration_times = np.vstack(
        access_location(cf, integration_time_location)
    )

    return signal_intensity * (mie_range ** 2) * (101.0 / integration_times)


def calc_rayleigh_SNR(cf, observation_id=None):
    if observation_id is not None:
        channel_A_SNR = access_location(cf,
            location_for_observation(
                MEASUREMENT_LOCATIONS['rayleigh_channel_A_SNR'],
                observation_id,
            )
        )

        channel_B_SNR = access_location(cf,
            location_for_observation(
                MEASUREMENT_LOCATIONS['rayleigh_channel_B_SNR'],
                observation_id,
            )
        )
        if observation_id == -1:
            channel_A_SNR = stack_measurement_array(channel_A_SNR)
            channel_B_SNR = stack_measurement_array(channel_B_SNR)
    else:
        channel_A_SNR = access_location(cf,
            OBSERVATION_LOCATIONS['rayleigh_channel_A_SNR'],
        )
        channel_B_SNR = access_location(cf,
            OBSERVATION_LOCATIONS['rayleigh_channel_B_SNR'],
        )

    return channel_A_SNR + channel_B_SNR


def calc_rayleigh_SNR_measurement(cf, observation_id=-1):
    return calc_rayleigh_SNR(cf, observation_id)


def calculate_albedo_off_nadir(cf, observation_id=None):
    """ Retrieves the albedo off_nadir values for the given file
    """
    start = cf.fetch_date('/mph/sensing_start')
    stop = cf.fetch_date('/mph/sensing_stop')

    mean = start + (stop - start) / 2
    if observation_id is not None:
        lons = access_location(cf,
            location_for_observation(
                MEASUREMENT_LOCATIONS['longitude_of_DEM_intersection'],
                observation_id,
            )
        )

        lats = access_location(cf,
            location_for_observation(
                MEASUREMENT_LOCATIONS['latitude_of_DEM_intersection'],
                observation_id,
            )
        )
        if observation_id == -1:
            lons = np.vstack(lons)
            lats = np.vstack(lats)
    else:
        lons = access_location(cf,
            OBSERVATION_LOCATIONS['longitude_of_DEM_intersection'],
        )
        lats = access_location(cf,
            OBSERVATION_LOCATIONS['latitude_of_DEM_intersection'],
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


def calculate_albedo_off_nadir_measurement(cf, observation_id=-1):
    return calculate_albedo_off_nadir(cf, observation_id)


# all observation fields and their respective coda path for their location in
# the DBL file.
OBSERVATION_LOCATIONS = {
    'time':                                     ['/geolocation', -1, 'observation_aocs/observation_centroid_time'],
    'longitude_of_DEM_intersection':            ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/longitude_of_dem_intersection'],
    'latitude_of_DEM_intersection':             ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/latitude_of_dem_intersection'],
    'altitude_of_DEM_intersection':             ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/altitude_of_dem_intersection'],
    'argument_of_latitude_of_dem_intersection': ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/argument_of_latitude_of_dem_intersection'],
    'sun_elevation_angle':                      ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/sun_elevation_at_dem_intersection'],
    'mie_longitude':                            ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'longitude_of_height_bin'],
    'mie_latitude':                             ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'latitude_of_height_bin'],
    'rayleigh_longitude':                       ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'longitude_of_height_bin'],
    'rayleigh_latitude':                        ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'latitude_of_height_bin'],
    'mie_altitude':                             ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'altitude_of_height_bin'],
    'rayleigh_altitude':                        ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'altitude_of_height_bin'],
    'rayleigh_topocentric_azimuth_of_height_bin': ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'topocentric_azimuth_of_height_bin'],
    'rayleigh_topocentric_elevation_of_height_bin': ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'topocentric_elevation_of_height_bin'],
    'rayleigh_target_to_sun_visibility_flag':   ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'target_to_sun_visibility_flag'],
    'mie_range':                                ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'satellite_range_of_height_bin'],
    'rayleigh_range':                           ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'satellite_range_of_height_bin'],
    'geoid_separation':                         ['/geolocation', -1, 'observation_geolocation/geoid_separation'],
    'velocity_at_DEM_intersection':             ['/geolocation', -1, 'observation_geolocation/line_of_sight_velocity'],
    'AOCS_pitch_angle':                         ['/geolocation', -1, 'observation_aocs/pitch_angle'],
    'AOCS_roll_angle':                          ['/geolocation', -1, 'observation_aocs/roll_angle'],
    'AOCS_yaw_angle':                           ['/geolocation', -1, 'observation_aocs/yaw_angle'],
    'mie_HLOS_wind_speed':                      ['/wind_velocity', -1, 'observation_wind_profile/mie_altitude_bin_wind_info', -1, 'wind_velocity'],
    'rayleigh_HLOS_wind_speed':                 ['/wind_velocity', -1, 'observation_wind_profile/rayleigh_altitude_bin_wind_info', -1, 'wind_velocity'],
    'mie_signal_intensity':                     ['/useful_signal', -1, 'observation_useful_signals/mie_altitude_bin_useful_signal_info', -1, 'useful_signal'],
    'rayleigh_signal_channel_A_intensity':      ['/useful_signal', -1, 'observation_useful_signals/rayleigh_altitude_bin_useful_signal_info', -1, 'useful_signal_channel_a'],
    'rayleigh_signal_channel_B_intensity':      ['/useful_signal', -1, 'observation_useful_signals/rayleigh_altitude_bin_useful_signal_info', -1, 'useful_signal_channel_b'],
    'rayleigh_signal_intensity':                calc_rayleigh_signal_intensity,
    'rayleigh_signal_intensity_range_corrected': calc_rayleigh_signal_intensity_range_corrected,
    'mie_signal_intensity_range_corrected':     calc_mie_signal_intensity_range_corrected,
    'rayleigh_signal_intensity_normalised':     calc_rayleigh_signal_intensity_normalised,
    'mie_signal_intensity_normalised':          calc_mie_signal_intensity_normalised,
    'mie_ground_velocity':                      ['/ground_wind_detection', -1, 'mie_ground_correction_velocity'],
    'rayleigh_ground_velocity':                 ['/ground_wind_detection', -1, 'rayleigh_ground_correction_velocity'],
    'mie_HBE_ground_velocity':                  ['/ground_wind_detection', -1, 'hbe_mie_ground_correction_velocity'],
    'rayleigh_HBE_ground_velocity':             ['/ground_wind_detection', -1, 'hbe_rayleigh_ground_correction_velocity'],
    'mie_total_ZWC':                            ['/ground_wind_detection', -1, 'mie_channel_total_zero_wind_correction'],
    'rayleigh_total_ZWC':                       ['/ground_wind_detection', -1, 'rayleigh_channel_total_zero_wind_correction'],
    'mie_ground_fwhm':                          ['/ground_wind_detection', -1, 'mie_ground_fwhm'],
    'mie_ground_useful_signal':                 ['/ground_wind_detection', -1, 'mie_ground_useful_signal'],
    'mie_ground_signal_to_noise_ratio':         ['/ground_wind_detection', -1, 'mie_ground_signal_to_noise_ratio'],
    'mie_ground_refined_signal_to_noise_ratio': ['/ground_wind_detection', -1, 'mie_ground_refined_signal_to_noise_ratio'],
    'rayleigh_ground_useful_signal':            ['/ground_wind_detection', -1, 'rayleigh_ground_useful_signal'],
    'rayleigh_ground_signal_to_noise_ratio' :   ['/ground_wind_detection', -1, 'rayleigh_ground_signal_to_noise_ratio'],
    'mie_average_ground_wind_bin_thickness':    ['/ground_wind_detection', -1, 'mie_average_ground_wind_bin_thickness'],
    'rayleigh_average_ground_wind_bin_thickness': ['/ground_wind_detection', -1, 'rayleigh_average_ground_wind_bin_thickness'],
    'mie_average_ground_wind_bin_thickness_above_dem': ['/ground_wind_detection', -1, 'mie_average_ground_wind_bin_thickness_above_dem'],
    'rayleigh_average_ground_wind_bin_thickness_above_dem': ['/ground_wind_detection', -1, 'rayleigh_average_ground_wind_bin_thickness_above_dem'],
    'mie_scattering_ratio':                     ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'scattering_ratio_mie'],
    'mie_refined_scattering_ratio':             ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'refined_scattering_ratio_mie'],
    'mie_SNR':                                  ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'mie_signal_to_noise_ratio'],
    'rayleigh_channel_A_SNR':                   ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'rayleigh_signal_to_noise_ratio_channel_a'],
    'rayleigh_channel_B_SNR':                   ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'rayleigh_signal_to_noise_ratio_channel_b'],
    'rayleigh_SNR':                             calc_rayleigh_SNR,
    'mie_error_quantifier':                     ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'error_quantifier_mie'],
    'rayleigh_error_quantifier':                ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'error_quantifier_rayleigh'],
    'rayleigh_enc_col_channel_a':               ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'enc_col_channel_a'],
    'rayleigh_enc_col_channel_b':               ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'enc_col_channel_b'],
    'rayleigh_enc_col_std_dev_channel_a':       ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'enc_col_std_dev_channel_a'],
    'rayleigh_enc_col_std_dev_channel_b':       ['/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'enc_col_std_dev_channel_b'],
    'laser_frequency':                          ['/product_confidence_data', -1, 'observation_pcd/avg_laser_frequency_offset'],
    'average_laser_energy':                     ['/product_confidence_data', -1, 'observation_pcd/avg_uv_energy'],
    'laser_frequency_offset_std_dev':           ['/product_confidence_data', -1, 'observation_pcd/laser_frequency_offset_std_dev'],
    'uv_energy_std_dev':                        ['/product_confidence_data', -1, 'observation_pcd/uv_energy_std_dev'],
    'mie_ref_pulse_signal_to_noise_ratio':      ['/product_confidence_data', -1, 'observation_pcd/mie_ref_pulse_signal_to_noise_ratio'],
    'mie_ref_pulse_refined_signal_to_noise_ratio': ['/product_confidence_data', -1, 'observation_pcd/mie_ref_pulse_refined_signal_to_noise_ratio'],
    'rayleigh_ref_pulse_signal_to_noise_ratio_channel_a': ['/product_confidence_data', -1, 'observation_pcd/rayleigh_ref_pulse_signal_to_noise_ratio_channel_a'],
    'rayleigh_ref_pulse_signal_to_noise_ratio_channel_b': ['/product_confidence_data', -1, 'observation_pcd/rayleigh_ref_pulse_signal_to_noise_ratio_channel_b'],
    'mie_num_peak_invalid':                     ['/product_confidence_data', -1, 'observation_pcd/corrected_mie_reference_pulse_response'],
    'mie_corrected_reference_pulse_response':   ['/product_confidence_data', -1, 'observation_pcd/num_mie_peak_invalid'],
    'rayleigh_corrected_reference_pulse_response': ['/product_confidence_data', -1, 'observation_pcd/corrected_rayleigh_reference_pulse_response'],
    'mie_num_invalid_reference_pulse':          ['/product_confidence_data', -1, 'observation_pcd/num_mie_invalid_reference_pulse'],
    'rayleigh_num_invalid_reference_pulse':     ['/product_confidence_data', -1, 'observation_pcd/num_rayleigh_invalid_measurements'],
    'rayleigh_enc_col_ref_pulse_channel_a':     ['/product_confidence_data', -1, 'observation_pcd/enc_col_ref_pulse_channel_a'],
    'rayleigh_enc_col_ref_pulse_channel_b':     ['/product_confidence_data', -1, 'observation_pcd/enc_col_ref_pulse_channel_b'],
    'rayleigh_enc_col_std_dev_ref_pulse_channel_a': ['/product_confidence_data', -1, 'observation_pcd/enc_col_std_dev_ref_pulse_channel_a'],
    'rayleigh_enc_col_std_dev_ref_pulse_channel_b': ['/product_confidence_data', -1, 'observation_pcd/enc_col_std_dev_ref_pulse_channel_b'],
    'aht_22_tel_m1':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/aht_22_tel_m1'],
    'aht_23_tel_m1':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/aht_23_tel_m1'],
    'aht_24_tel_m1':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/aht_24_tel_m1'],
    'aht_25_tel_m1':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/aht_26_tel_m1'],
    'aht_26_tel_m1':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/aht_27_tel_m1'],
    'aht_27_tel_m1':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/aht_22_tel_m1'],
    'tc_18_tel_m11':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/tc_18_tel_m11'],
    'tc_19_tel_m12':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/tc_19_tel_m12'],
    'tc_20_tel_m13':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/tc_20_tel_m13'],
    'tc_21_tel_m14':                            ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/tc_21_tel_m14'],
    'tc_25_tm15_ths1y':                         ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/tc_25_tm15_ths1y'],
    'tc_27_tm16_ths1y':                         ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/tc_27_tm16_ths1y'],
    'tc_29_ths2':                               ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/tc_29_ths2'],
    'tc_23_ths1':                               ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/tc_32_ths3'],
    'tc_32_ths3':                               ['/product_confidence_data', -1, 'observation_pcd/M1_Temperatures/aht_22_tel_m1'],
    'number_of_meas':                           ['/product_confidence_data', -1, 'n'],
    'number_of_pulses':                         ['/product_confidence_data', -1, 'p'],
    'rayleigh_bin_quality_flag':                ['/wind_velocity', -1, 'observation_wind_profile/rayleigh_altitude_bin_wind_info', -1, 'bin_quality_flag'],
    'mie_bin_quality_flag':                     ['/wind_velocity', -1, 'observation_wind_profile/mie_altitude_bin_wind_info', -1, 'bin_quality_flag'],
    'rayleigh_reference_pulse_quality_flag':    ['/wind_velocity', -1, 'observation_wind_profile/rayleigh_reference_pulse_quality_flag'],
    'mie_reference_pulse_quality_flag':         ['/wind_velocity', -1, 'observation_wind_profile/mie_reference_pulse_quality_flag'],
    'mie_bin_integration_times':                ['measurement', -1, 'mie_time_delays', 'bin_layer_integration_time'],
    'mie_bkg_integration_time':                 ['measurement', -1, 'mie_time_delays', 'background_integration_time'],
    'rayleigh_bin_integration_times':           ['measurement', -1, 'rayleigh_time_delays', 'bin_layer_integration_time'],
    'rayleigh_bkg_integration_time':            ['measurement', -1, 'rayleigh_time_delays', 'background_integration_time'],

    # Albedo values:
    'albedo_off_nadir':                         calculate_albedo_off_nadir,
}

# all measurement fields and their respective coda path for their location in
# the DBL file.
MEASUREMENT_LOCATIONS = {
    'time':                                     ['/geolocation', -1, 'measurement_aocs', -1, 'measurement_centroid_time'],
    'longitude_of_DEM_intersection':            ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/longitude_of_dem_intersection'],
    'latitude_of_DEM_intersection':             ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/latitude_of_dem_intersection'],
    'altitude_of_DEM_intersection':             ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/altitude_of_dem_intersection'],
    'argument_of_latitude_of_dem_intersection': ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/argument_of_latitude_of_dem_intersection'],
    'sun_elevation_angle':                      ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/sun_elevation_at_dem_intersection'],
    'mie_longitude':                            ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'longitude_of_height_bin'],
    'mie_latitude':                             ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'latitude_of_height_bin'],
    'rayleigh_longitude':                       ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'longitude_of_height_bin'],
    'rayleigh_latitude':                        ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'latitude_of_height_bin'],
    'mie_altitude':                             ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'altitude_of_height_bin'],
    'rayleigh_altitude':                        ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'altitude_of_height_bin'],
    'velocity_at_DEM_intersection':             ['/geolocation', -1, 'measurement_geolocation', -1, 'aocs_los_velocity'],
    'AOCS_pitch_angle':                         ['/geolocation', -1, 'measurement_aocs', -1, 'pitch_angle'],
    'AOCS_roll_angle':                          ['/geolocation', -1, 'measurement_aocs', -1, 'roll_angle'],
    'AOCS_yaw_angle':                           ['/geolocation', -1, 'measurement_aocs', -1, 'yaw_angle'],
    'mie_HLOS_wind_speed':                      ['/wind_velocity', -1, 'measurement_wind_profile', -1, 'mie_altitude_bin_wind_info', -1, 'wind_velocity'],
    'rayleigh_HLOS_wind_speed':                 ['/wind_velocity', -1, 'measurement_wind_profile', -1, 'rayleigh_altitude_bin_wind_info', -1, 'wind_velocity'],
    'mie_signal_intensity':                     ['/useful_signal', -1, 'measurement_useful_signal', -1, 'mie_altitude_bin_useful_signal_info', -1, 'useful_signal'],
    'rayleigh_signal_channel_A_intensity':      ['/useful_signal', -1, 'measurement_useful_signal', -1, 'rayleigh_altitude_bin_useful_signal_info', -1, 'useful_signal_channel_a'],
    'rayleigh_signal_channel_B_intensity':      ['/useful_signal', -1, 'measurement_useful_signal', -1, 'rayleigh_altitude_bin_useful_signal_info', -1, 'useful_signal_channel_b'],
    'rayleigh_signal_intensity':                calc_rayleigh_signal_intensity_measurement,
    'mie_ground_velocity':                      ['/wind_velocity', -1, 'measurement_wind_profile', -1, 'mie_ground_wind_velocity'],
    'rayleigh_ground_velocity':                 ['/wind_velocity', -1, 'measurement_wind_profile', -1, 'rayleigh_ground_wind_velocity'],

    'mie_mean_emitted_frequency':               ['/product_confidence_data', -1, 'measurement_pcd', -1, 'mie_mean_emitted_frequency'],
    'rayleigh_mean_emitted_frequency':          ['/product_confidence_data', -1, 'measurement_pcd', -1, 'rayleigh_mean_emitted_frequency'],
    'mie_emitted_frequency_std_dev':            ['/product_confidence_data', -1, 'measurement_pcd', -1, 'mie_emitted_frequency_std_dev'],
    'rayleigh_emitted_frequency_std_dev':       ['/product_confidence_data', -1, 'measurement_pcd', -1, 'rayleigh_emitted_frequency_std_dev'],

    'mie_scattering_ratio':                     ['/product_confidence_data', -1, 'measurement_pcd', -1, 'meas_alt_bin_pcd', -1, 'scattering_ratio_mie'],
    'mie_refined_scattering_ratio':             ['/product_confidence_data', -1, 'measurement_pcd', -1, 'meas_alt_bin_pcd', -1, 'refined_scattering_ratio_mie'],
    'mie_SNR':                                  ['/product_confidence_data', -1, 'measurement_pcd', -1, 'meas_alt_bin_pcd', -1, 'mie_signal_to_noise_ratio'],
    'rayleigh_channel_A_SNR':                   ['/product_confidence_data', -1, 'measurement_pcd', -1, 'meas_alt_bin_pcd', -1, 'rayleigh_signal_to_noise_ratio_channel_a'],
    'rayleigh_channel_B_SNR':                   ['/product_confidence_data', -1, 'measurement_pcd', -1, 'meas_alt_bin_pcd', -1, 'rayleigh_signal_to_noise_ratio_channel_b'],
    'rayleigh_SNR':                             calc_rayleigh_SNR_measurement,
    'average_laser_energy':                     ['/product_confidence_data', -1, 'measurement_pcd', -1, 'avg_uv_energy'],
    'laser_frequency':                          ['/product_confidence_data', -1, 'measurement_pcd', -1, 'avg_laser_frequency_offset'],
    'rayleigh_bin_quality_flag':                ['/wind_velocity', -1, 'measurement_wind_profile', -1, 'rayleigh_altitude_bin_wind_info', -1, 'bin_quality_flag'],
    'mie_bin_quality_flag':                     ['/wind_velocity', -1, 'measurement_wind_profile', -1, 'mie_altitude_bin_wind_info', -1, 'bin_quality_flag'],
    'rayleigh_reference_pulse_quality_flag':    ['/wind_velocity', -1, 'measurement_wind_profile', -1, 'rayleigh_reference_pulse_quality_flag'],
    'mie_reference_pulse_quality_flag':         ['/wind_velocity', -1, 'measurement_wind_profile', -1, 'mie_reference_pulse_quality_flag'],

    # Albedo values:
    'albedo_off_nadir':                         calculate_albedo_off_nadir_measurement,
}

# all fields whose values are actually arrays themselves
ARRAY_FIELDS = set([
    'mie_longitude',
    'mie_latitude',
    'rayleigh_longitude',
    'rayleigh_latitude',
    'mie_altitude',
    'rayleigh_altitude',
    'mie_range',
    'rayleigh_range',
    'mie_HLOS_wind_speed',
    'rayleigh_HLOS_wind_speed',
    'mie_signal_intensity',
    'rayleigh_signal_channel_A_intensity',
    'rayleigh_signal_channel_B_intensity',
    'mie_scattering_ratio',
    'mie_refined_scattering_ratio',
    'mie_SNR',
    'rayleigh_channel_A_SNR',
    'rayleigh_channel_B_SNR',
    'mie_error_quantifier',
    'rayleigh_error_quantifier',
    'rayleigh_bin_quality_flag',
    'mie_bin_quality_flag',
])


class L1BMeasurementDataExtractor(MeasurementDataExtractor):

    observation_locations = OBSERVATION_LOCATIONS
    measurement_locations = MEASUREMENT_LOCATIONS
    group_locations = {}
    ica_locations = {}
    sca_locations = {}
    mca_locations = {}
    array_fields = ARRAY_FIELDS

    def overlaps(self, cf, next_cf):
        location = MEASUREMENT_LOCATIONS['time']
        # length of time measurement_geolocation changes during the mission
        # need to check the lenght to know which is the last element to retrieve
        end = cf.fetch_date(
            location[0],
            cf.get_size(location[0])[0] - 1,
            location[2],
            cf.get_size(location[0], 0, location[2])[0] - 1,
            *location[4:]
        )
        begin = next_cf.fetch_date(
            location[0],
            0,
            location[2],
            0,
            *location[4:]
        )

        return begin < end

    def adjust_overlap(self, cf, next_cf, filters):
        stop_time = next_cf.fetch_date(
            '/geolocation', 0, 'measurement_aocs', 0, 'measurement_centroid_time'
        )

        if 'time' not in filters:
            filters['time'] = {'max': stop_time}

        elif 'max' not in filters['time']:
            filters['time']['max'] = stop_time

        else:
            filters['time']['max'] = min(stop_time, filters['time']['max'])

        return filters


extractor = L1BMeasurementDataExtractor()

extract_data = extractor.extract_data

# def extract_data(filenames, filters, observation_fields, measurement_fields,
#                  simple_observation_filters=False, convert_arrays=False):
#     """ Extract the data from the given filename(s) and apply the given filters.
#     """
#     filenames = [filenames] if isinstance(filenames, basestring) else filenames

#     out_observation_data = defaultdict(list)
#     out_measurement_data = defaultdict(list)

#     # check that filters are correct
#     for filter_name in filters:
#         if filter_name not in OBSERVATION_LOCATIONS  \
#                 and filter_name not in MEASUREMENT_LOCATIONS:
#             raise KeyError("No such field '%s'" % filter_name)

#     # check validity of observation/measurement fields
#     for observation_field in observation_fields:
#         if observation_field not in OBSERVATION_LOCATIONS:
#             raise KeyError("No such observation field '%s'" % observation_field)

#     for measurement_field in measurement_fields:
#         if measurement_field not in MEASUREMENT_LOCATIONS:
#             raise KeyError("No such measurement field '%s'" % measurement_field)

#     for cf in [CODAFile(filename) for filename in filenames]:
#         with cf:
#             # create a mask for observation data
#             observation_mask = None
#             for field_name, filter_value in filters.items():
#                 data = access_location(cf, OBSERVATION_LOCATIONS[field_name])

#                 new_mask = make_mask(
#                     data, filter_value.get('min'), filter_value.get('max'),
#                     field_name in ARRAY_FIELDS
#                 )

#                 observation_mask = combine_mask(new_mask, observation_mask)

#             if observation_mask is not None:
#                 filtered_observation_ids = np.nonzero(observation_mask)
#             else:
#                 filtered_observation_ids = None

#             # fetch the requested observation fields, filter accordingly and
#             # write to the output dict
#             for field_name in observation_fields:
#                 assert field_name in OBSERVATION_LOCATIONS
#                 data = access_location(cf, OBSERVATION_LOCATIONS[field_name])
#                 if filtered_observation_ids is not None:
#                     data = data[filtered_observation_ids]

#                 # convert to simple list instead of numpy array if requested
#                 if convert_arrays and isinstance(data, np.ndarray):
#                     data = _array_to_list(data)
#                 out_observation_data[field_name].extend(data)

#             # if we filter the measurements by observation ID, then use the
#             # filtered observation IDs as mask for the measurements.
#             # otherwise, use the full range of observations
#             if filtered_observation_ids is not None and \
#                     simple_observation_filters:
#                 observation_iterator = filtered_observation_ids[0]
#             else:
#                 observation_iterator = np.arange(cf.get_size('/geolocation')[0])

#             # collect measurement data
#             out_measurement_data.update(
#                 _read_measurements(
#                     cf, measurement_fields, filters, observation_iterator,
#                     convert_arrays
#                 )
#             )

#     return out_observation_data, out_measurement_data, filenames


# def _read_measurements(cf, measurement_fields, filters, observation_ids,
#                        convert_arrays):
#     out_measurement_data = defaultdict(list)

#     # return early, when no measurement fields are actually requested
#     if not measurement_fields:
#         return out_measurement_data

#     # Build a measurement mask
#     measurement_mask = None
#     for field_name, filter_value in filters.items():
#         # only apply filters for measurement fields
#         if field_name not in MEASUREMENT_LOCATIONS:
#             continue

#         location = MEASUREMENT_LOCATIONS[field_name]

#         data = np.vstack(access_location(cf, location)[observation_ids])

#         new_mask = make_mask(
#             data, filter_value.get('min'), filter_value.get('max'),
#             field_name in ARRAY_FIELDS
#         )
#         # combine the masks
#         measurement_mask = combine_mask(new_mask, measurement_mask)

#     # iterate over all requested fields
#     for field_name in measurement_fields:
#         location = MEASUREMENT_LOCATIONS[field_name]

#         # fetch the data, and apply the observation id mask
#         data = access_location(cf, location)[observation_ids]

#         # when a measurement mask was built, iterate over all measurement groups
#         # plus their mask respectively, and apply it to get a filtered list
#         if measurement_mask is not None:
#             tmp_data = [
#                 (
#                     _array_to_list(measurement[mask])
#                     if convert_arrays else measurement[mask]
#                 )
#                 for measurement, mask in izip(data, measurement_mask)
#             ]
#             data = np.empty(len(tmp_data), dtype=np.object)
#             data[:] = tmp_data

#         # convert to simple list instead of numpy array if requested
#         if convert_arrays and isinstance(data, np.ndarray):
#             data = _array_to_list(data)

#         out_measurement_data[field_name].append(data)

#     return out_measurement_data
