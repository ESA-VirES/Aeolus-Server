# ------------------------------------------------------------------------------
#
#  Aeolus - Level 1B data extraction
#
# Project: VirES-Aeolus
# Authors: Daniel Santillan <daniel.santillan@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2022 EOX IT Services GmbH
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

def calc_mie_reference_pulse(cf):
    reference_pulse = ['/reference_pulse', -1, 'mie_reference_pulse']
    array = np.vstack(access_location(cf,reference_pulse))
    return array.reshape((int(array.shape[0] / 600), 600, 20))

def calc_rayleigh_reference_pulse(cf):
    reference_pulse = ['/reference_pulse', -1, 'rayleigh_reference_pulse']
    array = np.vstack(access_location(cf,reference_pulse))
    return array.reshape((int(array.shape[0] / 600), 600, 20))

def calc_mie_measurement_data(cf):
    measurements = access_location(cf, ['/mie_measurement', -1, 'mie_measurement_data'])
    no_msr = access_location(cf, ['/house_keeping', -1, 'n'])
    signal = np.array([x for x in measurements])
    #assumption: No. of measurements does not change within one orbit file
    return signal[:, :np.median(no_msr).astype(int) * 25, :].reshape(signal.shape[0], np.median(no_msr).astype(int), 25, 20)

def calc_rayleigh_measurement_data(cf):
    measurements = access_location(cf, ['/rayleigh_measurement', -1, 'rayleigh_measurement_data'])
    no_msr = access_location(cf, ['/house_keeping', -1, 'n'])
    signal = np.array([x for x in measurements])
    #assumption: No. of measurements does not change within one orbit file
    return signal[:, :np.median(no_msr).astype(int) * 25, :].reshape(signal.shape[0], np.median(no_msr).astype(int), 25, 20)

# all observation fields and their respective coda path for their location in
# the DBL file.
OBSERVATION_LOCATIONS = {
    'time':                                     ['/geolocation', -1, 'observation_aocs/observation_centroid_time'],
    'mie_longitude':                            ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'longitude_of_height_bin'],
    'mie_latitude':                             ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'latitude_of_height_bin'],
    'mie_altitude':                             ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'altitude_of_height_bin'],
    'mie_topocentric_azimuth_of_height_bin':    ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'topocentric_azimuth_of_height_bin'],
    'mie_topocentric_elevation_of_height_bin':  ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'topocentric_elevation_of_height_bin'],
    'mie_target_to_sun_visibility_flag':        ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'target_to_sun_visibility_flag'],
    'mie_range':                                ['/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'satellite_range_of_height_bin'],
    'rayleigh_longitude':                       ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'longitude_of_height_bin'],
    'rayleigh_latitude':                        ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'latitude_of_height_bin'],
    'rayleigh_altitude':                        ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'altitude_of_height_bin'],
    'rayleigh_topocentric_azimuth_of_height_bin':   ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'topocentric_azimuth_of_height_bin'],
    'rayleigh_topocentric_elevation_of_height_bin': ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'topocentric_elevation_of_height_bin'],
    'rayleigh_target_to_sun_visibility_flag':   ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'target_to_sun_visibility_flag'],
    'rayleigh_range':                           ['/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'satellite_range_of_height_bin'],
    'latitude_of_DEM_intersection':             ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/latitude_of_dem_intersection'],
    'longitude_of_DEM_intersection':            ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/longitude_of_dem_intersection'],
    'altitude_of_DEM_intersection':             ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/altitude_of_dem_intersection'],
    'argument_of_latitude_of_dem_intersection': ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/argument_of_latitude_of_dem_intersection'],
    'sun_elevation_angle':                      ['/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/sun_elevation_at_dem_intersection'],
    'velocity_at_DEM_intersection':             ['/geolocation', -1, 'observation_geolocation/line_of_sight_velocity'],
    'geoid_separation':                         ['/geolocation', -1, 'observation_geolocation/geoid_separation'],
    'AOCS_x_position':                          ['/geolocation', -1, 'observation_aocs/x_position'],
    'AOCS_y_position':                          ['/geolocation', -1, 'observation_aocs/y_position'],
    'AOCS_z_position':                          ['/geolocation', -1, 'observation_aocs/z_position'],
    'AOCS_x_velocity':                          ['/geolocation', -1, 'observation_aocs/x_velocity'],
    'AOCS_y_velocity':                          ['/geolocation', -1, 'observation_aocs/y_velocity'],
    'AOCS_z_velocity':                          ['/geolocation', -1, 'observation_aocs/z_velocity'],
    'AOCS_roll_angle':                          ['/geolocation', -1, 'observation_aocs/roll_angle'],
    'AOCS_pitch_angle':                         ['/geolocation', -1, 'observation_aocs/pitch_angle'],
    'AOCS_yaw_angle':                           ['/geolocation', -1, 'observation_aocs/yaw_angle'],
    'num_reference_pulses':                     ['/reference_pulse', -1, 'num_reference_pulses'],
    'mie_reference_pulse':                      calc_mie_reference_pulse,
    'rayleigh_reference_pulse':                 calc_rayleigh_reference_pulse,
    'instrument_mode':                          ['/house_keeping', -1, 'instrument_mode'],
    'number_of_pulses_per_meas':                ['/house_keeping', -1, 'p'],
    'number_of_meas_per_obs':                   ['/house_keeping', -1, 'n'],
    'average_laser_frequency_offset':           ['/house_keeping', -1, 'laser_pulse_attributes/avg_laser_frequency_offset'],
    'average_laser_energy':                     ['/house_keeping', -1, 'laser_pulse_attributes/avg_uv_energy'],
    'laser_frequency_offset_std_dev':           ['/house_keeping', -1, 'laser_pulse_attributes/laser_freq_offset_std_dev'],
    'uv_energy_std_dev':                        ['/house_keeping', -1, 'laser_pulse_attributes/uv_energy_std_dev'],
    'dt1':                                      ['/house_keeping', -1, 'pulse_time_delays/dt1'],
    'dt2':                                      ['/house_keeping', -1, 'pulse_time_delays/dt2'],
    'dt3_fixed':                                ['/house_keeping', -1, 'pulse_time_delays/dt3_fixed'],
    'dt4':                                      ['/house_keeping', -1, 'pulse_time_delays/dt4'],
    'dt5':                                      ['/house_keeping', -1, 'pulse_time_delays/dt5'],
    'deu_imaging_integration_time':             ['/house_keeping', -1, 'pulse_time_delays/deu_imaging_integration_time'],
    'td_ray_mie':                               ['/house_keeping', -1, 'pulse_time_delays/td_ray_mie'],
    'mie_bin_integration_time':                 ['/house_keeping', -1, 'mie_time_delays/bin_layer_integration_time'],#[24]
    'mie_bkg_integration_time':                 ['/house_keeping', -1, 'mie_time_delays/background_integration_time'],
    'ray_bin_integration_time':                 ['/house_keeping', -1, 'rayleigh_time_delays/bin_layer_integration_time'],#[24]
    'ray_bkg_integration_time':                 ['/house_keeping', -1, 'rayleigh_time_delays/background_integration_time'],
    'mie_accd_temp':                            ['/house_keeping', -1, 'avg_mie_accd_die_temp'],
    'ray_accd_temp':                            ['/house_keeping', -1, 'avg_rayleigh_accd_die_temp'],
    'deu_temp':                                 ['/house_keeping', -1, 'deu_temp'],
    'm1_temp':                                  ['/house_keeping', -1, 'm1_temp'],
    'aht_22_tel_m1':                            ['/house_keeping', -1, 'aht_22_tel_m1'],
    'aht_23_tel_m1':                            ['/house_keeping', -1, 'aht_23_tel_m1'],
    'aht_24_tel_m1':                            ['/house_keeping', -1, 'aht_24_tel_m1'],
    'aht_25_tel_m1':                            ['/house_keeping', -1, 'aht_25_tel_m1'],
    'aht_26_tel_m4':                            ['/house_keeping', -1, 'aht_26_tel_m1'],
    'aht_27_tel_m5':                            ['/house_keeping', -1, 'aht_27_tel_m1'],
    'm1_tc_temp':                               ['/house_keeping', -1, 'm1_tc_temp'],
    'tc_18_tel_m11':                            ['/house_keeping', -1, 'tc_18_tel_m11'],
    'tc_19_tel_m12':                            ['/house_keeping', -1, 'tc_19_tel_m12'],
    'tc_20_tel_m13':                            ['/house_keeping', -1, 'tc_20_tel_m13'],
    'tc_21_tel_m14':                            ['/house_keeping', -1, 'tc_21_tel_m14'],
    'tc_25_tm15_ths1y':                         ['/house_keeping', -1, 'tc_25_tm15_ths1y'],
    'tc_27_tm16_ths1y':                         ['/house_keeping', -1, 'tc_27_tm16_ths1y'],
    'tc_29_ths2':                               ['/house_keeping', -1, 'tc_29_ths2'],
    'tc_23_ths1':                               ['/house_keeping', -1, 'tc_23_ths1'],
    'tc_32_ths3':                               ['/house_keeping', -1, 'tc_32_ths3'],
    'struts_temp_pxpy':                         ['/house_keeping', -1, 'struts_temp_pxpy'],
    'struts_temp_mxpy':                         ['/house_keeping', -1, 'struts_temp_mxpy'],
    'struts_temp_mpy':                          ['/house_keeping', -1, 'struts_temp_mpy'],
    'm2_tc_temp':                               ['/house_keeping', -1, 'm2_tc_temp'],
    'mie_measurement_data':                     calc_mie_measurement_data,
    'rayleigh_measurement_data':                calc_rayleigh_measurement_data,
}

# all measurement fields and their respective coda path for their location in
# the DBL file.
MEASUREMENT_LOCATIONS = {
    'time':                                     ['/geolocation', -1, 'measurement_aocs', -1, 'measurement_centroid_time'],
    'AOCS_x_position':                          ['/geolocation', -1, 'measurement_aocs', -1, 'x_position'],
    'AOCS_y_position':                          ['/geolocation', -1, 'measurement_aocs', -1, 'y_position'],
    'AOCS_z_position':                          ['/geolocation', -1, 'measurement_aocs', -1, 'z_position'],
    'AOCS_x_velocity':                          ['/geolocation', -1, 'measurement_aocs', -1, 'x_velocity'],
    'AOCS_y_velocity':                          ['/geolocation', -1, 'measurement_aocs', -1, 'y_velocity'],
    'AOCS_z_velocity':                          ['/geolocation', -1, 'measurement_aocs', -1, 'z_velocity'],
    'AOCS_roll_angle':                          ['/geolocation', -1, 'measurement_aocs', -1, 'roll_angle'],
    'AOCS_pitch_angle':                         ['/geolocation', -1, 'measurement_aocs', -1, 'pitch_angle'],
    'AOCS_yaw_angle':                           ['/geolocation', -1, 'measurement_aocs', -1, 'yaw_angle'],
    'mie_longitude':                            ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'longitude_of_height_bin'],
    'mie_latitude':                             ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'latitude_of_height_bin'],
    'mie_altitude':                             ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'altitude_of_height_bin'],
    'mie_range':                                ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'sattelite_range_of_height_bin'],
    'rayleigh_longitude':                       ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'longitude_of_height_bin'],
    'rayleigh_latitude':                        ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'latitude_of_height_bin'],
    'rayleigh_altitude':                        ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'altitude_of_height_bin'],
    'rayleigh_range':                           ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'sattelite_range_of_height_bin'],
    'latitude_of_DEM_intersection':             ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/latitude_of_dem_intersection'],
    'longitude_of_DEM_intersection':            ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/longitude_of_dem_intersection'],
    'altitude_of_DEM_intersection':             ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/altitude_of_dem_intersection'],
    'argument_of_latitude_of_dem_intersection': ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/argument_of_latitude_of_dem_intersection'],
    'sun_elevation_angle':                      ['/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/sun_elevation_at_dem_intersection'],
    'velocity_at_DEM_intersection':             ['/geolocation', -1, 'measurement_geolocation', -1, 'aocs_los_velocity'],
    #/mie_measurement[?]/start_of_observation_time
    #/mie_measurement[?]/num_measurements
    #/mie_measurement[?]/num_height_bins
    #/mie_measurement[?]/num_accd_columns
    #/rayleigh_measurement[?]/start_of_observation_time
    #/rayleigh_measurement[?]/num_measurements
    #/rayleigh_measurement[?]/num_height_bins
    #/rayleigh_measurement[?]/num_accd_columns
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
])



class L1AMeasurementDataExtractor(MeasurementDataExtractor):

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


extractor = L1AMeasurementDataExtractor()

extract_data = extractor.extract_data
