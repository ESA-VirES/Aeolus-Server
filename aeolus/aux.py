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

from itertools import chain, izip
from collections import defaultdict

import numpy as np

from aeolus.coda_utils import CODAFile, access_location
from aeolus.filtering import make_mask, combine_mask
from aeolus.albedo import sample_nadir


def _make_calc_albedo_nadir_aux_mrc_rrc(lon_location, lat_location):
    def _inner(cf):
        start = cf.fetch_date(
            '/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header'
            '/Validity_Period/Validity_Start'
        )
        stop = cf.fetch_date(
            '/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header'
            '/Validity_Period/Validity_Stop'
        )

        mean = start + (stop - start) / 2
        lons = np.vstack(access_location(cf, lon_location))
        lats = np.vstack(access_location(cf, lat_location))

        lons[lons > 180] -= 360
        shape = lons.shape

        if len(shape) > 1:
            lons = lons.flatten()
            lats = lats.flatten()

        data = sample_nadir(mean.year, mean.month, lons, lats)

        if len(shape) > 1:
            data = data.reshape(shape)

        return data

    return _inner


# ------------------------------------------------------------------------------
# AUX ISR
# ------------------------------------------------------------------------------

AUX_ISR_LOCATIONS = {
    'time':                                                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'First_Start_of_Observation_Time'],
    'freq_mie_USR_closest_to_rayleigh_filter_centre':       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'Freq_Mie_USR_Closest_to_Rayleigh_Filter_Centre'],
    'frequency_rayleigh_filter_centre':                     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'Freq_Rayleigh_Filter_Centre'],
    'num_of_valid_mie_results':                             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Stat/Num_Mie_Used'],
    'num_of_valid_rayleigh_results':                        ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Stat/Num_Rayleigh_Used'],
    'laser_frequency_offset':                               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Laser_Freq_Offset'],
    'mie_valid':                                            ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Mie_Valid'],
    'rayleigh_valid':                                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Rayleigh_Valid'],
    'fizeau_transmission':                                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Fizeau_Transmission'],
    'mie_response':                                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Mie_Response'],
    'rayleigh_channel_A_response':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Rayleigh_A_Response'],
    'rayleigh_channel_B_response':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Rayleigh_B_Response'],
    'num_of_raw_reference_pulses':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Stat/Num_Raw_Data'],
    'num_of_mie_reference_pulses':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Stat/Num_Mie_Used'],
    'num_of_rayleigh_reference_pulses':                     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Stat/Num_Rayleigh_Used'],
    'accumulated_laser_energy_mie':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Accumulated_Laser_Energy_Mie'],
    'mean_laser_energy_mie':                                ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mean_Laser_Energy_Mie'],
    'accumulated_laser_energy_rayleigh':                    ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Accumulated_Laser_Energy_Rayleigh'],
    'mean_laser_energy_rayleigh':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mean_Laser_Energy_Rayleigh'],
    'laser_energy_drift':                                   ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Laser_Energy_Drift'],
    'downhill_simplex_used':                                ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Downhill_Simplex_Used'],
    'num_of_iterations_mie_core_1':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mie_Core_1/Num_Iterations_Core_1'],
    'last_peak_difference_mie_core_1':                      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mie_Core_1/Last_Peak_Difference'],
    'FWHM_mie_core_2':                                      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mie_Core_2/Fwhm'],
    'num_of_iterations_mie_core_2':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mie_Core_2/Num_Iterations_Core_2'],
    'downhill_simplex_quality_flag':                        ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Downhill_Simplex_Used'],
    'rayleigh_spectrometer_temperature_9':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_9'],
    'rayleigh_spectrometer_temperature_10':                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_10'],
    'rayleigh_spectrometer_temperature_11':                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_11'],
    'rayleigh_thermal_hood_temperature_1':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'RSPT_Average_Temperature/Thermocouple_8_Ray_Spectrometer_Thermal_Hood_1'],
    'rayleigh_thermal_hood_temperature_2':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'RSPT_Average_Temperature/Thermocouple_9_Ray_Spectrometer_Thermal_Hood_2'],
    'rayleigh_thermal_hood_temperature_3':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'RSPT_Average_Temperature/Thermocouple_10_Ray_Spectrometer_Thermal_Hood_3'],
    'rayleigh_thermal_hood_temperature_4':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'RSPT_Average_Temperature/Thermocouple_11_Ray_Spectrometer_Thermal_Hood_4'],
    'rayleigh_optical_baseplate_avg_temperature':           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Optical_Baseplate_Average_Temperature'],
}

AUX_ISR_CALIBRATION_FIELDS = set([
    'time',
    'freq_mie_USR_closest_to_rayleigh_filter_centre',
    'frequency_rayleigh_filter_centre',
])

AUX_ISR_SCALAR_FIELDS = set([
    'num_of_valid_mie_results',
    'num_of_valid_rayleigh_results',
    'laser_frequency_offset',
    'mie_valid',
    'rayleigh_valid',
    'fizeau_transmission',
    'mie_response',
    'rayleigh_channel_A_response',
    'rayleigh_channel_B_response',
    'num_of_raw_reference_pulses',
    'num_of_mie_reference_pulses',
    'num_of_rayleigh_reference_pulses',
    'accumulated_laser_energy_mie',
    'mean_laser_energy_mie',
    'accumulated_laser_energy_rayleigh',
    'mean_laser_energy_rayleigh',
    'laser_energy_drift',
    'downhill_simplex_used',
    'num_of_iterations_mie_core_1',
    'last_peak_difference_mie_core_1',
    'FWHM_mie_core_2',
    'num_of_iterations_mie_core_2',
    'downhill_simplex_quality_flag',
    'rayleigh_spectrometer_temperature_9',
    'rayleigh_spectrometer_temperature_10',
    'rayleigh_spectrometer_temperature_11',
    'rayleigh_thermal_hood_temperature_1',
    'rayleigh_thermal_hood_temperature_2',
    'rayleigh_thermal_hood_temperature_3',
    'rayleigh_thermal_hood_temperature_4',
    'rayleigh_optical_baseplate_avg_temperature',
])


# ------------------------------------------------------------------------------
# AUX MET
# ------------------------------------------------------------------------------

AUX_MET_LOCATIONS = {
    'time_off_nadir':                               ['/geo_off_nadir', -1, 'amd_datetime'],
    'time_nadir':                                   ['/geo_nadir', -1, 'amd_datetime'],
    'latitude_off_nadir':                           ['/geo_off_nadir', -1, 'amd_latitude'],
    'latitude_nadir':                               ['/geo_nadir', -1, 'amd_latitude'],
    'longitude_off_nadir':                          ['/geo_off_nadir', -1, 'amd_longitude'],
    'longitude_nadir':                              ['/geo_nadir', -1, 'amd_longitude'],
    'surface_wind_component_u_off_nadir':           ['/met_off_nadir', -1, 'amd_us'],
    'surface_wind_component_u_nadir':               ['/met_nadir', -1, 'amd_us'],
    'surface_wind_component_v_off_nadir':           ['/met_off_nadir', -1, 'amd_vs'],
    'surface_wind_component_v_nadir':               ['/met_nadir', -1, 'amd_vs'],
    'surface_pressure_off_nadir':                   ['/met_off_nadir', -1, 'amd_ps'],
    'surface_pressure_nadir':                       ['/met_nadir', -1, 'amd_ps'],
    'surface_altitude_off_nadir':                   ['/met_off_nadir', -1, 'amd_zs'],
    'surface_altitude_nadir':                       ['/met_nadir', -1, 'amd_zs'],
    'layer_validity_flag_off_nadir':                ['/met_off_nadir', -1, 'profile_data', -1, 'amd_validity_flag'],
    'layer_validity_flag_nadir':                    ['/met_nadir', -1, 'profile_data', -1, 'amd_validity_flag'],
    'layer_pressure_off_nadir':                     ['/met_off_nadir', -1, 'profile_data', -1, 'amd_pnom'],
    'layer_pressure_nadir':                         ['/met_nadir', -1, 'profile_data', -1, 'amd_pnom'],
    'layer_altitude_off_nadir':                     ['/met_off_nadir', -1, 'profile_data', -1, 'amd_znom'],
    'layer_altitude_nadir':                         ['/met_nadir', -1, 'profile_data', -1, 'amd_znom'],
    'layer_temperature_off_nadir':                  ['/met_off_nadir', -1, 'profile_data', -1, 'amd_t'],
    'layer_temperature_nadir':                      ['/met_nadir', -1, 'profile_data', -1, 'amd_t'],
    'layer_wind_component_u_off_nadir':             ['/met_off_nadir', -1, 'profile_data', -1, 'amd_u'],
    'layer_wind_component_u_nadir':                 ['/met_nadir', -1, 'profile_data', -1, 'amd_u'],
    'layer_wind_component_v_off_nadir':             ['/met_off_nadir', -1, 'profile_data', -1, 'amd_v'],
    'layer_wind_component_v_nadir':                 ['/met_nadir', -1, 'profile_data', -1, 'amd_v'],
    'layer_rel_humidity_off_nadir':                 ['/met_off_nadir', -1, 'profile_data', -1, 'amd_rh'],
    'layer_rel_humidity_nadir':                     ['/met_nadir', -1, 'profile_data', -1, 'amd_rh'],
    'layer_spec_humidity_off_nadir':                ['/met_off_nadir', -1, 'profile_data', -1, 'amd_q'],
    'layer_spec_humidity_nadir':                    ['/met_nadir', -1, 'profile_data', -1, 'amd_q'],
    'layer_cloud_cover_off_nadir':                  ['/met_off_nadir', -1, 'profile_data', -1, 'amd_cc'],
    'layer_cloud_cover_nadir':                      ['/met_nadir', -1, 'profile_data', -1, 'amd_cc'],
    'layer_cloud_liquid_water_content_off_nadir':   ['/met_off_nadir', -1, 'profile_data', -1, 'amd_clwc'],
    'layer_cloud_liquid_water_content_nadir':       ['/met_nadir', -1, 'profile_data', -1, 'amd_clwc'],
    'layer_cloud_ice_water_content_off_nadir':      ['/met_off_nadir', -1, 'profile_data', -1, 'amd_ciwc'],
    'layer_cloud_ice_water_content_nadir':          ['/met_nadir', -1, 'profile_data', -1, 'amd_ciwc'],
}

AUX_MET_CALIBRATION_FIELDS = set([
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

AUX_MET_CALIBRATION_ARRAY_FIELDS = set([
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


# ------------------------------------------------------------------------------
# AUX MRC
# ------------------------------------------------------------------------------

AUX_MRC_LOCATIONS = {
    'lat_of_DEM_intersection':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Latitude_of_DEM_Intersection'],
    'lon_of_DEM_intersection':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Longitude_of_DEM_Intersection'],
    'time_freq_step':                                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Start_of_Observation_Time_Last_BRC'],
    'altitude':                                             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Altitude', -1],
    'satellite_range':                                      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Satellite_Range', -1],

    'frequency_offset':                                     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Offset'],
    'frequency_valid':                                      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Valid'],
    'measurement_response':                                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Response'],
    'measurement_response_valid':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Response_Valid'],
    'measurement_error_mie_response':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Error_Mie_Response'],
    'reference_pulse_response':                             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Response'],
    'reference_pulse_response_valid':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Response_Valid'],
    'reference_pulse_error_mie_response':                   ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Error_Mie_Response'],
    'normalised_useful_signal':                             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Normalized_Useful_Signal', -1],
    'mie_scattering_ratio':                                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Mie_Scattering_Ratio', -1],
    'num_measurements_usable':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Measurements_Usable'],
    'num_valid_measurements':                               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Valid_Measurements'],
    'num_reference_pulses_usable':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Reference_Pulses_Usable'],
    'num_mie_core_algo_fails_measurements':                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Mie_Core_Algo_Fails_Measurements'],
    'num_ground_echoes_not_detected_measurements':          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Ground_Echo_Not_Detected_Measurements'],
    'measurement_mean_sensitivity':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Mean_Sensitivity'],
    'measurement_zero_frequency':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Zero_Frequency'],
    'measurement_error_mie_response_std_dev':               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Error_Mie_Response_Std_Dev'],
    'measurement_offset_frequency':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Offset_Frequency'],
    'reference_pulse_mean_sensitivity':                     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Mean_Sensitivity'],
    'reference_pulse_zero_frequency':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Zero_Frequency'],
    'reference_pulse_error_mie_response_std_dev':           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Error_Mie_Response_Std_Dev'],
    'reference_pulse_offset_frequency':                     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Offset_Frequency'],
    'satisfied_min_valid_freq_steps_per_cal':               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Satisfied_Min_Valid_Freq_Per_Cal'],
    'freq_offset_data_monotonic':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Freq_Offset_Data_Monotonic'],
    'num_of_valid_frequency_steps':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Num_Valid_Frequency_Steps'],
    'measurement_mean_sensitivity_valid':                   ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Mean_Sensitivity_Valid'],
    'measurement_error_response_std_dev_valid':             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Error_Response_Std_Dev_Valid'],
    'measurement_zero_frequency_response_valid':            ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Zero_Freq_Response_Valid'],
    'measurement_data_monotonic':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Data_Monotonic'],
    'reference_pulse_mean_sensitivity_valid':               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Mean_Sensitivity_Valid'],
    'reference_pulse_error_response_std_dev_valid':         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Error_Response_Std_Dev_Valid'],
    'reference_pulse_zero_frequency_response_valid':        ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Zero_Freq_Response_Valid'],
    'reference_pulse_data_monotonic':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Data_Monotonic'],
    'mie_core_measurement_FWHM':                            ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/List_of_Calibration_MC_Results/Calibration_MC_Result', -1, 'Frequency_Step_MC_Results/FWHM'],
    'mie_core_measurement_amplitude':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/List_of_Calibration_MC_Results/Calibration_MC_Result', -1, 'Frequency_Step_MC_Results/Amplitude'],
    'mie_core_measurement_offset':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/List_of_Calibration_MC_Results/Calibration_MC_Result', -1, 'Frequency_Step_MC_Results/Offset'],

    # Albedo
    'albedo_nadir':                                         None,
}

calc_albedo_nadir_aux_mrc = _make_calc_albedo_nadir_aux_mrc_rrc(
    AUX_MRC_LOCATIONS['lon_of_DEM_intersection'],
    AUX_MRC_LOCATIONS['lat_of_DEM_intersection']
)

AUX_MRC_LOCATIONS['albedo_nadir'] = calc_albedo_nadir_aux_mrc


AUX_MRC_CALIBRATION_FIELDS = set([
    'measurement_mean_sensitivity',
    'measurement_zero_frequency',
    'measurement_error_mie_response_std_dev',
    'measurement_offset_frequency',
    'reference_pulse_mean_sensitivity',
    'reference_pulse_zero_frequency',
    'reference_pulse_error_mie_response_std_dev',
    'reference_pulse_offset_frequency',
    'satisfied_min_valid_freq_steps_per_cal',
    'freq_offset_data_monotonic',
    'num_of_valid_frequency_steps',
    'measurement_mean_sensitivity_valid',
    'measurement_error_response_std_dev_valid',
    'measurement_zero_frequency_response_valid',
    'measurement_data_monotonic',
    'reference_pulse_mean_sensitivity_valid',
    'reference_pulse_error_response_std_dev_valid',
    'reference_pulse_zero_frequency_response_valid',
    'reference_pulse_data_monotonic',
])

AUX_MRC_SCALAR_FIELDS = set([
    'lat_of_DEM_intersection',
    'lon_of_DEM_intersection',
    'time_freq_step',
    'frequency_offset',
    'frequency_valid',
    'measurement_response',
    'measurement_response_valid',
    'measurement_error_mie_response',
    'reference_pulse_response',
    'reference_pulse_response_valid',
    'reference_pulse_error_mie_response',
    'num_measurements_usable',
    'num_valid_measurements',
    'num_reference_pulses_usable',
    'num_mie_core_algo_fails_measurements',
    'num_ground_echoes_not_detected_measurements',
    'mie_core_measurement_FWHM',
    'mie_core_measurement_amplitude',
    'mie_core_measurement_offset',
    'albedo_nadir',
])

AUX_MRC_ARRAY_FIELDS = set([
    'altitude',
    'satellite_range',
    'normalised_useful_signal',
    'mie_scattering_ratio',
])


# ------------------------------------------------------------------------------
# AUX RRC
# ------------------------------------------------------------------------------


AUX_RRC_LOCATIONS = {
    'lat_of_DEM_intersection':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Latitude_of_DEM_Intersection'],
    'lon_of_DEM_intersection':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Longitude_of_DEM_Intersection'],
    'time_freq_step':                                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Start_of_Observation_Time_Last_BRC'],
    'altitude':                                             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Altitude', -1],
    'satellite_range':                                      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Satellite_Range', -1],
    'geoid_separation_obs':                                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'List_of_Geoid_Separations/Geoid_Separation', -1],
    # 'geoid_separation_freq_step':
    'frequency_offset':                                     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Offset'],
    'frequency_valid':                                      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Valid'],
    'ground_frequency_valid':                               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Frequency_Valid'],
    'measurement_response':                                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Response'],
    'measurement_response_valid':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Response_Valid'],
    'measurement_error_rayleigh_response':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Error_Rayleigh_Response'],
    'reference_pulse_response':                             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Response'],
    'reference_pulse_response_valid':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Response_Valid'],
    'reference_pulse_error_rayleigh_response':              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Measurement_Error_Rayleigh_Response'],
    'ground_measurement_response':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Measurement_Response'],
    'ground_measurement_response_valid':                    ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Measurement_Response_Valid'],
    'ground_measurement_error_rayleigh_response':           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Measurement_Error_Rayleigh_Response'],
    'normalised_useful_signal':                             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Normalized_Useful_Signal', -1],
    'num_measurements_usable':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Measurements_Usable'],
    'num_valid_measurements':                               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Valid_Measurements'],
    'num_reference_pulses_usable':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Reference_Pulses_Usable'],
    'num_measurements_valid_ground':                        ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Measurements_Valid_Ground'],
    'measurement_mean_sensitivity':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Mean_Sensitivity'],
    'measurement_zero_frequency':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Zero_Frequency'],
    'measurement_error_rayleigh_response_std_dev':          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Error_Rayleigh_Response_Std_Dev'],
    'measurement_offset_frequency':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Offset_Frequency'],
    'measurement_error_fit_coefficient':                    ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/List_of_Measurement_Error_Fit_Coefficients/Measurement_Error_Fit_Coefficient', -1],
    'reference_pulse_mean_sensitivity':                     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Mean_Sensitivity'],
    'reference_pulse_zero_frequency':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Zero_Frequency'],
    'reference_pulse_error_rayleigh_response_std_dev':      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Error_Rayleigh_Response_Std_Dev'],
    'reference_pulse_offset_frequency':                     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Offset_Frequency'],
    'reference_pulse_error_fit_coefficient':                ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/List_of_Reference_Pulse_Error_Fit_Coefficients/Reference_Pulse_Error_Fit_Coefficient', -1],
    'ground_measurement_mean_sensitivity':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Mean_Sensitivity'],
    'ground_measurement_zero_frequency':                    ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Zero_Frequency'],
    'ground_measurement_error_rayleigh_response_std_dev':   ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Error_Rayleigh_Response_Std_Dev'],
    'ground_measurement_offset_frequency':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Offset_Frequency'],
    'ground_measurement_error_fit_coefficient':             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/List_of_Ground_Measurement_Error_Fit_Coefficients/Ground_Measurement_Error_Fit_Coefficient', -1],
    'satisfied_min_valid_freq_steps_per_cal':               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Satisfied_Min_Valid_Freq_Per_Cal'],
    'satisfied_min_valid_ground_freq_steps_per_cal':        ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Satisfied_Min_Valid_Ground_Freq_Per_Cal'],
    'freq_offset_data_monotonic':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Freq_Offset_Data_Monotonic'],
    'num_of_valid_frequency_steps':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Num_Valid_Frequency_Steps'],
    'num_of_valid_ground_frequency_steps':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Num_Valid_Ground_Frequency_Steps'],
    'measurement_mean_sensitivity_valid':                   ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Mean_Sensitivity_Valid'],
    'measurement_error_response_std_dev_valid':             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Error_Response_Std_Dev_Valid'],
    'measurement_zero_frequency_response_valid':            ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Zero_Freq_Response_Valid'],
    'measurement_data_monotonic':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Data_Monotonic'],
    'reference_pulse_mean_sensitivity_valid':               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Mean_Sensitivity_Valid'],
    'reference_pulse_error_response_std_dev_valid':         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Error_Response_Std_Dev_Valid'],
    'reference_pulse_zero_frequency_response_valid':        ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Zero_Freq_Response_Valid'],
    'reference_pulse_data_monotonic':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Data_Monotonic'],
    'ground_measurement_mean_sensitivity_valid':            ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Mean_Sensitivity'],
    'ground_measurement_error_response_std_dev_valid':      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Error_Rayleigh_Response_Std_Dev'],
    'ground_measurement_zero_frequency_response_valid':     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Zero_Frequency'],
    'ground_measurement_data_monotonic':                    ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Ground_Measurement_Calibration_Validity/Data_Monotonic'],
    'rayleigh_spectrometer_temperature_9':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_9'],
    'rayleigh_spectrometer_temperature_10':                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_10'],
    'rayleigh_spectrometer_temperature_11':                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_11'],
    'rayleigh_thermal_hood_temperature_1':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'RSPT_Average_Temperature/Thermocouple_8_Ray_Spectrometer_Thermal_Hood_1'],
    'rayleigh_thermal_hood_temperature_2':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'RSPT_Average_Temperature/Thermocouple_9_Ray_Spectrometer_Thermal_Hood_2'],
    'rayleigh_thermal_hood_temperature_3':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'RSPT_Average_Temperature/Thermocouple_10_Ray_Spectrometer_Thermal_Hood_3'],
    'rayleigh_thermal_hood_temperature_4':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'RSPT_Average_Temperature/Thermocouple_11_Ray_Spectrometer_Thermal_Hood_4'],
    'rayleigh_optical_baseplate_avg_temperature':           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'Optical_Baseplate_Average'],

    # Albedo
    'albedo_nadir':                                         None,
}

calc_albedo_nadir_aux_rrc = _make_calc_albedo_nadir_aux_mrc_rrc(
    AUX_RRC_LOCATIONS['lon_of_DEM_intersection'],
    AUX_RRC_LOCATIONS['lat_of_DEM_intersection']
)

AUX_RRC_LOCATIONS['albedo_nadir'] = calc_albedo_nadir_aux_rrc

AUX_RRC_CALIBRATION_FIELDS = set([
    'measurement_mean_sensitivity',
    'measurement_zero_frequency',
    'measurement_error_rayleigh_response_std_dev',
    'measurement_offset_frequency',
    'reference_pulse_mean_sensitivity',
    'reference_pulse_zero_frequency',
    'reference_pulse_error_rayleigh_response_std_dev',
    'reference_pulse_offset_frequency',
    'ground_measurement_mean_sensitivity',
    'ground_measurement_zero_frequency',
    'ground_measurement_error_rayleigh_response_std_dev',
    'ground_measurement_offset_frequency',
    'satisfied_min_valid_freq_steps_per_cal',
    'satisfied_min_valid_ground_freq_steps_per_cal',
    'freq_offset_data_monotonic',
    'num_of_valid_frequency_steps',
    'num_of_valid_ground_frequency_steps',
    'measurement_mean_sensitivity_valid',
    'measurement_error_response_std_dev_valid',
    'measurement_zero_frequency_response_valid',
    'measurement_data_monotonic',
    'reference_pulse_mean_sensitivity_valid',
    'reference_pulse_error_response_std_dev_valid',
    'reference_pulse_zero_frequency_response_valid',
    'reference_pulse_data_monotonic',
    'ground_measurement_mean_sensitivity_valid',
    'ground_measurement_error_response_std_dev_valid',
    'ground_measurement_zero_frequency_response_valid',
    'ground_measurement_data_monotonic',
])

AUX_RRC_CALIBRATION_ARRAY_FIELDS = set([
    'measurement_error_fit_coefficient',
    'reference_pulse_error_fit_coefficient',
    'ground_measurement_error_fit_coefficient',
])

AUX_RRC_SCALAR_FIELDS = set([
    'lat_of_DEM_intersection',
    'lon_of_DEM_intersection',
    'time_freq_step',
    'frequency_offset',
    'frequency_valid',
    'ground_frequency_valid',
    'measurement_response',
    'measurement_response_valid',
    'measurement_error_rayleigh_response',
    'reference_pulse_response',
    'reference_pulse_response_valid',
    'reference_pulse_error_rayleigh_response',
    'ground_measurement_response',
    'ground_measurement_response_valid',
    'ground_measurement_error_rayleigh_response',
    'num_measurements_usable',
    'num_valid_measurements',
    'num_reference_pulses_usable',
    'num_measurements_valid_ground',
    'rayleigh_spectrometer_temperature_9',
    'rayleigh_spectrometer_temperature_10',
    'rayleigh_spectrometer_temperature_11',
    'rayleigh_thermal_hood_temperature_1',
    'rayleigh_thermal_hood_temperature_2',
    'rayleigh_thermal_hood_temperature_3',
    'rayleigh_thermal_hood_temperature_4',
    'rayleigh_optical_baseplate_avg_temperature',
    'albedo_nadir',
])

AUX_RRC_ARRAY_FIELDS = set([
    'altitude',
    'satellite_range',
    'geoid_separation_obs',
    'normalised_useful_signal',
])


# ------------------------------------------------------------------------------
# AUX ZWC
# ------------------------------------------------------------------------------

AUX_ZWC_LOCATIONS = {
    'time':                                                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Start_of_Observation_Time'],
    'lat_of_DEM_intersection':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Latitude_of_DEM_Intersection'],
    'lon_of_DEM_intersection':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Longitude_of_DEM_Intersection'],
    'roll_angle':                                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Roll_Angle'],
    'pitch_angle':                                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Pitch_Angle'],
    'yaw_angle':                                            ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Yaw_Angle'],
    'mie_range':                                            ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Mie_Satellite_Range_to_Target', -1],
    'rayleigh_range':                                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Rayleigh_Satellite_Range_to_Target', -1],
    'ZWC_result_type':                                      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'ZWC_Result_Type'],
    'mie_ground_correction_velocity':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Mie_Ground_Correction_Velocity'],
    'rayleigh_ground_correction_velocity':                  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Rayleigh_Ground_Correction_Velocity'],
    'num_of_mie_ground_bins':                               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Number_of_Mie_Ground_Bins'],
    'num_of_rayleigh_ground_bins':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Number_of_Rayleigh_Ground_Bins'],
    'mie_avg_ground_echo_bin_thickness':                    ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Mie_Average_Ground_Echo_Bin_Thickness'],
    'rayleigh_avg_ground_echo_bin_thickness':               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Rayleigh_Average_Ground_Echo_Bin_Thickness'],
    'mie_avg_ground_echo_bin_thickness_above_DEM':          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Mie_Average_Ground_Echo_Bin_Thickness_Above_DEM'],
    'rayleigh_avg_ground_echo_bin_thickness_above_DEM':     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Rayleigh_Average_Ground_Echo_Bin_Thickness_Above_DEM'],
    'mie_top_ground_bin_obs':                               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Mie_Min_Top_Ground_Bin'],
    'rayleigh_top_ground_bin_obs':                          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Rayleigh_Min_Top_Ground_Bin'],
    'mie_bottom_ground_bin_obs':                            ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Mie_Max_Bottom_Ground_Bin'],
    'rayleigh_bottom_ground_bin_obs':                       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Rayleigh_Max_Bottom_Ground_Bin'],
    'mie_measurements_used':                                ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Measurement_Used'],
    'mie_top_ground_bin_meas':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Top_Ground_Bin'],
    'mie_bottom_ground_bin_meas':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Bottom_Ground_Bin'],
    'mie_DEM_ground_bin':                                   ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Expected_Ground_Bin_Index'],
    'mie_height_difference_top_to_DEM_ground_bin':          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Height_Difference_Top_to_Expected'],
    'mie_height_difference_bottom_to_DEM_ground_bin':       ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Height_Difference_Bottom_to_Expected'],
    'mie_ground_bin_SNR_meas':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Mean_Ground_Bin_SNR'],
    'mie_topocentric_elevation_expected_ground_bin':        ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Topocentric_Elevation_Expected_Ground_Bin'],
    'mie_topocentric_elevation_top_ground_bin':             ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Topocentric_Elevation_Top_Ground_Bin'],
    'mie_topocentric_elevation_bottom_ground_bin':          ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Topocentric_Elevation_Bottom_Ground_Bin'],
    'mie_num_top_ground_bins_discarded':                    ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Num_Top_Ground_Bins_Discarded'],
    'mie_num_bottom_ground_bins_discarded':                 ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Num_Bottom_Ground_Bins_Discarded'],
    'rayleigh_measurements_used':                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Measurement_Used'],
    'rayleigh_top_ground_bin_meas':                         ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Top_Ground_Bin'],
    'rayleigh_bottom_ground_bin_meas':                      ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Bottom_Ground_Bin'],
    'rayleigh_DEM_ground_bin':                              ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Expected_Ground_Bin_Index'],
    'rayleigh_height_difference_top_to_DEM_ground_bin':     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Height_Difference_Top_to_Expected'],
    'rayleigh_height_difference_bottom_to_DEM_ground_bin':  ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Height_Difference_Bottom_to_Expected'],
    'rayleigh_channel_A_ground_SNR_meas':                   ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Channel_A_Mean_Ground_Bin_SNR'],
    'rayleigh_channel_B_ground_SNR_meas':                   ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Channel_B_Mean_Ground_Bin_SNR'],
    'rayleigh_topocentric_elevation_expected_ground_bin':   ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Topocentric_Elevation_Expected_Ground_Bin'],
    'rayleigh_topocentric_elevation_top_ground_bin':        ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Topocentric_Elevation_Top_Ground_Bin'],
    'rayleigh_topocentric_elevation_bottom_ground_bin':     ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Topocentric_Elevation_Bottom_Ground_Bin'],
    'rayleigh_num_top_ground_bins_discarded':               ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Num_Top_Ground_Bins_Discarded'],
    'rayleigh_num_bottom_ground_bins_discarded':            ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Num_Bottom_Ground_Bins_Discarded'],
    'DEM_height':                                           ['/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Info/DEM_Height', -1],
}


AUX_ZWC_CALIBRATION_FIELDS = set([
    'time',
    'lat_of_DEM_intersection',
    'lon_of_DEM_intersection',
    'roll_angle',
    'pitch_angle',
    'yaw_angle',
    'ZWC_result_type',
    'mie_ground_correction_velocity',
    'rayleigh_ground_correction_velocity',
    'num_of_mie_ground_bins',
    'num_of_rayleigh_ground_bins',
    'mie_avg_ground_echo_bin_thickness',
    'mie_topocentric_elevation_expected_ground_bin',
    'mie_topocentric_elevation_top_ground_bin',
    'mie_topocentric_elevation_bottom_ground_bin',
    'mie_num_top_ground_bins_discarded',
    'mie_num_bottom_ground_bins_discarded',
    'rayleigh_avg_ground_echo_bin_thickness',
    'mie_avg_ground_echo_bin_thickness_above_DEM',
    'rayleigh_avg_ground_echo_bin_thickness_above_DEM',
    'mie_top_ground_bin_obs',
    'rayleigh_top_ground_bin_obs',
    'mie_bottom_ground_bin_obs',
    'rayleigh_bottom_ground_bin_obs',
    'rayleigh_topocentric_elevation_expected_ground_bin',
    'rayleigh_topocentric_elevation_top_ground_bin',
    'rayleigh_topocentric_elevation_bottom_ground_bin',
    'rayleigh_num_top_ground_bins_discarded',
    'rayleigh_num_bottom_ground_bins_discarded',
])

AUX_ZWC_CALIBRATION_ARRAY_FIELDS = set([
    'mie_range',
    'rayleigh_range',
    'DEM_height',
])

AUX_ZWC_SCALAR_FIELDS = set([
    'mie_measurements_used',
    'mie_top_ground_bin_meas',
    'mie_bottom_ground_bin_meas',
    'mie_DEM_ground_bin',
    'mie_height_difference_top_to_DEM_ground_bin',
    'mie_height_difference_bottom_to_DEM_ground_bin',
    'mie_ground_bin_SNR_meas',
    'rayleigh_measurements_used',
    'rayleigh_top_ground_bin_meas',
    'rayleigh_bottom_ground_bin_meas',
    'rayleigh_DEM_ground_bin',
    'rayleigh_height_difference_top_to_DEM_ground_bin',
    'rayleigh_height_difference_bottom_to_DEM_ground_bin',
    'rayleigh_channel_A_ground_SNR_meas',
    'rayleigh_channel_B_ground_SNR_meas',
])


TYPE_TO_FIELDS = {
    'ISR': (
        AUX_ISR_LOCATIONS,
        AUX_ISR_CALIBRATION_FIELDS,
        set([]),
        AUX_ISR_SCALAR_FIELDS,
        set([])
    ),
    'MET': (
        AUX_MET_LOCATIONS,
        AUX_MET_CALIBRATION_FIELDS,
        AUX_MET_CALIBRATION_ARRAY_FIELDS,
        set([]),
        set([])
    ),
    'MRC': (
        AUX_MRC_LOCATIONS,
        AUX_MRC_CALIBRATION_FIELDS,
        set([]),
        AUX_MRC_SCALAR_FIELDS,
        AUX_MRC_ARRAY_FIELDS
    ),
    'RRC': (
        AUX_RRC_LOCATIONS,
        AUX_RRC_CALIBRATION_FIELDS,
        AUX_RRC_CALIBRATION_ARRAY_FIELDS,
        AUX_RRC_SCALAR_FIELDS,
        AUX_RRC_ARRAY_FIELDS
    ),
    'ZWC': (
        AUX_ZWC_LOCATIONS,
        AUX_ZWC_CALIBRATION_FIELDS,
        AUX_ZWC_CALIBRATION_ARRAY_FIELDS,
        AUX_ZWC_SCALAR_FIELDS,
        set([])
    ),
}

AUX_FILE_TYPE_PATH = (
    'Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/File_Type'
)


def fetch_ground_points(codafile, product_type=None):
    product_type = product_type or codafile.product_type[:7]

    assert product_type in (
        'AUX_ISR', 'AUX_MRC', 'AUX_RRC', 'AUX_ZWC', 'AUX_MET'
    )
    if product_type == 'AUX_ISR':
        return None
    elif product_type == 'AUX_MET':
        locations = AUX_MET_LOCATIONS
    elif product_type == 'AUX_MRC':
        locations = AUX_MRC_LOCATIONS
    elif product_type == 'AUX_RRC':
        locations = AUX_RRC_LOCATIONS
    elif product_type == 'AUX_ZWC':
        locations = AUX_ZWC_LOCATIONS

    if product_type in ('AUX_MRC', 'AUX_RRC'):
        return [
            (lon if lon < 180 else lon - 360, lat)
            for lon, lat in zip(
                chain.from_iterable(
                    codafile.fetch(*locations['lon_of_DEM_intersection'])
                ),
                chain.from_iterable(
                    codafile.fetch(*locations['lat_of_DEM_intersection'])
                )
            )
        ]

    elif product_type == 'AUX_MET':
        items = sorted(zip(
            # np.concatenate(
                list(codafile.fetch(*locations['time_off_nadir'])) +
                list(codafile.fetch(*locations['time_nadir'])),
            # ),
            # np.concatenate(
                list(codafile.fetch(*locations['latitude_off_nadir'])) +
                list(codafile.fetch(*locations['latitude_nadir'])),
            # ),
            # np.concatenate(
                list(codafile.fetch(*locations['latitude_off_nadir'])) +
                list(codafile.fetch(*locations['latitude_nadir'])),
            # ),
        ), key=lambda i: i[0])

        return [
            (lon if lon < 180 else lon - 360, lat)
            for _, lon, lat in items
        ]

    else:
        return [
            (lon if lon < 180 else lon - 360, lat)
            for lon, lat in zip(
                codafile.fetch(*locations['lon_of_DEM_intersection']),
                codafile.fetch(*locations['lat_of_DEM_intersection'])
            )
        ]


def between(value, min_value=None, max_value=None):
    is_between = True
    if min_value is not None:
        is_between &= value >= min_value
    if max_value is not None:
        is_between &= value <= max_value

    return is_between


def _array_to_list(data):
    if isinstance(data, np.ndarray):
        isobject = data.dtype == np.object
        data = data.tolist()
        if isobject:
            data = [
                _array_to_list(obj) for obj in data
            ]
    return data


def extract_data(filenames, filters, fields, aux_type, convert_arrays=False):
    """
    """

    aux_type = str(aux_type)  # to convert unicode to str
    filenames = [filenames] if isinstance(filenames, basestring) else filenames

    (
        locations,
        calibration_fields,
        calibration_array_fields,
        scalar_fields,
        array_fields
    ) = TYPE_TO_FIELDS[aux_type]

    calibration_filters = {
        name: filter_value
        for name, filter_value in filters.items()
        if name in calibration_fields or name in calibration_array_fields
    }

    frequency_filters = {
        name: filter_value
        for name, filter_value in filters.items()
        if name not in calibration_filters
    }

    requested_calibration_fields = [
        field
        for field in fields
        if field in calibration_fields or field in calibration_array_fields
    ]

    requested_frequency_fields = [
        field
        for field in fields
        if field not in requested_calibration_fields
    ]

    for filename in filenames:
        calibration_data = {}
        frequency_data = {}

        with CODAFile(filename) as cf:
            # make a mask of all calibrations to be included, by only looking at
            # the fields for whole calibrations
            calibration_mask = None
            for field_name, filter_value in calibration_filters.items():
                path = locations[field_name][:]
                new_mask = make_mask(
                    access_location(cf, path),
                    filter_value.get('min'), filter_value.get('max'),
                    field_name in calibration_array_fields
                )
                calibration_mask = combine_mask(new_mask, calibration_mask)

            # when the mask is done, create an array of indices for calibrations
            # to be included
            calibration_nonzero_ids = None
            if calibration_mask is not None:
                calibration_nonzero_ids = np.nonzero(calibration_mask)
                calibration_ids = calibration_nonzero_ids[0]

            elif aux_type == 'MET':
                num_calibrations = cf.get_size('/geo_nadir')[0]
                calibration_ids = range(num_calibrations)
            else:
                num_calibrations = cf.get_size(
                    '/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_%s/'
                    'List_of_Data_Set_Records/Data_Set_Record' % aux_type
                )[0]
                calibration_ids = range(num_calibrations)

            # load all desired values for the requested calibrations
            for field_name in requested_calibration_fields:
                path = locations[field_name]
                field_data = access_location(cf, path)

                if calibration_nonzero_ids is not None:
                    field_data = field_data[calibration_nonzero_ids]

                if convert_arrays:
                    field_data = _array_to_list(field_data)

                # write out data
                calibration_data[field_name] = field_data

            # build a mask of all frequencies within a specific calibration
            frequency_masks = None
            for field_name, filter_value in frequency_filters.items():
                path = locations[field_name]

                field_data = access_location(cf, path)[calibration_ids]

                new_masks = [
                    make_mask(
                        frequency_field_data,
                        filter_value.get('min'), filter_value.get('max'),
                        field_name in array_fields
                    )
                    for frequency_field_data in field_data
                ]

                if frequency_masks:
                    frequency_masks = [
                        combine_mask(new_mask_, frequency_mask)
                        for new_mask_, frequency_mask
                        in izip(new_masks, frequency_masks)
                    ]
                else:
                    frequency_masks = new_masks

            # make an array of all indices to be included
            frequency_ids = None
            if frequency_masks is not None:
                frequency_ids = [
                    np.nonzero(frequency_mask)
                    for frequency_mask in frequency_masks
                ]

            # iterate over all requested frequency fields and write the
            # possibly subset data to the output
            for field_name in requested_frequency_fields:
                path = locations[field_name]

                field_data = access_location(cf, path)[calibration_ids]

                if frequency_ids is not None:
                    field_data = [
                        frequency_field_data[mask]
                        for frequency_field_data, mask
                        in izip(field_data, frequency_ids)
                    ]

                if convert_arrays:
                    field_data = [
                        _array_to_list(frequency_field_data)
                        for frequency_field_data in field_data
                    ]
                else:
                    # TODO: steck as object array!
                    pass

                # write out data
                frequency_data[field_name] = field_data

        yield calibration_data, frequency_data


def get_aux_type(collection):
    name = collection.range_type.name
    if not name.startswith('AUX'):
        raise Exception("Collection %s  is not an AUX collection" % collection)

    return name.split("_")[1]


# test_file = '/mnt/data/AE_OPER_AUX_ISR_1B_20071002T103629_20071002T110541_0002.EEF'
test_file = '/mnt/data/AE_OPER_AUX_MRC_1B_20071031T021229_20071031T022829_0002.EEF'
# test_file = '/mnt/data/AE_OPER_AUX_RRC_1B_20071031T021229_20071031T022829_0002.EEF'
# test_file = '/mnt/data/AE_OPER_AUX_ZWC_1B_20071101T202641_20071102T000841_0001.EEF'


def main():

    from django.utils.timezone import utc
    import datetime
    fields=[u'lat_of_DEM_intersection', u'lon_of_DEM_intersection', u'mie_ground_correction_velocity', u'rayleigh_ground_correction_velocity', u'roll_angle', u'pitch_angle', u'yaw_angle', u'num_of_mie_ground_bins', u'rayleigh_avg_ground_echo_bin_thickness', u'mie_avg_ground_echo_bin_thickness_above_DEM', u'rayleigh_avg_ground_echo_bin_thickness_above_DEM', u'rayleigh_channel_A_ground_SNR_meas', u'mie_DEM_ground_bin', u'ZWC_result_type']
    filters={'time': {'max': datetime.datetime(2007, 11, 2, 0, 0, 8, tzinfo=utc), 'min': datetime.datetime(2007, 11, 1, 20, 26, 8, tzinfo=utc)}}
    next(extract_data("/mnt/data/AE_OPER_AUX_ZWC_1B_20071101T202641_20071102T000841_0001.EEF", filters, fields, "ZWC", True))

    # from pprint import pprint

    # data = extract_data('/mnt/data/AE_OPER_AUX_ISR_1B_20071002T103629_20071002T110541_0002.EEF', {
    #     # 'freq_mie_USR_closest_to_rayleigh_filter_centre': {
    #     #     'max': 1,
    #     # },
    #     # 'mie_response': {
    #     #     'min': 10,
    #     #     'max': 12,
    #     # }
    # }, [
    #     'mie_valid',
    #     # 'freq_mie_USR_closest_to_rayleigh_filter_centre',
    # ], 'ISR')

    # # data = extract_data(
    # #     '/mnt/data/AE_OPER_AUX_MRC_1B_20071031T021229_20071031T022829_0002.EEF',
    # #     {
    # #         'lat_of_DEM_intersection': {
    # #             'max': 0,
    # #         },
    # #         'altitude': {
    # #             'min': 25000,
    # #             # 'max': 12,
    # #         }
    # #     }, [
    # #         'measurement_mean_sensitivity',
    # #         'lat_of_DEM_intersection',
    # #         'altitude',
    # #     ], 'MRC'
    # # )

    # pprint(dict(data))

    # # from pprint import pprint
    # # import contextlib
    # # import time
    # # from aeolus.coda_utils import CODAFile, datetime_to_coda_time
    # # import numpy as np

    # # @contextlib.contextmanager
    # # def timed(name):
    # #     start = time.time()
    # #     yield
    # #     print str(int((time.time() - start) * 1000)) + 'ms', name

    # # # locations = AUX_ISR_LOCATIONS
    # # locations = AUX_MRC_LOCATIONS
    # # # locations = AUX_RRC_LOCATIONS
    # # # locations = AUX_ZWC_LOCATIONS

    # # with CODAFile(test_file) as cf:
    # #     for name, path in locations.items():
    # #         #with timed(name):
    # #         value = cf.fetch(*path)
    # #         if isinstance(value, np.ndarray):
    # #             shape = []
    # #             while value.dtype == np.object:
    # #                 value = np.stack(value)
    # #                 shape.extend(value.shape)

    # #             print "shape", shape, value.shape
    # #         # print cf.get_size(*path)
    # #         print name, value.shape if not isinstance(value, (int, float)) else 0



if __name__ == "__main__":
    main()
