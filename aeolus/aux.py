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

from itertools import chain


AUX_ISR_LOCATIONS = {
    'freq_mie_USR_closest_to_rayleigh_filter_centre': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'Freq_Mie_USR_Closest_to_Rayleigh_Filter_Centre'),
    'frequency_Rayleigh_filter_centre': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'Freq_Rayleigh_Filter_Centre'),
    'num_of_valid_mie_results': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Stat/Num_Mie_Used'),
    'num_of_valid_rayleigh_results': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Stat/Num_Rayleigh_Used'),
    'laser_frequency_offset': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Laser_Freq_Offset'),
    'mie_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Mie_Valid'),
    'rayleigh_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Rayleigh_Valid'),
    'fizeau_transmission': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Fizeau_Transmission'),
    'mie_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Mie_Response'),
    'rayleigh_channel_A_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Rayleigh_A_Response'),
    'rayleigh_channel_B_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Rayleigh_B_Response'),
    'num_of_raw_reference_pulses': ('/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/Total_Num_of_Reference_Pulses'),
    'num_of_mie_reference_pulses': ('/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/Num_of_Mie_Reference_Pulses_Used'),
    'num_of_rayleigh_reference_pulses': ('/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/Num_of_Rayleigh_Reference_Pulses_Used'),
    'accumulated_laser_energy_Mie': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Accumulated_Laser_Energy_Mie'),
    'mean_laser_energy_mie': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mean_Laser_Energy_Mie'),
    'accumulated_laser_energy_rayleigh': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Accumulated_Laser_Energy_Rayleigh'),
    'mean_laser_energy_rayleigh': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mean_Laser_Energy_Rayleigh'),
    'laser_energy_drift': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Laser_Energy_Drift'),
    'downhill_simplex_used': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Downhill_Simplex_Used'),
    'num_of_iterations_mie_core_1': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mie_Core_1/Num_Iterations_Core_1'),
    'last_peak_difference_mie_core_1': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mie_Core_1/Last_Peak_Difference'),
    'FWHM_mie_core_2': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mie_Core_2/Fwhm'),
    'num_of_iterations_mie_core_2': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Mie_Core_2/Num_Iterations_Core_2'),
    'downhill_simplex_quality_flag': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Data_Quality/Downhill_Simplex_Used'),
    'rayleigh_spectrometer_temperature_9': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_9'),
    'rayleigh_spectrometer_temperature_10': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_10'),
    'rayleigh_spectrometer_temperature_11': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_11'),
    'rayleigh_thermal_hood_temperature_1': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'RSPT_Average_Temperature/Thermocouple_8_Ray_Spectrometer_Thermal_Hood_1'),
    'rayleigh_thermal_hood_temperature_2': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'RSPT_Average_Temperature/Thermocouple_9_Ray_Spectrometer_Thermal_Hood_2'),
    'rayleigh_thermal_hood_temperature_3': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'RSPT_Average_Temperature/Thermocouple_10_Ray_Spectrometer_Thermal_Hood_3'),
    'rayleigh_thermal_hood_temperature_4': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'RSPT_Average_Temperature/Thermocouple_11_Ray_Spectrometer_Thermal_Hood_4'),
    'rayleigh_optical_baseplate_avg_temperature': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ISR/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_ISR_Results/ISR_Result', -1, 'Optical_Baseplate_Average_Temperature'),
}

AUX_MRC_LOCATIONS = {
    'lat_of_DEM_intersection': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Latitude_of_DEM_Intersection'),
    'lon_of_DEM_intersection': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Longitude_of_DEM_Intersection'),
    'time_freq_step': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Start_of_Observation_Time_Last_BRC'),
    'altitude': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Altitude', -1),
    'satellite_range': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Satellite_Range', -1),
    'frequency_offset': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Offset'),
    'frequency_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Valid'),
    'measurement_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Response'),
    'measurement_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Response_Valid'),
    'measurement_error_mie_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Error_Mie_Response'),
    'reference_pulse_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Response'),
    'reference_pulse_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Response_Valid'),
    'reference_pulse_error_mie_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Error_Mie_Response'),
    'normalised_useful_signal': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Normalized_Useful_Signal', -1),
    'mie_scattering_ratio': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Mie_Scattering_Ratio', -1),
    'num_measurements_usable': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Measurements_Usable'),
    'num_valid_measurements': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Valid_Measurements'),
    'num_reference_pulses_usable': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Reference_Pulses_Usable'),
    'num_mie_core_algo_fails_measurements': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Mie_Core_Algo_Fails_Measurements'),
    'num_ground_echoes_not_detected_measurements': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Ground_Echo_Not_Detected_Measurements'),
    'measurement_mean_sensitivity': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Mean_Sensitivity'),
    'measurement_zero_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Zero_Frequency'),
    'measurement_error_mie_response_std_dev': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Error_Mie_Response_Std_Dev'),
    'measurement_offset_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Offset_Frequency'),
    'reference_pulse_mean_sensitivity': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Mean_Sensitivity'),
    'reference_pulse_zero_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Zero_Frequency'),
    'reference_pulse_error_mie_response_std_dev': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Error_Mie_Response_Std_Dev'),
    'reference_pulse_offset_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Offset_Frequency'),
    'satisfied_min_valid_freq_steps_per_cal': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Satisfied_Min_Valid_Freq_Per_Cal'),
    'freq_offset_data_monotonic': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Freq_Offset_Data_Monotonic'),
    'num_of_valid_frequency_steps': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Num_Valid_Frequency_Steps'),
    'measurement_mean_sensitivity_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Mean_Sensitivity_Valid'),
    'measurement_error_response_std_dev_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Error_Response_Std_Dev_Valid'),
    'measurement_zero_frequency_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Zero_Freq_Response_Valid'),
    'measurement_data_monotonic': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Data_Monotonic'),
    'reference_pulse_mean_sensitivity_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Mean_Sensitivity_Valid'),
    'reference_pulse_error_response_std_dev_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Error_Response_Std_Dev_Valid'),
    'reference_pulse_zero_frequency_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Zero_Freq_Response_Valid'),
    'reference_pulse_data_monotonic': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Data_Monotonic'),
    'mie_core_measurement_FWHM': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/List_of_Calibration_MC_Results/Calibration_MC_Result', -1, 'List_of_Measurement_MC_Results/Measurement_MC_Results', -1, 'FWHM'),
    'mie_core_measurement_amplitude': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/List_of_Calibration_MC_Results/Calibration_MC_Result', -1, 'List_of_Measurement_MC_Results/Measurement_MC_Results', -1, 'Amplitude'),
    'mie_core_measurement_offset': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_MRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/List_of_Calibration_MC_Results/Calibration_MC_Result', -1, 'List_of_Measurement_MC_Results/Measurement_MC_Results', -1, 'Offset'),
}

AUX_RRC_LOCATIONS = {
    'lat_of_DEM_intersection': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Latitude_of_DEM_Intersection'),
    'lon_of_DEM_intersection': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Longitude_of_DEM_Intersection'),
    'time_freq_step': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Start_of_Observation_Time_Last_BRC'),
    'altitude': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Altitude', -1),
    'satellite_range': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'Satellite_Range', -1),
    'geoid_separation_obs': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Geolocations/Frequency_Step_Geolocation', -1, 'List_of_Geoid_Separations/Geoid_Separation', -1),
    # 'geoid_separation_freq_step':
    'frequency_offset': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Offset'),
    'frequency_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Valid'),
    'ground_frequency_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Frequency_Valid'),
    'measurement_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Response'),
    'measurement_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Response_Valid'),
    'measurement_error_rayleigh_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Measurement_Error_Rayleigh_Response'),
    'reference_pulse_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Response'),
    'reference_pulse_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Reference_Pulse_Response_Valid'),
    'reference_pulse_error_rayleigh_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Measurement_Error_Rayleigh_Response'),
    'ground_measurement_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Measurement_Response'),
    'ground_measurement_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Measurement_Response_Valid'),
    'ground_measurement_error_rayleigh_response': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Ground_Measurement_Error_Rayleigh_Response'),
    'normalised_useful_signal': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Normalized_Useful_Signal', -1),
    'num_measurements_usable': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Measurements_Usable'),
    'num_valid_measurements': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Valid_Measurements'),
    'num_reference_pulses_usable': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Reference_Pulses_Usable'),
    'num_measurements_valid_ground': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Results/Frequency_Step_Result', -1, 'Frequency_Step_Data_Statistics/Num_Measurements_Valid_Ground'),
    'measurement_mean_sensitivity': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Mean_Sensitivity'),
    'measurement_zero_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Zero_Frequency'),
    'measurement_error_rayleigh_response_std_dev': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Error_Rayleigh_Response_Std_Dev'),
    'measurement_offset_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/Measurement_Offset_Frequency'),
    'measurement_error_fit_coefficient': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Response_Calibration/List_of_Measurement_Error_Fit_Coefficients/Measurement_Error_Fit_Coefficient', -1),
    'reference_pulse_mean_sensitivity': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Mean_Sensitivity'),
    'reference_pulse_zero_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Zero_Frequency'),
    'reference_pulse_error_rayleigh_response_std_dev': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Error_Rayleigh_Response_Std_Dev'),
    'reference_pulse_offset_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/Reference_Pulse_Offset_Frequency'),
    'reference_pulse_error_fit_coefficient': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Reference_Pulse_Response_Calibration/List_of_Reference_Pulse_Error_Fit_Coefficients/Reference_Pulse_Error_Fit_Coefficient', -1),
    'ground_measurement_mean_sensitivity': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Mean_Sensitivity'),
    'ground_measurement_zero_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Zero_Frequency'),
    'ground_measurement_error_rayleigh_response_std_dev': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Error_Rayleigh_Response_Std_Dev'),
    'ground_measurement_offset_frequency': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Offset_Frequency'),
    'ground_measurement_error_fit_coefficient': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/List_of_Ground_Measurement_Error_Fit_Coefficients/Ground_Measurement_Error_Fit_Coefficient', -1),
    'satisfied_min_valid_freq_steps_per_cal': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Satisfied_Min_Valid_Freq_Per_Cal'),
    'satisfied_min_valid_ground_freq_steps_per_cal': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Satisfied_Min_Valid_Ground_Freq_Per_Cal'),
    'freq_offset_data_monotonic': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Freq_Offset_Data_Monotonic'),
    'num_of_valid_frequency_steps': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Num_Valid_Frequency_Steps'),
    'num_of_valid_ground_frequency_steps': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Num_Valid_Ground_Frequency_Steps'),
    'measurement_mean_sensitivity_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Mean_Sensitivity_Valid'),
    'measurement_error_response_std_dev_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Error_Response_Std_Dev_Valid'),
    'measurement_zero_frequency_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Zero_Freq_Response_Valid'),
    'measurement_data_monotonic': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Measurement_Calibration_Validity/Data_Monotonic'),
    'reference_pulse_mean_sensitivity_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Mean_Sensitivity_Valid'),
    'reference_pulse_error_response_std_dev_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Error_Response_Std_Dev_Valid'),
    'reference_pulse_zero_frequency_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Zero_Freq_Response_Valid'),
    'reference_pulse_data_monotonic': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Reference_Pulse_Calibration_Validity/Data_Monotonic'),
    'ground_measurement_mean_sensitivity_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Mean_Sensitivity'),
    'ground_measurement_error_response_std_dev_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Error_Rayleigh_Response_Std_Dev'),
    'ground_measurement_zero_frequency_response_valid': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Ground_Measurement_Response_Calibration/Ground_Measurement_Zero_Frequency'),
    'ground_measurement_data_monotonic': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Calibration_Validity_Indicators/Ground_Measurement_Calibration_Validity/Data_Monotonic'),
    'rayleigh_spectrometer_temperature_9': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_9'),
    'rayleigh_spectrometer_temperature_10': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_10'),
    'rayleigh_spectrometer_temperature_11': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'Etalon_Average_Temperature/Ray_Spectrometer_Temp_11'),
    'rayleigh_thermal_hood_temperature_1': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'RSPT_Average_Temperature/Thermocouple_8_Ray_Spectrometer_Thermal_Hood_1'),
    'rayleigh_thermal_hood_temperature_2': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'RSPT_Average_Temperature/Thermocouple_9_Ray_Spectrometer_Thermal_Hood_2'),
    'rayleigh_thermal_hood_temperature_3': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'RSPT_Average_Temperature/Thermocouple_10_Ray_Spectrometer_Thermal_Hood_3'),
    'rayleigh_thermal_hood_temperature_4': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'RSPT_Average_Temperature/Thermocouple_11_Ray_Spectrometer_Thermal_Hood_4'),
    'rayleigh_optical_baseplate_avg_temperature': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_RRC/List_of_Data_Set_Records/Data_Set_Record', -1, 'List_of_Frequency_Step_Temperatures/Frequency_Step_Temperature', -1, 'Optical_Baseplate_Average'),
}


AUX_ZWC_LOCATIONS = {
    'lat_of_DEM_intersection': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Latitude_of_DEM_Intersection'),
    'lon_of_DEM_intersection': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Longitude_of_DEM_Intersection'),
    'roll_angle': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Roll_Angle'),
    'pitch_angle': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Pitch_Angle'),
    'yaw_angle': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Yaw_Angle'),
    'mie_range': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Mie_Satellite_Range_to_Target', -1),
    'rayleigh_range': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Observation_Info/Rayleigh_Satellite_Range_to_Target', -1),
    'ZWC_result_type': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'ZWC_Result_Type'),
    'mie_ground_correction_velocity': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Mie_Ground_Correction_Velocity'),
    'rayleigh_ground_correction_velocity': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Rayleigh_Ground_Correction_Velocity'),
    'num_of_mie_ground_bins': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Number_of_Mie_Ground_Bins'),
    'mie_avg_ground_echo_bin_thickness': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Mie_Average_Ground_Echo_Bin_Thickness'),
    'rayleigh_avg_ground_echo_bin_thickness': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Rayleigh_Average_Ground_Echo_Bin_Thickness'),
    'mie_avg_ground_echo_bin_thickness_above_DEM': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Mie_Average_Ground_Echo_Bin_Thickness_Above_DEM'),
    'rayleigh_avg_ground_echo_bin_thickness_above_DEM': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Rayleigh_Average_Ground_Echo_Bin_Thickness_Above_DEM'),
    'mie_top_ground_bin_obs': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Mie_Min_Top_Ground_Bin'),
    'rayleigh_top_ground_bin_obs': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Rayleigh_Min_Top_Ground_Bin'),
    'mie_bottom_ground_bin_obs': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Mie_Max_Bottom_Ground_Bin'),
    'rayleigh_bottom_ground_bin_obs': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/Rayleigh_Max_Bottom_Ground_Bin'),
    'mie_measurements_used': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Measurement_Used'),
    'mie_top_ground_bin_meas': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Top_Ground_Bin'),
    'mie_bottom_ground_bin_meas': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Bottom_Ground_Bin'),
    'mie_DEM_ground_bin': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Expected_Ground_Bin_Index'),
    'mie_height_difference_top_to_DEM_ground_bin': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Height_Difference_Top_to_Expected'),
    'mie_ground_bin_SNR_meas': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Mie_Measurement_Validity_Indicators/Mie_Measurement_Validity_Indicators', -1, 'Mean_Ground_Bin_SNR'),
    'rayleigh_measurements_used': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Measurement_Used'),
    'rayleigh_top_ground_bin_meas': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Top_Ground_Bin'),
    'rayleigh_bottom_ground_bin_meas': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Bottom_Ground_Bin'),
    'rayleigh_DEM_ground_bin': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Expected_Ground_Bin_Index'),
    'rayleigh_height_difference_top_to_DEM_ground_bin': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Height_Difference_Top_to_Expected'),
    'rayleigh_channel_A_ground_SNR_meas': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Channel_A_Mean_Ground_Bin_SNR'),
    'rayleigh_channel_B_ground_SNR_meas': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Validity_Indicators/List_of_Rayleigh_Measurement_Validity_Indicators/Rayleigh_Measurement_Validity_Indicators', -1, 'Channel_B_Mean_Ground_Bin_SNR'),
    'DEM_height': ('/Earth_Explorer_File/Data_Block/Auxiliary_Calibration_ZWC/List_of_Data_Set_Records/Data_Set_Record', -1, 'Measurement_Info/DEM_Height', -1),
}


AUX_FILE_TYPE_PATH = (
    'Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/File_Type'
)


def fetch_ground_points(codafile, product_type=None):
    product_type = product_type or codafile.fetch(AUX_FILE_TYPE_PATH)[:7]

    assert product_type in ('AUX_ISR', 'AUX_MRC', 'AUX_RRC', 'AUX_ZWC')
    if product_type == 'AUX_ISR':
        return None
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

    else:
        return [
            (lon if lon < 180 else lon - 360, lat)
            for lon, lat in zip(
                codafile.fetch(*locations['lon_of_DEM_intersection']),
                codafile.fetch(*locations['lat_of_DEM_intersection'])
            )
        ]
