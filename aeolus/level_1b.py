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

from datetime import datetime
from collections import defaultdict

import numpy as np

from aeolus.coda_utils import CODAFile, datetime_to_coda_time


# all observation fields and their respective coda path for their location in
# the DBL file.
OBSERVATION_LOCATIONS = {
    'time': ('/geolocation', -1, 'observation_aocs/observation_centroid_time'),
    'longitude_of_DEM_intersection': ('/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/longitude_of_dem_intersection'),
    'latitude_of_DEM_intersection': ('/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/latitude_of_dem_intersection'),
    'altitude_of_DEM_intersection': ('/geolocation', -1, 'observation_geolocation/geolocation_of_dem_intersection/altitude_of_dem_intersection'),
    'mie_longitude': ('/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'longitude_of_height_bin'),
    'mie_latitude': ('/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'latitude_of_height_bin'),
    'rayleigh_longitude': ('/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'longitude_of_height_bin'),
    'rayleigh_latitude': ('/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'latitude_of_height_bin'),
    'mie_altitude': ('/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'altitude_of_height_bin'),
    'rayleigh_altitude': ('/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'altitude_of_height_bin'),
    'mie_range': ('/geolocation', -1, 'observation_geolocation/observation_mie_geolocation', -1, 'satellite_range_of_height_bin'),
    'rayleigh_range': ('/geolocation', -1, 'observation_geolocation/observation_rayleigh_geolocation', -1, 'satellite_range_of_height_bin'),
    'geoid_separation': ('/geolocation', -1, 'observation_geolocation/geoid_separation'),
    'velocity_at_DEM_intersection': ('/geolocation', -1, 'observation_geolocation/line_of_sight_velocity'),
    'AOCS_pitch_angle': ('/geolocation', -1, 'observation_aocs/pitch_angle'),
    'AOCS_roll_angle': ('/geolocation', -1, 'observation_aocs/roll_angle'),
    'AOCS_yaw_angle': ('/geolocation', -1, 'observation_aocs/yaw_angle'),
    'mie_HLOS_wind_speed': ('/wind_velocity', -1, 'observation_wind_profile/mie_altitude_bin_wind_info', -1, 'wind_velocity'),
    'rayleigh_HLOS_wind_speed': ('/wind_velocity', -1, 'observation_wind_profile/rayleigh_altitude_bin_wind_info', -1, 'wind_velocity'),
    'mie_signal_intensity': ('/useful_signal', -1, 'observation_useful_signals/mie_altitude_bin_useful_signal_info', -1, 'useful_signal'),
    'rayleigh_signal_channel_A_intensity': ('/useful_signal', -1, 'observation_useful_signals/rayleigh_altitude_bin_useful_signal_info', -1, 'useful_signal_channel_a'),
    'rayleigh_signal_channel_B_intensity': ('/useful_signal', -1, 'observation_useful_signals/rayleigh_altitude_bin_useful_signal_info', -1, 'useful_signal_channel_b'),
    # 'rayleigh_signal_intensity: '),
    # 'rayleigh_signal_intensity: '),
    'mie_ground_velocity': ('/ground_wind_detection', -1, 'mie_ground_correction_velocity'),
    'rayleigh_ground_velocity': ('/ground_wind_detection', -1, 'rayleigh_ground_correction_velocity'),
    'mie_HBE_ground_velocity': ('/ground_wind_detection', -1, 'hbe_mie_ground_correction_velocity'),
    'rayleigh_HBE_ground_velocity': ('/ground_wind_detection', -1, 'hbe_rayleigh_ground_correction_velocity'),
    'mie_total_ZWC': ('/ground_wind_detection', -1, 'mie_channel_total_zero_wind_correction'),
    'rayleigh_total_ZWC': ('/ground_wind_detection', -1, 'rayleigh_channel_total_zero_wind_correction'),
    'mie_scattering_ratio': ('/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'refined_scattering_ratio_mie'),
    'mie_SNR': ('/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'mie_signal_to_noise_ratio'),
    'rayleigh_channel_A_SNR': ('/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'rayleigh_signal_to_noise_ratio_channel_a'),
    'rayleigh_channel_B_SNR': ('/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'rayleigh_signal_to_noise_ratio_channel_b'),
    # 'rayleigh_SNR: '),
    # 'rayleigh_SNR: '),
    'mie_error_quantifier': ('/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'error_quantifier_mie'),
    'rayleigh_error_quantifier': ('/product_confidence_data', -1, 'observation_pcd/observation_alt_bin_pcd', -1, 'error_quantifier_rayleigh'),
    'average_laser_energy': ('/product_confidence_data', -1, 'observation_pcd/avg_uv_energy'),
    'laser_frequency': ('/product_confidence_data', -1, 'observation_pcd/avg_laser_frequency_offset'),
    'rayleigh_bin_quality_flag': ('/wind_velocity', -1, 'observation_wind_profile/rayleigh_altitude_bin_wind_info', -1, 'bin_quality_flag'),
    'mie_bin_quality_flag': ('/wind_velocity', -1, 'observation_wind_profile/mie_altitude_bin_wind_info', -1, 'bin_quality_flag'),
    'rayleigh_reference_pulse_quality_flag': ('/wind_velocity', -1, 'observation_wind_profile/rayleigh_reference_pulse_quality_flag'),
    'mie_reference_pulse_quality_flag': ('/wind_velocity', -1, 'observation_wind_profile/mie_reference_pulse_quality_flag'),
}

# all measurement fields and their respective coda path for their location in
# the DBL file.
MEASUREMENT_LOCATIONS = {
    'time': ('/geolocation', -1, 'measurement_aocs', -1, 'measurement_centroid_time'),
    'longitude_of_DEM_intersection': ('/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/longitude_of_dem_intersection'),
    'latitude_of_DEM_intersection': ('/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/latitude_of_dem_intersection'),
    'altitude_of_DEM_intersection': ('/geolocation', -1, 'measurement_geolocation', -1, 'geolocation_of_dem_intersection/altitude_of_dem_intersection'),
    'mie_longitude': ('/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'longitude_of_height_bin'),
    'mie_latitude': ('/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'latitude_of_height_bin'),
    'rayleigh_longitude': ('/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'longitude_of_height_bin'),
    'rayleigh_latitude': ('/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'latitude_of_height_bin'),
    'mie_altitude': ('/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation', -1, 'altitude_of_height_bin'),
    'rayleigh_altitude': ('/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation', -1, 'altitude_of_height_bin'),
    'velocity_at_DEM_intersection': ('/geolocation', -1, 'measurement_geolocation', -1, 'aocs_los_velocity'),
    'AOCS_pitch_angle': ('/geolocation', -1, 'measurement_aocs', -1, 'pitch_angle'),
    'AOCS_roll_angle': ('/geolocation', -1, 'measurement_aocs', -1, 'roll_angle'),
    'AOCS_yaw_angle': ('/geolocation', -1, 'measurement_aocs', -1, 'yaw_angle'),
    'mie_HLOS_wind_speed': ('/wind_velocity', -1, 'measurement_wind_profile', -1, 'mie_altitude_bin_wind_info', -1, 'wind_velocity'),
    'rayleigh_HLOS_wind_speed': ('/wind_velocity', -1, 'measurement_wind_profile', -1, 'rayleigh_altitude_bin_wind_info', -1, 'wind_velocity'),
    'mie_signal_intensity': ('/useful_signal', -1, 'measurement_useful_signal', -1, 'mie_altitude_bin_useful_signal_info', -1, 'useful_signal'),
    'rayleigh_signal_channel_A_intensity': ('/useful_signal', -1, 'measurement_useful_signal', -1, 'rayleigh_altitude_bin_useful_signal_info', -1, 'useful_signal_channel_a'),
    'rayleigh_signal_channel_B_intensity': ('/useful_signal', -1, 'measurement_useful_signal', -1, 'rayleigh_altitude_bin_useful_signal_info', -1, 'useful_signal_channel_b'),
    'mie_ground_velocity': ('/wind_velocity', -1, 'measurement_wind_profile', -1, 'mie_ground_wind_velocity'),
    'rayleigh_ground_velocity': ('/wind_velocity', -1, 'measurement_wind_profile', -1, 'rayleigh_ground_wind_velocity'),
    'mie_scattering_ratio': ('/product_confidence_data', -1, 'measurement_pcd', -1, 'meas_alt_bin_pcd', -1, 'refined_scattering_ratio_mie'),
    'mie_SNR': ('/product_confidence_data', -1, 'measurement_pcd', -1, 'meas_alt_bin_pcd', -1, 'mie_signal_to_noise_ratio'),
    'rayleigh_channel_A_SNR': ('/product_confidence_data', -1, 'measurement_pcd', -1, 'meas_alt_bin_pcd', -1, 'rayleigh_signal_to_noise_ratio_channel_a'),
    'rayleigh_channel_B_SNR': ('/product_confidence_data', -1, 'measurement_pcd', -1, 'meas_alt_bin_pcd', -1, 'rayleigh_signal_to_noise_ratio_channel_b'),
    'average_laser_energy': ('/product_confidence_data', -1, 'measurement_pcd', -1, 'avg_uv_energy'),
    'laser_frequency': ('/product_confidence_data', -1, 'measurement_pcd', -1, 'avg_laser_frequency_offset'),
    'rayleigh_bin_quality_flag': ('/wind_velocity', -1, 'measurement_wind_profile', -1, 'rayleigh_altitude_bin_wind_info', -1, 'bin_quality_flag'),
    'mie_bin_quality_flag': ('/wind_velocity', -1, 'measurement_wind_profile', -1, 'mie_altitude_bin_wind_info', -1, 'bin_quality_flag'),
    'rayleigh_reference_pulse_quality_flag': ('/wind_velocity', -1, 'measurement_wind_profile', -1, 'rayleigh_reference_pulse_quality_flag'),
    'mie_reference_pulse_quality_flag': ('/wind_velocity', -1, 'measurement_wind_profile', -1, 'mie_reference_pulse_quality_flag'),
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
    'mie_SNR',
    'rayleigh_channel_A_SNR',
    'rayleigh_channel_B_SNR',
    'mie_error_quantifier',
    'rayleigh_error_quantifier',
    'rayleigh_bin_quality_flag',
    'mie_bin_quality_flag',
])


def _make_mask(data, min_value=None, max_value=None, is_array=False):
    """ Utility function to generate a bitmask with the given filter values.
        When the data itself is an Array of arrays, the filter is broadcast to
        the sub-arrays and a summary value is used (if any of the sub-arrays
        values are ``True`)
    """
    if is_array:
        mask = np.empty(data.shape, dtype=bool)
        for i, array in enumerate(data):
            mask[i] = np.any(_make_mask(array, min_value, max_value, False))
        return mask

    if isinstance(min_value, datetime):
        min_value = datetime_to_coda_time(min_value)
    if isinstance(max_value, datetime):
        max_value = datetime_to_coda_time(max_value)

    if min_value is not None and min_value == max_value:
        mask = data == min_value
    elif min_value is not None and max_value is not None:
        mask = np.logical_and(
            data <= max_value,
            data >= min_value
        )
    elif min_value is not None:
        mask = data >= min_value
    elif max_value is not None:
        mask = data <= max_value
    else:
        raise NotImplementedError
    return mask


def _combine_mask(mask_a, mask_b=None):
    """ Combine two bit masks of the same shape. One may be unset (and thus
        ``None``).
    """
    if mask_b is None:
        return mask_a

    return np.logical_and(mask_a, mask_b)


def extract_data(filenames, filters, observation_fields, measurement_fields,
                 simple_observation_filters=False, convert_arrays=False):
    """ Extract the data from the given filename(s) and apply the given filters.
    """
    filenames = [filenames] if isinstance(filenames, basestring) else filenames

    out_observation_data = defaultdict(list)
    out_measurement_data = defaultdict(list)

    for cf in [CODAFile(filename) for filename in filenames]:
        with cf:
            # create a mask for observation data
            observation_mask = None
            for field_name, filter_value in filters.items():
                assert (
                    field_name in OBSERVATION_LOCATIONS or
                    field_name in MEASUREMENT_LOCATIONS
                )

                data = cf.fetch(*OBSERVATION_LOCATIONS[field_name])
                new_mask = _make_mask(
                    data, filter_value.get('min'), filter_value.get('max'),
                    field_name in ARRAY_FIELDS
                )

                observation_mask = _combine_mask(new_mask, observation_mask)

            if observation_mask is not None:
                filtered_observation_ids = np.nonzero(observation_mask)
            else:
                filtered_observation_ids = None

            # fetch the requested observation fields, filter accordingly and
            # write to the output dict
            for field_name in observation_fields:
                assert field_name in OBSERVATION_LOCATIONS
                data = cf.fetch(*OBSERVATION_LOCATIONS[field_name])
                if filtered_observation_ids is not None:
                    data = data[filtered_observation_ids]

                # convert to simple list instead of numpy array if requested
                if convert_arrays and isinstance(data, np.ndarray):
                    data = data.tolist()

                out_observation_data[field_name].append(data)

            # if we filter the measurements by observation ID, then use the
            # filtered observation IDs as mask for the measurements.
            # otherwise, use the full range of observations
            if filtered_observation_ids is not None and \
                    simple_observation_filters:
                observation_iterator = filtered_observation_ids[0]
            else:
                observation_iterator = xrange(
                    cf.get_size('/geolocation')[0]
                )

            # iterate all (or selected) observations
            for observation_id in observation_iterator:
                # build a measurement bitmask:
                # loop over all filter fields, fetch the filter field data and
                # perform the filter.
                measurement_mask = None
                for field_name, filter_value in filters.items():
                    # only apply filters for measurement fields
                    if field_name not in MEASUREMENT_LOCATIONS:
                        continue

                    path = MEASUREMENT_LOCATIONS[field_name]
                    data = cf.fetch(path[0], int(observation_id), *path[2:])

                    new_mask = _make_mask(
                        data, filter_value.get('min'), filter_value.get('max'),
                        field_name in ARRAY_FIELDS
                    )

                    # combine the masks
                    measurement_mask = _combine_mask(new_mask, measurement_mask)

                filtered_measurement_ids = np.nonzero(measurement_mask)
                for field_name in measurement_fields:
                    path = MEASUREMENT_LOCATIONS[field_name]
                    data = cf.fetch(path[0], int(observation_id), *path[2:])

                    # convert to simple list instead of numpy array if requested
                    if convert_arrays and isinstance(data, np.ndarray):
                        data = data.tolist()

                    out_measurement_data[field_name].append(
                        data[filtered_measurement_ids]
                    )

    return out_observation_data, out_measurement_data




###

test_file = '/mnt/data/AE_OPER_ALD_U_N_1B_20151001T104454059_005379000_046330_0001/AE_OPER_ALD_U_N_1B_20151001T104454059_005379000_046330_0001.DBL'


def main():
    pass
    from pprint import pprint
    pprint(
        extract_data(test_file, {
            'time': {
                'min': datetime(2015, 10, 01, 10, 44, 54),
                'max': datetime(2015, 10, 01, 10, 45, 0),
                # datetime(2015, 10, 01, 10, 50, 54)
            },
            'mie_longitude': {'min': 100, 'max': 120},
        }, [
            'mie_range', 'longitude_of_DEM_intersection'
        ], [
            #'rayleigh_channel_A_SNR'
        ], True)
    )



    # from aeolus.coda_utils import CODAFile, datetime_to_coda_time

    # cf = CODAFile()


    # data = cf.fetch(*MEASUREMENT_LOCATIONS['time'])
    # print type(data), data.dtype

    # + MEASUREMENT_LOCATIONS.items():

    # for name, path in OBSERVATION_LOCATIONS.items():
    #     #print(name, path[0], 0, path[2:])
    #     data = cf.fetch(path[0], 0, *path[2:])
    #     assert isinstance(data, np.ndarray) if name in ARRAY_FIELDS else isinstance(data, (float, int))

    #     # if name not in ARRAY_FIELDS:
    #     #     print name, data
    #     # else: print name, data


    # for name, path in MEASUREMENT_LOCATIONS.items():
    #     data = cf.fetch(path[0], 0, path[2], 0, *path[4:])
    #     assert isinstance(data, np.ndarray) if name in ARRAY_FIELDS else isinstance(data, (float, int))
    #     if name not in ARRAY_FIELDS:
    #         print name, data
    #     else: print name, data



# main()
