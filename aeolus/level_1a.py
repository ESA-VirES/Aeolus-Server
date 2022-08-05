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
    #['/geolocation', -1 'observation_aocs/x_position'],
    #['/geolocation', -1 'observation_aocs/y_position'],
    #['/geolocation', -1 'observation_aocs/z_position'],
    #['/geolocation', -1 'observation_aocs/x_velocity'],
    #['/geolocation', -1 'observation_aocs/y_velocity'],
    #['/geolocation', -1 'observation_aocs/z_velocity'],
    'AOCS_roll_angle':                          ['/geolocation', -1, 'observation_aocs/roll_angle'],
    'AOCS_pitch_angle':                         ['/geolocation', -1, 'observation_aocs/pitch_angle'],
    'AOCS_yaw_angle':                           ['/geolocation', -1, 'observation_aocs/yaw_angle'],
}

# all measurement fields and their respective coda path for their location in
# the DBL file.
MEASUREMENT_LOCATIONS = {
    'time':                                     ['/geolocation', -1, 'measurement_aocs', -1, 'measurement_centroid_time'],
    #['/geolocation', -1, 'measurement_aocs', -1, 'x_position'],
    #['/geolocation', -1, 'measurement_aocs', -1, 'y_position'],
    #['/geolocation', -1, 'measurement_aocs', -1, 'z_position'],
    #['/geolocation', -1, 'measurement_aocs', -1, 'x_velocity'],
    #['/geolocation', -1, 'measurement_aocs', -1, 'y_velocity'],
    #['/geolocation', -1, 'measurement_aocs', -1, 'z_velocity'],
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
    'mie_measurement_data': ['/mie_measurement', -1, 'mie_measurement_data'], #[750,20]
    #/rayleigh_measurement[?]/start_of_observation_time
    #/rayleigh_measurement[?]/num_measurements
    #/rayleigh_measurement[?]/num_height_bins
    #/rayleigh_measurement[?]/num_accd_columns
    'rayleigh_measurement_data': ['/rayleigh_measurement', -1, 'rayleigh_measurement_data'], #[750,20]
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
