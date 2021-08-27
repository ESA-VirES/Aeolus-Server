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

import numpy as np

from aeolus.coda_utils import access_location
from aeolus.albedo import sample_offnadir
from aeolus.extraction.measurement import MeasurementDataExtractor


def calculate_L1B_centroid_time_obs(cf):
    """
    """
    t_start = cf.fetch(*OBSERVATION_LOCATIONS['L1B_start_time_obs'])
    t_centroid = np.zeros(t_start.shape, t_start.dtype)
    t_centroid[:-1] = (t_start[:-1] + t_start[1:]) / 2
    if t_centroid.shape[0] > 1:
        t_centroid[-1] = t_start[-1] + (t_start[-1] - t_start[-2]) / 2

    return t_centroid


def calculate_longitude_of_DEM_intersection_obs(cf):
    """ nearest neighbour using measurement values and time
    """
    t_centroid = calculate_L1B_centroid_time_obs(cf)

    measurement_lons = cf.fetch(
        *MEASUREMENT_LOCATIONS['longitude_of_DEM_intersection_meas']
    )

    measurement_times = cf.fetch(
        *MEASUREMENT_LOCATIONS['L1B_time_meas']
    )

    lon_of_DEM_intersection = np.zeros(t_centroid.shape)

    for i, values in enumerate(zip(measurement_lons, measurement_times)):
        measurement_lon, measurement_time = values
        lon_of_DEM_intersection[i] = measurement_lon[(
            np.abs(measurement_time - t_centroid[i])
        ).argmin()]

    return lon_of_DEM_intersection


def calculate_latitude_of_DEM_intersection_obs(cf):
    """ nearest neighbour using measurement values and time
    """
    t_centroid = calculate_L1B_centroid_time_obs(cf)

    measurement_lats = cf.fetch(
        *MEASUREMENT_LOCATIONS['latitude_of_DEM_intersection_meas']
    )

    measurement_times = cf.fetch(
        *MEASUREMENT_LOCATIONS['L1B_time_meas']
    )

    lat_of_DEM_intersection = np.zeros(t_centroid.shape)

    for i, values in enumerate(zip(measurement_lats, measurement_times)):
        measurement_lat, measurement_time = values
        lat_of_DEM_intersection[i] = measurement_lat[(
            np.abs(measurement_time - t_centroid[i])
        ).argmin()]

    return lat_of_DEM_intersection


def calculate_altitude_of_DEM_intersection_obs(cf):
    """ maximum using measurement values and time
    """
    measurement_altitude = cf.fetch(
        *MEASUREMENT_LOCATIONS['altitude_of_DEM_intersection_meas']
    )

    return np.amax(np.vstack(measurement_altitude), axis=1)


def calculate_mie_altitude_obs(cf):
    """ horizontal averaging
    """
    mie_altitude_meas = cf.fetch(*MEASUREMENT_LOCATIONS['mie_altitude_meas'])
    mie_altitude_obs = np.zeros((mie_altitude_meas.shape[0],), np.object)

    for i, alts in enumerate(mie_altitude_meas):
        mie_altitude_obs[i] = np.average(alts, axis=0)

    return mie_altitude_obs


def calculate_rayleigh_altitude_obs(cf):
    """ horizontal averaging
    """
    rayleigh_altitude_meas = cf.fetch(
        *MEASUREMENT_LOCATIONS['rayleigh_altitude_meas']
    )
    rayleigh_altitude_obs = np.zeros(
        (rayleigh_altitude_meas.shape[0],), np.object
    )

    for i, alts in enumerate(rayleigh_altitude_meas):
        rayleigh_altitude_obs[i] = np.average(alts, axis=0)

    return rayleigh_altitude_obs


def calculate_group_end_time(cf):
    """ use last measurement/observation end time
    """
    # print cf.fetch(*GROUP_LOCATIONS['group_start_obs'])
    # print cf.fetch(*GROUP_LOCATIONS['group_start_meas_obs'])

    end_obs = cf.fetch(*GROUP_LOCATIONS['group_end_obs']) - 1
    end_meas = cf.fetch(*GROUP_LOCATIONS['group_end_meas_obs']) - 1

    measurement_times = np.vstack(
        cf.fetch(*MEASUREMENT_LOCATIONS['L1B_time_meas'])
    )
    return measurement_times[(end_obs, end_meas)]


def calculate_group_centroid_time(cf):
    """ average from start/end
    """
    start_times = cf.fetch(*GROUP_LOCATIONS['group_start_time'])
    end_times = calculate_group_end_time(cf)

    return (start_times + end_times) / 2


def location_for_observation(location, observation_id):
    return [location[0], observation_id] + location[2:]


def calculate_albedo_off_nadir(cf, observation_id=None):
    """ Retrieves the albedo off_nadir values for the given file
    """
    start = cf.fetch_date('/mph/sensing_start')
    stop = cf.fetch_date('/mph/sensing_stop')

    mean = start + (stop - start) / 2
    if observation_id is not None:
        lons = access_location(cf,
            location_for_observation(
                MEASUREMENT_LOCATIONS['longitude_of_DEM_intersection_meas'],
                observation_id,
            )
        )

        lats = access_location(cf,
            location_for_observation(
                MEASUREMENT_LOCATIONS['latitude_of_DEM_intersection_meas'],
                observation_id,
            )
        )
        if observation_id == -1:
            lons = np.vstack(lons)
            lats = np.vstack(lats)
    else:
        lons = access_location(cf,
            OBSERVATION_LOCATIONS['longitude_of_DEM_intersection_obs'],
        )
        lats = access_location(cf,
            OBSERVATION_LOCATIONS['latitude_of_DEM_intersection_obs'],
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


def calculate_albedo_off_nadir_meas(cf, observation_id=-1):
    return calculate_albedo_off_nadir(cf, observation_id)


def calculate_SCA_longitude_of_DEM_intersection(cf):
    sca_mask = cf.fetch('/meas_pcd', -1, 'l2a_processing_qc/sca_applied')
    values = access_location(cf,
        OBSERVATION_LOCATIONS['longitude_of_DEM_intersection_obs']
    )
    return values[sca_mask.nonzero()]


def calculate_SCA_latitude_of_DEM_intersection(cf):
    sca_mask = cf.fetch('/meas_pcd', -1, 'l2a_processing_qc/sca_applied')
    values = access_location(cf,
        OBSERVATION_LOCATIONS['latitude_of_DEM_intersection_obs']
    )
    return values[sca_mask.nonzero()]


def _alternative_location_accessor(location, alternative_location):
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

get_SCA_QC_flag = _alternative_location_accessor(
    ['/sca_pcd', -1, 'qc_flag'],
    ['/sca_pcd', -1, 'bin_1_clear']
)


OBSERVATION_LOCATIONS = {
    'L1B_start_time_obs':                           ['/geolocation', -1, 'start_of_obs_time'],
    'L1B_centroid_time_obs':                        calculate_L1B_centroid_time_obs,
    'longitude_of_DEM_intersection_obs':            calculate_longitude_of_DEM_intersection_obs,
    'latitude_of_DEM_intersection_obs':             calculate_latitude_of_DEM_intersection_obs,
    'altitude_of_DEM_intersection_obs':             calculate_altitude_of_DEM_intersection_obs,
    'geoid_separation_obs':                         ['/geolocation', -1, 'geoid_separation'],
    'mie_altitude_obs':                             calculate_mie_altitude_obs,
    'rayleigh_altitude_obs':                        calculate_rayleigh_altitude_obs,
    'L1B_num_of_meas_per_obs':                      ['/geolocation', -1, 'num_meas_eff'],
    'sca_mask':                                     ['/meas_pcd', -1, 'l2a_processing_qc/sca_applied'],
    'ica_mask':                                     ['/meas_pcd', -1, 'l2a_processing_qc/ica_applied'],
    'mca_mask':                                     ['/meas_pcd', -1, 'l2a_processing_qc/mca_applied'],
    # Albedo values:
    'albedo_off_nadir':                             calculate_albedo_off_nadir,
}

MEASUREMENT_LOCATIONS = {
    'L1B_time_meas':                                ['/geolocation', -1, 'measurement_geolocation', -1, 'centroid_time'],
    'longitude_of_DEM_intersection_meas':           ['/geolocation', -1, 'measurement_geolocation', -1, 'longitude_of_dem_intersection'],
    'latitude_of_DEM_intersection_meas':            ['/geolocation', -1, 'measurement_geolocation', -1, 'latitude_of_dem_intersection'],
    'altitude_of_DEM_intersection_meas':            ['/geolocation', -1, 'measurement_geolocation', -1, 'altitude_of_dem_intersection'],
    'mie_altitude_meas':                            ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation_height_bin', -1, 'altitude_of_height_bin'],
    'rayleigh_altitude_meas':                       ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation_height_bin', -1, 'altitude_of_height_bin'],

    # Albedo values:
    'albedo_off_nadir':                             calculate_albedo_off_nadir_meas,
}

GROUP_LOCATIONS = {
    'group_start_time':                             ['/group_pcd', -1, 'starttime'],
    'group_end_time':                               calculate_group_end_time,
    'group_centroid_time':                          calculate_group_centroid_time,
    'group_middle_bin_start_altitude':              ['/group_optical_properties', -1, 'group_geolocation_middle_bins/start_altitude'],
    'group_middle_bin_stop_altitude':               ['/group_optical_properties', -1, 'group_geolocation_middle_bins/stop_altitude'],
    'group_start_obs':                              ['/group_pcd', -1, 'brc_start'],
    'group_start_meas_obs':                         ['/group_pcd', -1, 'measurement_start'],
    'group_end_obs':                                ['/group_pcd', -1, 'brc_end'],
    'group_end_meas_obs':                           ['/group_pcd', -1, 'measurement_end'],
    'group_height_bin_index':                       ['/group_pcd', -1, 'height_bin_index'],

    'group_extinction_variance':                    ['/group_pcd', -1, 'particle_extinction_variance'],
    'group_backscatter_variance':                   ['/group_pcd', -1, 'particle_backscatter_variance'],
    'group_LOD_variance':                           ['/group_pcd', -1, 'particle_lod_variance'],
    'group_middle_bin_extinction_variance_top':     ['/group_pcd', -1, 'mid_particle_extinction_variance_top'],
    'group_middle_bin_backscatter_variance_top':    ['/group_pcd', -1, 'mid_particle_backscatter_variance_top'],
    'group_middle_bin_LOD_variance_top':            ['/group_pcd', -1, 'mid_particle_lod_variance_top'],
    'group_middle_bin_BER_variance_top':            ['/group_pcd', -1, 'mid_particle_ber_variance_top'],
    'group_middle_bin_extinction_variance_bottom':  ['/group_pcd', -1, 'mid_particle_extinction_variance_bot'],
    'group_middle_bin_backscatter_variance_bottom': ['/group_pcd', -1, 'mid_particle_backscatter_variance_bot'],
    'group_middle_bin_LOD_variance_bottom':         ['/group_pcd', -1, 'mid_particle_lod_variance_bot'],
    'group_middle_bin_BER_variance_bottom':         ['/group_pcd', -1, 'mid_particle_ber_variance_bot'],

    'group_extinction':                             ['/group_optical_properties', -1, 'group_optical_property/group_extinction'],
    'group_backscatter':                            ['/group_optical_properties', -1, 'group_optical_property/group_backscatter'],
    'group_LOD':                                    ['/group_optical_properties', -1, 'group_optical_property/group_lod'],
    'group_SR':                                     ['/group_optical_properties', -1, 'group_optical_property/group_sr'],
    'group_middle_bin_extinction_top':              ['/group_optical_properties', -1, 'group_optical_property_middle_bins/mid_extinction_top'],
    'group_middle_bin_backscatter_top':             ['/group_optical_properties', -1, 'group_optical_property_middle_bins/mid_backscatter_top'],
    'group_middle_bin_LOD_top':                     ['/group_optical_properties', -1, 'group_optical_property_middle_bins/mid_lod_top'],
    'group_middle_bin_BER_top':                     ['/group_optical_properties', -1, 'group_optical_property_middle_bins/mid_ber_top'],
    'group_middle_bin_extinction_bottom':           ['/group_optical_properties', -1, 'group_optical_property_middle_bins/mid_extinction_bot'],
    'group_middle_bin_backscatter_bottom':          ['/group_optical_properties', -1, 'group_optical_property_middle_bins/mid_backscatter_bot'],
    'group_middle_bin_LOD_bottom':                  ['/group_optical_properties', -1, 'group_optical_property_middle_bins/mid_lod_bot'],
    'group_middle_bin_BER_bottom':                  ['/group_optical_properties', -1, 'group_optical_property_middle_bins/mid_ber_bot'],
    # 'scene_classification_aladin_cloud_flag':       [''], # TODO: multiple possibilities: '/scene_classification', -1, 'aladin_cloud_flag/clrh', '/scene_classification', -1, 'aladin_cloud_flag/clsr', '/scene_classification', -1, 'aladin_cloud_flag/downclber', '/scene_classification', -1, 'aladin_cloud_flag/topclber'
    'scene_classification_NWP_cloud_flag':          ['/scene_classification', -1, 'nwp_cloud_flag'],
    'scene_classification_group_class_reliability': ['/scene_classification', -1, 'l2a_group_class_reliability'],
}

ICA_LOCATIONS = {
    'ICA_time_obs':                                 ['/ica_optical_properties', -1, 'starttime'],
    'ICA_QC_flag':                                  ['/ica_pcd', -1, 'qc_flag'],
    'ICA_filling_case':                             ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'case'],
    'ICA_extinction':                               ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'extinction'],
    'ICA_backscatter':                              ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'backscatter'],
    'ICA_LOD':                                      ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'lod'],
}

SCA_LOCATIONS = {
    'SCA_QC_flag':                                  get_SCA_QC_flag,
    'SCA_processing_qc_flag':                       ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'processing_qc_flag'],
    'SCA_extinction_variance':                      ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'extinction_variance'],
    'SCA_backscatter_variance':                     ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'backscatter_variance'],
    'SCA_LOD_variance':                             ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'lod_variance'],
    'SCA_middle_bin_processing_qc_flag':            ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'processing_qc_flag'],
    'SCA_middle_bin_extinction_variance':           ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'extinction_variance'],
    'SCA_middle_bin_backscatter_variance':          ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'backscatter_variance'],
    'SCA_middle_bin_LOD_variance':                  ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'lod_variance'],
    'SCA_middle_bin_BER_variance':                  ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'ber_variance'],
    'SCA_time_obs':                                 ['/sca_optical_properties', -1, 'starttime'],
    'SCA_extinction':                               ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'extinction'],
    'SCA_backscatter':                              ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'backscatter'],
    'SCA_LOD':                                      ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'lod'],
    'SCA_SR':                                       ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'sr'],
    'SCA_middle_bin_altitude_obs':                  ['/sca_optical_properties', -1, 'geolocation_middle_bins', -1, 'altitude'],
    'SCA_middle_bin_extinction':                    ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'extinction'],
    'SCA_middle_bin_backscatter':                   ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'backscatter'],
    'SCA_middle_bin_LOD':                           ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'lod'],
    'SCA_middle_bin_BER':                           ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'ber'],
    'SCA_longitude_of_DEM_intersection':            calculate_SCA_longitude_of_DEM_intersection,
    'SCA_latitude_of_DEM_intersection':             calculate_SCA_latitude_of_DEM_intersection,
}

MCA_LOCATIONS = {
    'MCA_time_obs':                                 ['/mca_optical_properties', -1, 'starttime'],
    'MCA_clim_BER':                                 ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'climber'],
    'MCA_extinction':                               ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'extinction'],
    'MCA_LOD':                                      ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'lod'],
}

ARRAY_FIELDS = set([
    'mie_altitude_obs',
    'rayleigh_altitude_obs',
    'mie_altitude_meas',
    'rayleigh_altitude_meas',
    'SCA_extinction_variance',
    'SCA_backscatter_variance',
    'SCA_LOD_variance',
    'SCA_middle_bin_extinction_variance',
    'SCA_middle_bin_backscatter_variance',
    'SCA_middle_bin_LOD_variance',
    'SCA_middle_bin_BER_variance',
    'SCA_extinction',
    'SCA_backscatter',
    'SCA_LOD',
    'SCA_SR',
    'SCA_middle_bin_extinction',
    'SCA_middle_bin_backscatter',
    'SCA_middle_bin_LOD',
    'SCA_middle_bin_BER',
    'ICA_filling_case',
    'ICA_extinction',
    'ICA_backscatter',
    'ICA_LOD',
    'MCA_clim_BER',
    'MCA_extinction',
    'MCA_LOD',
])


class L2AMeasurementDataExtractor(MeasurementDataExtractor):

    observation_locations = OBSERVATION_LOCATIONS
    measurement_locations = MEASUREMENT_LOCATIONS
    group_locations = GROUP_LOCATIONS
    ica_locations = ICA_LOCATIONS
    sca_locations = SCA_LOCATIONS
    mca_locations = MCA_LOCATIONS
    array_fields = ARRAY_FIELDS

    def overlaps(self, cf, next_cf):
        location = MEASUREMENT_LOCATIONS['L1B_time_meas']

        end = cf.fetch_date(
            location[0],
            cf.get_size(location[0])[0] - 1,
            location[2],
            29,
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
        location = MEASUREMENT_LOCATIONS['L1B_time_meas']
        stop_time = next_cf.fetch_date(
            location[0], 0, location[2], 0, *location[4:]
        )

        for field in ['L1B_time_meas', 'L1B_time_obs', 'group_end_time']:

            if field not in filters:
                filters[field] = {'max': stop_time}

            elif 'max' not in filters[field]:
                filters[field]['max'] = stop_time

            else:
                filters[field]['max'] = min(
                    stop_time, filters[field]['max']
                )

        return filters

extractor = L2AMeasurementDataExtractor()

extract_data = extractor.extract_data
