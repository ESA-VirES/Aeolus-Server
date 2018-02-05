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
    'L1B_start_time_obs':                           ['/geolocation', -1, 'start_of_obs_time'],
    # 'L1B_centroid_time_obs':                      [''],
    'L1B_time_meas':                                ['/geolocation', -1, 'measurement_geolocation', -1, 'centroid_time'],
    'SCA_time_obs':                                 ['/sca_optical_properties', -1, 'starttime'],
    'ICA_time_obs':                                 ['/ica_optical_properties', -1, 'starttime'],
    'MCA_time_obs':                                 ['/mca_optical_properties', -1, 'starttime'],
    'group_start_time':                             ['/group_pcd', -1, 'starttime'],
    # 'group_end_time':                             [''],
    # 'group_centroid_time':                        [''],
    'longitude_of_DEM_intersection_meas':           ['/geolocation', -1, 'measurement_geolocation', -1, 'longitude_of_dem_intersection'],
    # 'longitude_of_DEM_intersection_obs':          [''],
    'latitude_of_DEM_intersection_meas':            ['/geolocation', -1, 'measurement_geolocation', -1, 'latgitude_of_dem_intersection'],
    # 'latitude_of_DEM_intersection_obs':           [''],
    'altitude_of_DEM_intersection_meas':            ['/geolocation', -1, 'measurement_geolocation', -1, 'altitude_of_dem_intersection'],
    # 'altitude_of_DEM_intersection_obs':           [''],
    'geoid_separation_obs':                         ['/geolocation', -1, 'geoid_separation'],
    'mie_altitude_meas':                            ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation_height_bin', -1, '/altitude_of_height_bin'],
    # 'mie_altitude_obs':                           [''],
    'rayleigh_altitude_meas':                       ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation_height_bin', -1, '/altitude_of_height_bin'],
    # 'rayleigh_altitude_obs':                      [''],
    'SCA_middle_bin_altitude_obs':                  ['/sca_optical_properties', -1, 'geolocation_middle_bins', -1, 'altitude'],
    'group_middle_bin_start_altitude':              ['/group_optical_properties', -1, 'group_geolocation_middle_bins/start_altitude'],
    'group_middle_bin_stop_altitude':               ['/group_optical_properties', -1, 'group_geolocation_middle_bins/stop_altitude'],
    'group_start_obs':                              ['/group_pcd', -1, 'brc_start'],
    'group_start_meas_obs':                         ['/group_pcd', -1, 'measurement_start'],
    'group_end_obs':                                ['/group_pcd', -1, 'brc_end'],
    'group_end_meas_obs':                           ['/group_pcd', -1, 'measurement_end'],
    'group_height_bin_index':                       ['/group_pcd', -1, 'height_bin_index'],
    'L1B_num_of_meas_per_obs':                      ['/geolocation', -1, 'num_meas_eff'],

    'SCA_QC_flag':                                  ['/sca_pcd', -1, 'qc_flag'],
    'SCA_extinction_variance':                      ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'extinction_variance'],
    'SCA_backscatter_variance':                     ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'backscatter_variance'],
    'SCA_LOD_variance':                             ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'lod_variance'],
    'SCA_middle_bin_extinction_variance':           ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'extinction_variance'],
    'SCA_middle_bin_backscatter_variance':          ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'backscatter_variance'],
    'SCA_middle_bin_LOD_variance':                  ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'lod_variance'],
    'SCA_middle_bin_BER_variance':                  ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'ber_variance'],
    'ICA_QC_flag':                                  ['/ica_pcd', -1, 'qc_flag'],
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
    'SCA_extinction':                               ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'extinction'],
    'SCA_backscatter':                              ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'backscatter'],
    'SCA_LOD':                                      ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'lod'],
    'SCA_SR':                                       ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'sr'],
    'SCA_middle_bin_extinction':                    ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'extinction'],
    'SCA_middle_bin_backscatter':                   ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'backscatter'],
    'SCA_middle_bin_LOD':                           ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'lod'],
    'SCA_middle_bin_BER':                           ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'ber'],
    'ICA_filling_case':                             ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'case'],
    'ICA_extinction':                               ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'extinction'],
    'ICA_backscatter':                              ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'backscatter'],
    'ICA_LOD':                                      ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'lod'],
    'MCA_clim_BER':                                 ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'climber'],
    'MCA_extinction':                               ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'extinction'],
    'MCA_LOD':                                      ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'lod'],
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


OBSERVATION_LOCATIONS = {
    'L1B_start_time_obs':                           ['/geolocation', -1, 'start_of_obs_time'],
    # 'L1B_centroid_time_obs':                      [''],
    'L1B_time_meas':                                ['/geolocation', -1, 'measurement_geolocation', -1, 'centroid_time'],
    'SCA_time_obs':                                 ['/sca_optical_properties', -1, 'starttime'],
    'ICA_time_obs':                                 ['/ica_optical_properties', -1, 'starttime'],
    'MCA_time_obs':                                 ['/mca_optical_properties', -1, 'starttime'],
    # 'longitude_of_DEM_intersection_obs':          [''],
    # 'latitude_of_DEM_intersection_obs':           [''],
    # 'altitude_of_DEM_intersection_obs':           [''],
    'geoid_separation_obs':                         ['/geolocation', -1, 'geoid_separation'],
    # 'mie_altitude_obs':                           [''],
    # 'rayleigh_altitude_obs':                      [''],
    'SCA_middle_bin_altitude_obs':                  ['/sca_optical_properties', -1, 'geolocation_middle_bins', -1, 'altitude'],
    'L1B_num_of_meas_per_obs':                      ['/geolocation', -1, 'num_meas_eff'],
    'SCA_QC_flag':                                  ['/sca_pcd', -1, 'qc_flag'],
    'SCA_extinction_variance':                      ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'extinction_variance'],
    'SCA_backscatter_variance':                     ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'backscatter_variance'],
    'SCA_LOD_variance':                             ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'lod_variance'],
    'SCA_middle_bin_extinction_variance':           ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'extinction_variance'],
    'SCA_middle_bin_backscatter_variance':          ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'backscatter_variance'],
    'SCA_middle_bin_LOD_variance':                  ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'lod_variance'],
    'SCA_middle_bin_BER_variance':                  ['/sca_pcd', -1, 'profile_pcd_mid_bins', -1, 'ber_variance'],
    'ICA_QC_flag':                                  ['/ica_pcd', -1, 'qc_flag'],
    'SCA_extinction':                               ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'extinction'],
    'SCA_backscatter':                              ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'backscatter'],
    'SCA_LOD':                                      ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'lod'],
    'SCA_SR':                                       ['/sca_optical_properties', -1, 'sca_optical_properties', -1, 'sr'],
    'SCA_middle_bin_extinction':                    ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'extinction'],
    'SCA_middle_bin_backscatter':                   ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'backscatter'],
    'SCA_middle_bin_LOD':                           ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'lod'],
    'SCA_middle_bin_BER':                           ['/sca_optical_properties', -1, 'sca_optical_properties_mid_bins', -1, 'ber'],
    'ICA_filling_case':                             ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'case'],
    'ICA_extinction':                               ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'extinction'],
    'ICA_backscatter':                              ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'backscatter'],
    'ICA_LOD':                                      ['/ica_optical_properties', -1, 'ica_optical_properties', -1, 'lod'],
    'MCA_clim_BER':                                 ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'climber'],
    'MCA_extinction':                               ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'extinction'],
    'MCA_LOD':                                      ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'lod'],
}

MEASUREMENT_LOCATIONS = {
    'longitude_of_DEM_intersection_meas':           ['/geolocation', -1, 'measurement_geolocation', -1, 'longitude_of_dem_intersection'],
    'latitude_of_DEM_intersection_meas':            ['/geolocation', -1, 'measurement_geolocation', -1, 'latgitude_of_dem_intersection'],
    'altitude_of_DEM_intersection_meas':            ['/geolocation', -1, 'measurement_geolocation', -1, 'altitude_of_dem_intersection'],
    'mie_altitude_meas':                            ['/geolocation', -1, 'measurement_geolocation', -1, 'mie_geolocation_height_bin', -1, '/altitude_of_height_bin'],
    'rayleigh_altitude_meas':                       ['/geolocation', -1, 'measurement_geolocation', -1, 'rayleigh_geolocation_height_bin', -1, '/altitude_of_height_bin'],
}

GROUP_LOCATIONS = {
    'group_start_time':                             ['/group_pcd', -1, 'starttime'],
    # 'group_end_time':                             [''],
    # 'group_centroid_time':                        [''],
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

ARRAY_FIELDS = set([
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


def _array_to_list(data):
    if isinstance(data, np.ndarray):
        isobject = data.dtype == np.object
        data = data.tolist()
        if isobject:
            data = [
                _array_to_list(obj) for obj in data
            ]
    return data


def extract_data(filenames, filters, observation_fields, measurement_fields,
                 group_fields, simple_observation_filters=False,
                 convert_arrays=False):
    """ Extract the data from the given filename(s) and apply the given filters.
    """
    filenames = [filenames] if isinstance(filenames, basestring) else filenames

    out_observation_data = defaultdict(list)
    out_measurement_data = defaultdict(list)
    out_group_data = defaultdict(list)

    for field_name, filter_value in filters.items():
        assert (
            field_name in OBSERVATION_LOCATIONS or
            field_name in MEASUREMENT_LOCATIONS or
            field_name in GROUP_LOCATIONS
        )

    observation_filters = {
        name: value
        for name, value in filters.items()
        if name in OBSERVATION_LOCATIONS
    }

    measurement_filters = {
        name: value
        for name, value in filters.items()
        if name in MEASUREMENT_LOCATIONS
    }

    group_filters = {
        name: value
        for name, value in filters.items()
        if name in GROUP_LOCATIONS
    }

    for cf in [CODAFile(filename) for filename in filenames]:
        with cf:
            # create a mask for observation data
            observation_mask = None
            for field_name, filter_value in observation_filters.items():
                data = cf.fetch(*OBSERVATION_LOCATIONS[field_name])
                new_mask = make_mask(
                    data, filter_value.get('min'), filter_value.get('max'),
                    field_name in ARRAY_FIELDS
                )

                observation_mask = combine_mask(new_mask, observation_mask)

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
                    data = _array_to_list(data)
                out_observation_data[field_name].extend(data)

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

            out_measurement_data.update(
                _read_measurements(
                    cf, measurement_fields, measurement_filters,
                    observation_iterator,
                    convert_arrays
                )
            )

            # Handle "groups", by building a group mask for all filters related
            # to groups
            group_mask = None
            for field_name, filter_value in group_filters.items():
                data = cf.fetch(*GROUP_LOCATIONS[field_name])
                new_mask = make_mask(
                    data, filter_value.get('min'), filter_value.get('max'),
                    field_name in ARRAY_FIELDS
                )

                group_mask = combine_mask(new_mask, group_mask)

            if group_mask is not None:
                filtered_group_ids = np.nonzero(group_mask)
            else:
                filtered_group_ids = None

            # fetch the requested observation fields, filter accordingly and
            # write to the output dict
            for field_name in group_fields:
                assert field_name in GROUP_LOCATIONS
                data = cf.fetch(*GROUP_LOCATIONS[field_name])
                if filtered_group_ids is not None:
                    data = data[filtered_group_ids]

                # convert to simple list instead of numpy array if requested
                if convert_arrays and isinstance(data, np.ndarray):
                    data = _array_to_list(data)
                out_group_data[field_name].extend(data)

    return out_observation_data, out_measurement_data, out_group_data


def _read_measurements(cf, measurement_fields, filters, observation_ids,
                       convert_arrays):

    out_measurement_data = defaultdict(list)

    # iterate all (or selected) observations
    for observation_id in observation_ids:
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

            new_mask = make_mask(
                data, filter_value.get('min'), filter_value.get('max'),
                field_name in ARRAY_FIELDS
            )

            # combine the masks
            measurement_mask = combine_mask(new_mask, measurement_mask)

        filtered_measurement_ids = None
        if measurement_mask is not None:
            filtered_measurement_ids = np.nonzero(measurement_mask)
            if measurement_mask.shape == filtered_measurement_ids[0].shape:
                filtered_measurement_ids = None

        for field_name in measurement_fields:
            path = MEASUREMENT_LOCATIONS[field_name]
            data = cf.fetch(path[0], int(observation_id), *path[2:])

            if filtered_measurement_ids:
                data = data[filtered_measurement_ids]
            # convert to simple list instead of numpy array if requested
            if convert_arrays and isinstance(data, np.ndarray):
                data = _array_to_list(data)

            out_measurement_data[field_name].append(data)

    return out_measurement_data



# test_file = '/mnt/data/AE_OPER_ALD_U_N_2A_20151001T104454059_005379000_046330_0001/AE_OPER_ALD_U_N_2A_20151001T104454059_005379000_046330_0001.DBL'


# test_file = '/mnt/data/AE_OPER_ALD_U_N_2A_20101002T000000059_000083999_017071_0001.DBL'

test_file = '/mnt/data/AE_OPER_ALD_U_N_2A_20101002T000000059_001188000_017071_0001.DBL'

def main():
    #print
    extract_data(
        test_file,
        filters={
            'group_LOD_variance': {
                'min': 0.0,
                'max':  0.3
            }
        },
        observation_fields=[
            # 'L1B_start_time_obs'
        ],
        measurement_fields=[
            # 'longitude_of_DEM_intersection_meas'
        ],
        group_fields=[
            'group_LOD_variance'
        ],
    )

if __name__ == '__main__':
    main()
