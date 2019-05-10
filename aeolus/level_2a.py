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

from collections import defaultdict
from itertools import izip

import numpy as np

from aeolus.coda_utils import CODAFile, access_location, check_fields
from aeolus.filtering import make_mask, combine_mask
from aeolus.albedo import sample_offnadir
from aeolus.extraction.measurement import MeasurementDataExtractor


def calculate_L1B_centroid_time_obs(cf):
    """
    """
    t_start = cf.fetch(*OBSERVATION_LOCATIONS['L1B_start_time_obs'])
    t_centroid = np.zeros(t_start.shape, t_start.dtype)
    t_centroid[:-1] = (t_start[:-1] + t_start[1:]) / 2
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

    for i, values in enumerate(izip(measurement_lons, measurement_times)):
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

    for i, values in enumerate(izip(measurement_lats, measurement_times)):
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


OBSERVATION_LOCATIONS = {
    'L1B_start_time_obs':                           ['/geolocation', -1, 'start_of_obs_time'],
    'L1B_centroid_time_obs':                        calculate_L1B_centroid_time_obs,
    'MCA_time_obs':                                 ['/mca_optical_properties', -1, 'starttime'],
    'longitude_of_DEM_intersection_obs':            calculate_longitude_of_DEM_intersection_obs,
    'latitude_of_DEM_intersection_obs':             calculate_latitude_of_DEM_intersection_obs,
    'altitude_of_DEM_intersection_obs':             calculate_altitude_of_DEM_intersection_obs,
    'geoid_separation_obs':                         ['/geolocation', -1, 'geoid_separation'],
    'mie_altitude_obs':                             calculate_mie_altitude_obs,
    'rayleigh_altitude_obs':                        calculate_rayleigh_altitude_obs,
    'L1B_num_of_meas_per_obs':                      ['/geolocation', -1, 'num_meas_eff'],
    'MCA_clim_BER':                                 ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'climber'],
    'MCA_extinction':                               ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'extinction'],
    'MCA_LOD':                                      ['/mca_optical_properties', -1, 'mca_optical_properties', -1, 'lod'],

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
    'SCA_QC_flag':                                  ['/sca_pcd', -1, 'qc_flag'],
    'SCA_extinction_variance':                      ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'extinction_variance'],
    'SCA_backscatter_variance':                     ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'backscatter_variance'],
    'SCA_LOD_variance':                             ['/sca_pcd', -1, 'profile_pcd_bins', -1, 'lod_variance'],
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


# def _array_to_list(data):
#     if isinstance(data, np.ndarray):
#         isobject = data.dtype == np.object
#         data = data.tolist()
#         if isobject:
#             data = [
#                 _array_to_list(obj) for obj in data
#             ]
#     return data


# def check_has_groups(cf):
#     """ Test whether the codafile has groups
#     """
#     return cf.fetch('/group_pcd') is not None


# def extract_data(filenames, filters, observation_fields, measurement_fields,
#                  group_fields, simple_observation_filters=False,
#                  convert_arrays=False):
#     """ Extract the data from the given filename(s) and apply the given filters.
#     """
#     filenames = [filenames] if isinstance(filenames, basestring) else filenames

#     out_observation_data = defaultdict(list)
#     out_measurement_data = defaultdict(list)
#     out_group_data = defaultdict(list)

#     check_fields(
#         filters.keys(),
#         OBSERVATION_LOCATIONS.keys() +
#         MEASUREMENT_LOCATIONS.keys() +
#         GROUP_LOCATIONS.keys(),
#         'filter'
#     )
#     check_fields(observation_fields, OBSERVATION_LOCATIONS.keys(), 'observation')
#     check_fields(measurement_fields, MEASUREMENT_LOCATIONS.keys(), 'measurement')
#     check_fields(group_fields, GROUP_LOCATIONS.keys(), 'group')

#     observation_filters = {
#         name: value
#         for name, value in filters.items()
#         if name in OBSERVATION_LOCATIONS
#     }

#     measurement_filters = {
#         name: value
#         for name, value in filters.items()
#         if name in MEASUREMENT_LOCATIONS
#     }

#     group_filters = {
#         name: value
#         for name, value in filters.items()
#         if name in GROUP_LOCATIONS
#     }

#     for cf in [CODAFile(filename) for filename in filenames]:
#         with cf:
#             # create a mask for observation data
#             observation_mask = None
#             for field_name, filter_value in observation_filters.items():
#                 location = OBSERVATION_LOCATIONS[field_name]

#                 data = access_location(cf, location)

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
#                 location = OBSERVATION_LOCATIONS[field_name]

#                 data = access_location(cf, location)

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
#                 observation_iterator = xrange(
#                     cf.get_size('/geolocation')[0]
#                 )

#             out_measurement_data.update(
#                 _read_measurements(
#                     cf, measurement_fields, measurement_filters,
#                     observation_iterator,
#                     convert_arrays
#                 )
#             )

#             # check whether groups are available in the product
#             if not check_has_groups(cf):
#                 continue

#             # Handle "groups", by building a group mask for all filters related
#             # to groups
#             group_mask = None
#             for field_name, filter_value in group_filters.items():
#                 location = GROUP_LOCATIONS[field_name]

#                 data = access_location(cf, location)

#                 new_mask = make_mask(
#                     data, filter_value.get('min'), filter_value.get('max'),
#                     field_name in ARRAY_FIELDS
#                 )

#                 group_mask = combine_mask(new_mask, group_mask)

#             if group_mask is not None:
#                 filtered_group_ids = np.nonzero(group_mask)
#             else:
#                 filtered_group_ids = None

#             # fetch the requested observation fields, filter accordingly and
#             # write to the output dict
#             for field_name in group_fields:
#                 if field_name not in GROUP_LOCATIONS:
#                     raise KeyError('Unknown group field %s' % field_name)
#                 location = GROUP_LOCATIONS[field_name]

#                 data = access_location(location)

#                 if filtered_group_ids is not None:
#                     data = data[filtered_group_ids]

#                 # convert to simple list instead of numpy array if requested
#                 if convert_arrays and isinstance(data, np.ndarray):
#                     data = _array_to_list(data)
#                 out_group_data[field_name].extend(data)

#     return out_observation_data, out_measurement_data, out_group_data


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



# # test_file = '/mnt/data/AE_OPER_ALD_U_N_2A_20151001T104454059_005379000_046330_0001/AE_OPER_ALD_U_N_2A_20151001T104454059_005379000_046330_0001.DBL'
# test_file = '/mnt/data/AE_OPER_ALD_U_N_2A_20101002T000000059_000083999_017071_0001.DBL'

# # test_file = '/mnt/data/AE_OPER_ALD_U_N_2A_20101002T000000059_000083999_017071_0001.DBL'

# # test_file = '/mnt/data/AE_OPER_ALD_U_N_2A_20101002T000000059_001188000_017071_0001.DBL'

# def main():

#     print extract_data(
#         test_file,
#         filters={
#             # 'L1B_centroid_time_obs': {
#             #     'min': 497015823.059997
#             #     # 'max':  0.3
#             # }
#         },
#         observation_fields=[
#             # 'rayleigh_altitude_obs'
#         ],
#         measurement_fields=[
#             # 'longitude_of_DEM_intersection_meas'
#         ],
#         group_fields=[
#             # 'group_LOD_variance'
#             'group_centroid_time'
#         ],
#     )

# if __name__ == '__main__':
#     main()
