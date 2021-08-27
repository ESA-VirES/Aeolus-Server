# ------------------------------------------------------------------------------
#
#  Aeolus - Helpers for measurement (Level 1B/2A) data extraction
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
from copy import deepcopy

import numpy as np
from netCDF4 import Dataset

from aeolus.coda_utils import (
    CODAFile, access_location, check_fields, NoSuchFieldException
)
from aeolus.filtering import make_mask, make_array_mask, combine_mask
from aeolus.extraction import exception


def check_has_groups(cf):
    """ Test whether the codafile has groups
    """
    try:
        return cf.fetch('/group_pcd') is not None
    except NoSuchFieldException:
        return False


class MeasurementDataExtractor(object):
    # def __init__(self, observation_locations, measurement_locations,
    #              group_locations, array_fields):
    #     self.observation_locations = observation_locations
    #     self.measurement_locations = measurement_locations
    #     self.group_locations = group_locations or {}
    #     self.array_fields = array_fields

    def extract_data(self, filenames, filters,
                     observation_fields, measurement_fields, group_fields,
                     ica_fields, sca_fields, mca_fields,
                     simple_observation_filters=False):
        """ Extract the data from the given filename(s) and apply the given
            filters.
        """

        check_fields(
            list(filters.keys()),
            list(self.observation_locations.keys()) +
            list(self.measurement_locations.keys()) +
            list(self.group_locations.keys()) +
            list(self.ica_locations.keys()) +
            list(self.sca_locations.keys()) +
            list(self.mca_locations.keys()),
            'filter'
        )
        check_fields(
            observation_fields, self.observation_locations.keys(), 'observation'
        )
        check_fields(
            measurement_fields, self.measurement_locations.keys(), 'measurement'
        )
        check_fields(
            group_fields, self.group_locations.keys(), 'group'
        )
        check_fields(
            ica_fields, self.ica_locations.keys(), 'ICA'
        )

        orig_filters = filters

        files = [
            (
                CODAFile(coda_filename),
                Dataset(netcdf_filename) if netcdf_filename else None

            )
            for (coda_filename, netcdf_filename) in filenames
        ]

        for i, (cf, ds) in enumerate(files):
            out_observation_data = defaultdict(list)
            out_measurement_data = defaultdict(list)
            out_group_data = defaultdict(list)
            out_ica_data = defaultdict(list)
            out_sca_data = defaultdict(list)
            out_mca_data = defaultdict(list)

            next_cf = files[i + 1][0] if (i + 1) < len(files) else None

            # handle the overlap with the next product file by adjusting the
            # data filters.
            if next_cf and self.overlaps(cf, next_cf):
                filters = self.adjust_overlap(
                    cf, next_cf, deepcopy(orig_filters)
                )
            else:
                filters = orig_filters

            observation_filters = {
                name: value
                for name, value in filters.items()
                if name in self.observation_locations
            }

            measurement_filters = {
                name: value
                for name, value in filters.items()
                if name in self.measurement_locations
            }

            group_filters = {
                name: value
                for name, value in filters.items()
                if name in self.group_locations
            }

            ica_filters = {
                name: value
                for name, value in filters.items()
                if name in self.ica_locations
            }

            sca_filters = {
                name: value
                for name, value in filters.items()
                if name in self.sca_locations
            }

            mca_filters = {
                name: value
                for name, value in filters.items()
                if name in self.mca_locations
            }

            with cf:
                # create a mask for observation data
                observation_mask = None
                observation_array_masks = {}

                for field_name, filter_value in observation_filters.items():
                    location = self.observation_locations[field_name]

                    data = optimized_access(
                        cf, ds, 'OBSERVATION_DATA', field_name, location
                    )

                    new_mask = make_mask(
                        data, filter_value.get('min'), filter_value.get('max'),
                        field_name in self.array_fields
                    )

                    observation_mask = combine_mask(new_mask, observation_mask)

                    if field_name in self.array_fields:
                        data = np.vstack(data)
                        size = data.shape[-1]
                        new_array_mask = make_array_mask(
                            data, **filter_value
                        )
                        observation_array_masks[size] = combine_mask(
                            new_array_mask, observation_array_masks.get(size)
                        )

                if observation_mask is not None:
                    filtered_observation_ids = np.nonzero(observation_mask)
                    for size, observation_array_mask in \
                            observation_array_masks.items():
                        observation_array_masks[size] = np.logical_not(
                            observation_array_mask[
                                filtered_observation_ids
                            ]
                        )
                else:
                    filtered_observation_ids = None

                # fetch the requested observation fields, filter accordingly and
                # write to the output dict
                for field_name in observation_fields:
                    location = self.observation_locations[field_name]

                    data = optimized_access(
                        cf, ds, 'OBSERVATION_DATA', field_name, location
                    )
                    if filtered_observation_ids is not None:
                        data = data[filtered_observation_ids]

                    if data.shape[0] and field_name in self.array_fields:
                        data = np.vstack(data)
                        size = data.shape[-1]
                        if size in observation_array_masks:
                            data = np.ma.MaskedArray(
                                data, observation_array_masks[size]
                            )

                    out_observation_data[field_name] = data

                # if we filter the measurements by observation ID, then use the
                # filtered observation IDs as mask for the measurements.
                # otherwise, use the full range of observations
                if filtered_observation_ids is not None and \
                        simple_observation_filters:
                    observation_iterator = filtered_observation_ids[0]
                else:
                    observation_iterator = np.arange(
                        cf.get_size('/geolocation')[0]
                    )

                out_measurement_data.update(
                    self._read_measurements(
                        cf, ds, measurement_fields, measurement_filters,
                        observation_iterator,
                        cf.get_size('/geolocation')[0]
                    )
                )

                # check whether groups are available in the product
                if self.array_fields and check_has_groups(cf):
                    # Handle "groups", by building a group mask for all filters
                    # related to groups
                    group_mask = None
                    for field_name, filter_value in group_filters.items():
                        location = self.group_locations[field_name]

                        data = optimized_access(
                            cf, ds, 'OBSERVATION_DATA', field_name, location
                        )

                        new_mask = make_mask(
                            data, filter_value.get('min'),
                            filter_value.get('max'),
                            field_name in self.array_fields
                        )

                        group_mask = combine_mask(new_mask, group_mask)

                    if group_mask is not None:
                        filtered_group_ids = np.nonzero(group_mask)
                    else:
                        filtered_group_ids = None

                    # fetch the requested observation fields, filter accordingly
                    # and write to the output dict
                    for field_name in group_fields:
                        if field_name not in self.group_locations:
                            raise KeyError('Unknown group field %s' % field_name)
                        location = self.group_locations[field_name]

                        data = optimized_access(
                            cf, ds, 'GROUP_DATA', field_name, location
                        )

                        if filtered_group_ids is not None:
                            data = data[filtered_group_ids]

                        out_group_data[field_name].extend(data)

                # handle ICA filters/fields
                ica_mask = None
                ica_array_mask = None
                for field_name, filter_value in ica_filters.items():
                    location = self.ica_locations[field_name]

                    data = optimized_access(
                        cf, ds, 'ICA_DATA', field_name, location
                    )

                    new_mask = make_mask(
                        data, filter_value.get('min'), filter_value.get('max'),
                        field_name in self.array_fields
                    )

                    ica_mask = combine_mask(new_mask, ica_mask)

                    if field_name in self.array_fields:
                        data = np.vstack(data)
                        new_array_mask = make_array_mask(
                            data, **filter_value
                        )
                        ica_array_mask = combine_mask(
                            new_array_mask, ica_array_mask
                        )

                if ica_mask is not None:
                    filtered_ica_ids = np.nonzero(ica_mask)
                    if ica_array_mask is not None:
                        ica_array_mask = ica_array_mask[
                            filtered_ica_ids
                        ]
                else:
                    filtered_ica_ids = None

                if ica_array_mask is not None:
                    # for np.ma.MaskedArrays we need True/False the other way
                    # around
                    ica_array_mask = np.logical_not(
                        ica_array_mask
                    )

                # fetch the requested ICA fields, filter accordingly and
                # write to the output dict
                for field_name in ica_fields:
                    location = self.ica_locations[field_name]

                    data = optimized_access(
                        cf, ds, 'ICA_DATA', field_name, location
                    )

                    if filtered_ica_ids is not None:
                        data = data[filtered_ica_ids]

                    if data.shape[0] and field_name in self.array_fields:
                        data = np.vstack(data)
                        data = np.ma.MaskedArray(data, ica_array_mask)

                    out_ica_data[field_name] = data

                # handle SCA filters/fields
                sca_mask = None
                sca_array_mask = None
                for field_name, filter_value in sca_filters.items():
                    location = self.sca_locations[field_name]

                    data = optimized_access(
                        cf, ds, 'SCA_DATA', field_name, location
                    )

                    new_mask = make_mask(
                        data, filter_value.get('min'), filter_value.get('max'),
                        field_name in self.array_fields
                    )

                    sca_mask = combine_mask(new_mask, sca_mask)

                    if field_name in self.array_fields:
                        data = np.vstack(data)
                        new_array_mask = make_array_mask(
                            data, **filter_value
                        )
                        sca_array_mask = combine_mask(
                            new_array_mask, sca_array_mask
                        )

                if sca_mask is not None:
                    filtered_sca_ids = np.nonzero(sca_mask)
                    if sca_array_mask is not None:
                        sca_array_mask = sca_array_mask[
                            filtered_sca_ids
                        ]
                else:
                    filtered_sca_ids = None

                if sca_array_mask is not None:
                    # for np.ma.MaskedArrays we need True/False the other way
                    # around
                    sca_array_mask = np.logical_not(
                        sca_array_mask
                    )

                # fetch the requested SCA fields, filter accordingly and
                # write to the output dict
                for field_name in sca_fields:
                    location = self.sca_locations[field_name]

                    data = optimized_access(
                        cf, ds, 'SCA_DATA', field_name, location
                    )

                    if filtered_sca_ids is not None:
                        data = data[filtered_sca_ids]

                    if data.shape[0] and field_name in self.array_fields:
                        data = np.vstack(data)
                        data = np.ma.MaskedArray(data, sca_array_mask)

                    out_sca_data[field_name] = data

                # handle MCA filters/fields
                mca_mask = None
                mca_array_mask = None
                for field_name, filter_value in mca_filters.items():
                    location = self.mca_locations[field_name]

                    data = optimized_access(
                        cf, ds, 'MCA_DATA', field_name, location
                    )

                    new_mask = make_mask(
                        data, filter_value.get('min'), filter_value.get('max'),
                        field_name in self.array_fields
                    )

                    mca_mask = combine_mask(new_mask, mca_mask)

                    if field_name in self.array_fields:
                        data = np.vstack(data)
                        new_array_mask = make_array_mask(
                            data, **filter_value
                        )
                        mca_array_mask = combine_mask(
                            new_array_mask, mca_array_mask
                        )

                if mca_mask is not None:
                    filtered_mca_ids = np.nonzero(mca_mask)
                    if mca_array_mask is not None:
                        mca_array_mask = mca_array_mask[
                            filtered_mca_ids
                        ]
                else:
                    filtered_mca_ids = None

                if mca_array_mask is not None:
                    # for np.ma.MaskedArrays we need True/False the other way
                    # around
                    mca_array_mask = np.logical_not(
                        mca_array_mask
                    )

                # fetch the requested mca fields, filter accordingly and
                # write to the output dict
                for field_name in mca_fields:
                    location = self.mca_locations[field_name]

                    data = optimized_access(
                        cf, ds, 'MCA_DATA', field_name, location
                    )

                    if filtered_mca_ids is not None:
                        data = data[filtered_mca_ids]

                    if data.shape[0] and field_name in self.array_fields:
                        data = np.vstack(data)
                        data = np.ma.MaskedArray(data, mca_array_mask)

                    out_mca_data[field_name] = data

                yield (
                    out_observation_data, out_measurement_data,
                    out_group_data, out_ica_data, out_sca_data, out_mca_data
                )

    def _read_measurements(self, cf, ds, measurement_fields, filters,
                           observation_ids, total_observations):

        out_measurement_data = defaultdict(list)

        # return early, when no measurement fields are actually requested
        if not measurement_fields:
            return out_measurement_data

        if not len(observation_ids):
            for field in measurement_fields:
                out_measurement_data[field] = None
            return out_measurement_data

        # Build a measurement mask
        measurement_mask = None
        measurement_array_masks = {}
        for field_name, filter_value in filters.items():
            # only apply filters for measurement fields
            if field_name not in self.measurement_locations:
                continue

            location = self.measurement_locations[field_name]

            data = access_measurements(
                cf, ds, field_name, location, observation_ids,
                total_observations, field_name in self.array_fields
            )

            new_mask = make_mask(
                data, filter_value.get('min'), filter_value.get('max'),
                field_name in self.array_fields
            )

            # combine the masks
            measurement_mask = combine_mask(new_mask, measurement_mask)

            if field_name in self.array_fields:
                size = data.shape[-1]
                new_array_mask = make_array_mask(
                    data, **filter_value
                )
                measurement_array_masks[size] = combine_mask(
                    new_array_mask, measurement_array_masks.get(size)
                )

        if measurement_mask is not None:
            measurement_ids = np.nonzero(measurement_mask)
            for size, measurement_array_mask in measurement_array_masks.items():
                measurement_array_masks[size] = np.logical_not(
                    measurement_array_mask[measurement_ids]
                )
        else:
            measurement_ids = None

        # iterate over all requested fields
        for field_name in measurement_fields:
            location = self.measurement_locations[field_name]

            # fetch the data, and apply the observation id mask
            data = access_measurements(
                cf, ds, field_name, location, observation_ids,
                total_observations, field_name in self.array_fields
            )

            if measurement_ids is not None:
                data = data[measurement_ids]

            if field_name in self.array_fields:
                try:
                    size = data.shape[-1]
                    data = np.ma.MaskedArray(data, measurement_array_masks[size])
                except KeyError:
                    pass

            out_measurement_data[field_name] = data

        return out_measurement_data

    def overlaps(self, cf, next_cf):
        raise NotImplementedError

    def adjust_overlap(self, cf, next_cf, filters):
        raise NotImplementedError


def access_measurements(cf, ds, field_name, location, observation_ids,
                        total_observations, is_array):
    """ "Smart" measurement function, using one of two methods to read from a
        DBL file: either in one big sweep or in many smaller. When only a
        portion (up to 90% of the total measurements) is required, then the
        reading is done in many small portions (one for each observation)
    """

    if ds:
        group_name = "MEASUREMENT_DATA"
        group = ds.groups.get(group_name)
        if group:
            variable = group.variables.get(field_name)
            if variable:
                return variable[observation_ids]

    used_sized = float(observation_ids.shape[0]) / float(total_observations)

    # use many "single reads" when only < 90% of measurements are read
    if used_sized < 0.9 and not callable(location):
        data = [
            access_location(cf, location[:1] + [int(i)] + location[2:])
            for i in observation_ids
        ]
        data = np.vstack(data)

    else:
        data = access_location(cf, location)[observation_ids]

    if is_array:
        return stack_measurement_array(data)
    else:
        data = np.vstack(data)

    return data


def optimized_access(cf, ds, group_name, field_name, location):
    if ds:
        group = ds.groups.get(group_name)
        if group:
            variable = group.variables.get(field_name)
            if variable:
                return variable[:]

    try:
        return access_location(cf, location)
    except:
        raise exception.InvalidFieldError(field_name, location)


def stack_measurement_array(data):
    return np.rollaxis(np.dstack([
        np.vstack(d) for d in data]), 2
    )
