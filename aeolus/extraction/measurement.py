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
from itertools import izip

import numpy as np
import coda

from aeolus.coda_utils import CODAFile, access_location, check_fields
from aeolus.filtering import make_mask, combine_mask


def check_has_groups(cf):
    """ Test whether the codafile has groups
    """
    try:
        return cf.fetch('/group_pcd') is not None
    except coda.CodacError:
        return False


def _array_to_list(data):
    if isinstance(data, np.ndarray):
        isobject = data.dtype == np.object
        data = data.tolist()
        if isobject:
            data = [
                _array_to_list(obj) for obj in data
            ]
    return data


class MeasurementDataExtractor(object):
    def __init__(self, observation_locations, measurement_locations,
                 group_locations, array_fields):
        self.observation_locations = observation_locations
        self.measurement_locations = measurement_locations
        self.group_locations = group_locations or {}
        self.array_fields = array_fields

    def extract_data(self, filenames, filters,
                     observation_fields, measurement_fields, group_fields,
                     simple_observation_filters=False,
                     convert_arrays=False):
        """ Extract the data from the given filename(s) and apply the given
            filters.
        """
        filenames = (
            [filenames] if isinstance(filenames, basestring) else filenames
        )

        check_fields(
            filters.keys(),
            self.observation_locations.keys() +
            self.measurement_locations.keys() +
            self.group_locations.keys(),
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

        for cf in [CODAFile(filename) for filename in filenames]:
            out_observation_data = defaultdict(list)
            out_measurement_data = defaultdict(list)
            out_group_data = defaultdict(list)

            with cf:
                # create a mask for observation data
                observation_mask = None
                for field_name, filter_value in observation_filters.items():
                    location = self.observation_locations[field_name]

                    data = access_location(cf, location)

                    new_mask = make_mask(
                        data, filter_value.get('min'), filter_value.get('max'),
                        field_name in self.array_fields
                    )

                    observation_mask = combine_mask(new_mask, observation_mask)

                if observation_mask is not None:
                    filtered_observation_ids = np.nonzero(observation_mask)
                else:
                    filtered_observation_ids = None

                # fetch the requested observation fields, filter accordingly and
                # write to the output dict
                for field_name in observation_fields:
                    location = self.observation_locations[field_name]

                    data = access_location(cf, location)

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
                    observation_iterator = np.arange(
                        cf.get_size('/geolocation')[0]
                    )

                out_measurement_data.update(
                    self._read_measurements(
                        cf, measurement_fields, measurement_filters,
                        observation_iterator,
                        convert_arrays
                    )
                )

                # check whether groups are available in the product
                if self.array_fields and check_has_groups(cf):
                    # Handle "groups", by building a group mask for all filters
                    # related to groups
                    group_mask = None
                    for field_name, filter_value in group_filters.items():
                        location = self.group_locations[field_name]

                        data = access_location(cf, location)

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

                        data = access_location(location)

                        if filtered_group_ids is not None:
                            data = data[filtered_group_ids]

                        # convert to simple list instead of numpy array if
                        # requested
                        if convert_arrays and isinstance(data, np.ndarray):
                            data = _array_to_list(data)
                        out_group_data[field_name].extend(data)

                yield out_observation_data, out_measurement_data, out_group_data

    def _read_measurements(self, cf, measurement_fields, filters,
                           observation_ids, convert_arrays):
        out_measurement_data = defaultdict(list)

        # return early, when no measurement fields are actually requested
        if not measurement_fields:
            return out_measurement_data

        # Build a measurement mask
        measurement_mask = None
        for field_name, filter_value in filters.items():
            # only apply filters for measurement fields
            if field_name not in self.measurement_locations:
                continue

            location = self.measurement_locations[field_name]

            data = np.vstack(access_location(cf, location)[observation_ids])

            new_mask = make_mask(
                data, filter_value.get('min'), filter_value.get('max'),
                field_name in self.array_fields
            )
            # combine the masks
            measurement_mask = combine_mask(new_mask, measurement_mask)

        # iterate over all requested fields
        for field_name in measurement_fields:
            location = self.measurement_locations[field_name]

            # fetch the data, and apply the observation id mask
            data = access_location(cf, location)[observation_ids]

            # when a measurement mask was built, iterate over all measurement
            # groups plus their mask respectively, and apply it to get a filtered
            # list
            if measurement_mask is not None:
                tmp_data = [
                    (
                        _array_to_list(measurement[mask])
                        if convert_arrays else measurement[mask]
                    )
                    for measurement, mask in izip(data, measurement_mask)
                ]
                data = np.empty(len(tmp_data), dtype=np.object)
                data[:] = tmp_data

            # convert to simple list instead of numpy array if requested
            if convert_arrays and isinstance(data, np.ndarray):
                data = _array_to_list(data)

            out_measurement_data[field_name].extend(data)

        return out_measurement_data