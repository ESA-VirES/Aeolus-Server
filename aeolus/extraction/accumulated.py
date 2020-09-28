# ------------------------------------------------------------------------------
#
#  Aeolus - Helpers for accumulated data (Level 2B/2C) data extraction
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

from aeolus.coda_utils import CODAFile, access_location, NoSuchFieldException
from aeolus.filtering import make_mask, make_array_mask, combine_mask
from aeolus.extraction import exception


class AccumulatedDataExtractor(object):

    def extract_data(self, filenames, filters,
                     mie_grouping_fields, rayleigh_grouping_fields,
                     mie_profile_fields, rayleigh_profile_fields,
                     mie_wind_fields, rayleigh_wind_fields, measurement_fields,
                     convert_arrays=False):

        # filenames = [
        #     filenames
        # ] if isinstance(filenames, basestring) else filenames
        orig_filters = filters

        files = [
            (
                CODAFile(coda_filename),
                Dataset(netcdf_filename) if netcdf_filename else None

            )
            for (coda_filename, netcdf_filename) in filenames
        ]

        for i, (cf, ds) in enumerate(files):
            mie_grouping_data = defaultdict(list)
            rayleigh_grouping_data = defaultdict(list)
            mie_profile_data = defaultdict(list)
            rayleigh_profile_data = defaultdict(list)
            mie_wind_data = defaultdict(list)
            rayleigh_wind_data = defaultdict(list)
            measurement_data = defaultdict(list)

            next_cf = files[i + 1][0] if (i + 1) < len(files) else None

            # handle the overlap with the next product file by adjusting the
            # data filters.
            if next_cf and self.overlaps(cf, next_cf):
                filters = self.adjust_overlap(
                    cf, next_cf, deepcopy(orig_filters)
                )
            else:
                filters = orig_filters

            mie_grouping_filters = {
                name: value
                for name, value in filters.items()
                if name in self.mie_grouping_fields_defs
            }

            rayleigh_grouping_filters = {
                name: value
                for name, value in filters.items()
                if name in self.rayleigh_grouping_fields_defs
            }

            mie_profile_filters = {
                name: value
                for name, value in filters.items()
                if name in self.mie_profile_fields_defs
            }

            rayleigh_profile_filters = {
                name: value
                for name, value in filters.items()
                if name in self.rayleigh_profile_fields_defs
            }

            mie_wind_filters = {
                name: value
                for name, value in filters.items()
                if name in self.mie_wind_fields_defs
            }

            rayleigh_wind_filters = {
                name: value
                for name, value in filters.items()
                if name in self.rayleigh_wind_fields_defs
            }

            measurement_filters = {
                name: value
                for name, value in filters.items()
                if name in self.measurement_fields_defs
            }

            with cf:
                if mie_grouping_fields:
                    mie_grouping_mask, mie_grouping_array_mask = \
                        self._create_type_masks(
                            cf, ds, mie_grouping_filters
                        )
                else:
                    mie_grouping_mask, mie_grouping_array_mask = (
                        None, None
                    )

                if mie_profile_fields:
                    mie_profile_mask, mie_profile_array_mask = \
                        self._create_type_masks(
                            cf, ds, mie_profile_filters
                        )
                else:
                    mie_profile_mask, mie_profile_array_mask = (
                        None, None
                    )

                if mie_wind_fields:
                    mie_wind_mask, mie_wind_array_mask = \
                        self._create_type_masks(
                            cf, ds, mie_wind_filters
                        )
                else:
                    mie_wind_mask, mie_wind_array_mask = (
                        None, None
                    )

                if rayleigh_grouping_fields:
                    rayleigh_grouping_mask, rayleigh_grouping_array_mask = \
                        self._create_type_masks(
                            cf, ds, rayleigh_grouping_filters
                        )
                else:
                    rayleigh_grouping_mask, rayleigh_grouping_array_mask = (
                        None, None
                    )

                if rayleigh_profile_fields:
                    rayleigh_profile_mask, rayleigh_profile_array_mask = \
                        self._create_type_masks(
                            cf, ds, rayleigh_profile_filters
                        )
                else:
                    rayleigh_profile_mask, rayleigh_profile_array_mask = (
                        None, None
                    )

                if rayleigh_wind_fields:
                    rayleigh_wind_mask, rayleigh_wind_array_mask = \
                        self._create_type_masks(
                            cf, ds, rayleigh_wind_filters
                        )
                else:
                    rayleigh_wind_mask, rayleigh_wind_array_mask = (
                        None, None
                    )

                if measurement_fields:
                    measurement_mask, measurement_array_mask = \
                        self._create_type_masks(
                            cf, ds, measurement_filters
                        )
                else:
                    measurement_mask, measurement_array_mask = (
                        None, None
                    )

                mask_cache = {}

                # mie profile to mie wind mask
                if mie_profile_mask is not None:
                    mie_wind_mask = self._join_mask(
                        cf, ds,
                        'mie_wind_profile_wind_result_id',
                        '/sph/NumMieWindResults',
                        mie_profile_mask,
                        mie_wind_mask,
                        mask_cache
                    )

                # measurement to mie wind mask
                if measurement_mask is not None:
                    mie_wind_mask = self._join_mask(
                        cf, ds,
                        'mie_measurement_map',
                        '/sph/NumMieWindResults',
                        measurement_mask,
                        mie_wind_mask,
                        mask_cache
                    )

                # rayleigh profile to rayleigh wind mask
                if rayleigh_profile_mask is not None:
                    rayleigh_wind_mask = self._join_mask(
                        cf, ds,
                        'rayleigh_wind_profile_wind_result_id',
                        '/sph/NumRayleighWindResults',
                        rayleigh_profile_mask,
                        rayleigh_wind_mask,
                        mask_cache
                    )

                # measurement to rayleigh wind mask
                if measurement_mask is not None:
                    rayleigh_wind_mask = self._join_mask(
                        cf, ds,
                        'rayleigh_measurement_map',
                        '/sph/NumRayleighWindResults',
                        measurement_mask,
                        rayleigh_wind_mask,
                        mask_cache
                    )

                output_items = [(
                    mie_grouping_fields,
                    mie_grouping_data,
                    mie_grouping_mask,
                    mie_grouping_array_mask
                ), (
                    mie_profile_fields,
                    mie_profile_data,
                    mie_profile_mask,
                    mie_profile_array_mask
                ), (
                    mie_wind_fields,
                    mie_wind_data,
                    mie_wind_mask,
                    mie_wind_array_mask
                ), (
                    rayleigh_grouping_fields,
                    rayleigh_grouping_data,
                    rayleigh_grouping_mask,
                    rayleigh_grouping_array_mask
                ), (
                    rayleigh_profile_fields,
                    rayleigh_profile_data,
                    rayleigh_profile_mask,
                    rayleigh_profile_array_mask
                ), (
                    rayleigh_wind_fields,
                    rayleigh_wind_data,
                    rayleigh_wind_mask,
                    rayleigh_wind_array_mask
                ), (
                    measurement_fields,
                    measurement_data,
                    measurement_mask,
                    measurement_array_mask
                )]

                for (fields, data, mask, array_mask) in output_items:
                    if array_mask is not None and mask is not None:
                        array_mask = np.logical_not(array_mask[mask.nonzero()])

                    self._make_outputs(
                        cf, ds, fields, data, mask, array_mask,
                    )

            yield (
                mie_grouping_data,
                rayleigh_grouping_data,
                mie_profile_data,
                rayleigh_profile_data,
                mie_wind_data,
                rayleigh_wind_data,
                measurement_data,
            )

    def _fetch_array(self, cf, ds, name, cache=None):
        if cache and name in cache:
            return cache[name]

        data = None
        if ds and ds.groups.get('DATA'):
            group = ds.groups.get('DATA')
            if group and name in group.variables:
                data = group.variables[name][:]

        if data is None:
            try:
                path = self.locations[name]
                data = access_location(cf, path)
            except NoSuchFieldException:
                raise exception.InvalidFieldError(name, path)

            if name in self.array_fields:
                data = np.vstack(data)

        if cache:
            cache[name] = data

        return data

    def _create_type_masks(self, cf, ds, filters):
        mask = None
        array_mask = None
        for field, filter_value in filters.items():
            is_array = field in self.array_fields
            data = self._fetch_array(cf, ds, field)
            new_mask = make_mask(
                data=data,
                # is_array=is_array,
                **filter_value
            )
            mask = combine_mask(new_mask, mask)

            if is_array:
                new_array_mask = make_array_mask(data, **filter_value)
                array_mask = combine_mask(new_array_mask, array_mask)

        return mask, array_mask

    def _join_mask(self, cf, ds, mapping_field, length_field, related_mask,
                   joined_mask, mask_cache):
        ids = self._fetch_array(cf, ds, mapping_field, mask_cache)
        new_mask = np.zeros((cf.fetch(length_field),), np.bool)

        filtered = ids[np.nonzero(related_mask)]

        if filtered.shape[0] > 0:
            stacked = np.hstack(filtered)
            new_mask[stacked[stacked != 0] - 1] = True
            new_mask = combine_mask(new_mask, joined_mask)

        return new_mask

    def _make_outputs(self, cf, ds, fields, output, mask, array_mask):
        ids = np.nonzero(mask) if mask is not None else None

        for field in fields:
            data = self._fetch_array(cf, ds, field)
            if mask is not None:
                data = data[ids]

            if field in self.array_fields:
                data = np.ma.MaskedArray(data, array_mask)

            output[field] = data

    def _array_to_list(self, data):
        if isinstance(data, np.ndarray):
            isobject = data.dtype == np.object
            data = data.tolist()
            if isobject:
                data = [
                    self._array_to_list(obj) for obj in data
                ]
        return data

    def overlaps(self, cf, next_cf):
        end_time = cf.fetch_date('mph/sensing_stop')
        begin_time = next_cf.fetch_date('mph/sensing_start')
        return end_time > begin_time

    def adjust_overlap(self, cf, next_cf, filters):
        stop_time = next_cf.fetch_date('mph/sensing_start')

        for field in self.overlap_fields:

            if field not in filters:
                filters[field] = {'max': stop_time}

            elif 'max' not in filters[field]:
                filters[field]['max'] = stop_time

            else:
                filters[field]['max'] = min(
                    stop_time, filters[field]['max']
                )

        return filters
