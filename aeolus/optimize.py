# ------------------------------------------------------------------------------
#
#  Create optimized files for products
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

import os.path
import logging

from netCDF4 import Dataset
import numpy as np

from aeolus.coda_utils import CODAFile, access_location, NoSuchFieldException
from aeolus import level_1b
from aeolus import level_2a
from aeolus import level_2b
from aeolus import level_2c
from aeolus import aux
from aeolus import aux_met


logger = logging.getLogger(__name__)

# range-type -> CODA file locations
LOCATIONS = {
    'ALD_U_N_1B': {
        'OBSERVATION_DATA': level_1b.OBSERVATION_LOCATIONS,
        'MEASUREMENT_DATA': level_1b.MEASUREMENT_LOCATIONS,
    },
    'ALD_U_N_2A': {
        'OBSERVATION_DATA': level_2a.OBSERVATION_LOCATIONS,
        'MEASUREMENT_DATA': level_2a.MEASUREMENT_LOCATIONS,
    },
    'ALD_U_N_2B': {
        'DATA': level_2b.locations,
    },
    'ALD_U_N_2C': {
        'DATA': level_2c.locations,
    },
    'AUX_ISR_1B': {
        'DATA': aux.AUX_ISR_LOCATIONS,
    },
    'AUX_MRC_1B': {
        'DATA': aux.AUX_MRC_LOCATIONS,
    },
    'AUX_RRC_1B': {
        'DATA': aux.AUX_RRC_LOCATIONS,
    },
    'AUX_ZWC_1B': {
        'DATA': aux.AUX_ZWC_LOCATIONS,
    },
    'AUX_MET_12': {
        'DATA': aux_met.LOCATIONS,
    }
}


class OptimizationError(Exception):
    pass


def create_optimized_file(input_file, product_type_name, output_path, update,
                          fields=None):
    """ Creates an optimized netcdf file for the given product
    """

    # get the CODA locations for later access

    try:
        location_groups = LOCATIONS[product_type_name]
    except KeyError:
        raise OptimizationError(
            "Product range type '%s' is not supported" % product_type_name
        )

    # check that fields actually exist for that product
    if fields is not None:
        for field in fields:
            for locations in location_groups.values():
                if field in locations:
                    break
            else:
                raise OptimizationError("Unknown field '%s'" % field)

    # select correct file mode
    mode = "w"
    if os.path.exists(output_path):
        if update:
            # 'a' only works when a file exists
            mode = "a"
        else:
            raise OptimizationError(
                "Output path '%s' already exists" % output_path
            )

    # loop through all locations, access them and save them to the netcdf

    try:
        logger.info(
            "Starting optimization for file '%s' to generate '%s'"
            % (input_file, output_path)
        )
        with Dataset(output_path, mode, format="NETCDF4") as out_ds:
            with CODAFile(input_file) as in_cf:
                gen = _optimize_fields(
                    product_type_name, location_groups, in_cf, out_ds,
                    update, fields
                )
                for group_name, name in gen:
                    yield (group_name, name)

    except:
        try:
            logger.error(
                "Failed to generate optimized file, deleting '%s'" % output_path
            )
            os.remove(output_path)
        except OSError:
            pass
        raise


def _optimize_fields(product_type_name, location_groups, in_cf, out_ds, update,
                     fields=None):
    for group_name, locations in location_groups.items():
        if group_name in out_ds.groups:
            if update:
                group = out_ds.groups[group_name]
            else:
                raise OptimizationError('Group %s already exists' % group_name)
        else:
            group = out_ds.createGroup(group_name)

        for name, location in locations.items():
            # if we have a dedicated list of fields to optimize, we skip if the
            # current field is not in that list
            if fields is not None and name not in fields:
                continue
            # check of the variable already exists. If mode is `update`, simply
            # skip over existing ones. If not, fail the generation.
            # Otherwise just create the variable normally
            variable = None
            if name in group.variables:
                if update and fields is None:
                    continue
                elif update and fields is not None:
                    variable = group.variables[name]
                else:
                    raise OptimizationError(
                        'Variable %s already exists for group %s'
                        % (name, group_name)
                    )

            logger.info("Optimizing %s/%s" % (group_name, name))
            yield (group_name, name)

            if product_type_name == 'AUX_MET_12' and len(location) > 3:
                first = location[:1] + [0] + location[2:]
                try:
                    first_values = access_location(in_cf, first)
                except NoSuchFieldException:
                    logger.warn('No such field %s' % (name))
                    continue

                if variable is None:
                    shape = (
                        in_cf.get_size(location[0])[0],
                        first_values.shape[0]
                    )

                    # make a list of all dimension names and check if
                    # they, are already available, otherwise create them
                    dimnames = [
                        "arr_%d" % v for v in shape
                    ]
                    for dimname, size in zip(dimnames, shape):
                        if dimname not in out_ds.dimensions:
                            out_ds.createDimension(dimname, size)

                    variable = group.createVariable(name, '%s%i' % (
                        first_values.dtype.kind,
                        first_values.dtype.itemsize
                    ), dimensions=dimnames)

                try:
                    data = access_location(in_cf, location)
                    for i, item in enumerate(data):
                        variable[i] = item
                except NoSuchFieldException:
                    logger.warn('No such field %s' % (name))
                    continue

            else:
                try:
                    values = access_location(in_cf, location)
                except NoSuchFieldException:
                    logger.warn('No such field %s' % (name))
                    continue

                dimensionality = get_dimensionality(values)

                if variable is None:
                    # get the correct dimensionality for the values and
                    # reshape if necessary

                    if dimensionality == 3:
                        init_num = values.shape[0]
                        values = np.vstack(np.hstack(values))
                        values = values.reshape(
                            values.shape[0] // init_num,
                            init_num,
                            values.shape[1]
                        ).swapaxes(0, 1)
                    elif dimensionality == 2:
                        values = np.vstack(values)

                    # make a list of all dimension names and check if
                    # they, are already available, otherwise create them
                    dimnames = [
                        "arr_%d" % v for v in values.shape
                    ]
                    for dimname, size in zip(dimnames, values.shape):
                        if dimname not in out_ds.dimensions:
                            out_ds.createDimension(dimname, size)

                    # create a variable and store the data in it
                    variable = group.createVariable(name, '%s%i' % (
                        values.dtype.kind,
                        values.dtype.itemsize
                    ), dimensions=dimnames)

                if dimensionality == 1:
                    variable[:] = values
                elif dimensionality == 2:
                    variable[:, :] = values
                elif dimensionality == 3:
                    variable[:, :, :] = values


def get_dimensionality(values):
    """
    """
    num = 1
    values_slice = values
    while hasattr(values_slice, 'dtype') and values_slice.dtype.kind == 'O':
        values_slice = values_slice[0]
        num += 1

    return num
