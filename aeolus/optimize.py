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
from aeolus import level_1a
from aeolus import level_1b
from aeolus import level_2a
from aeolus import level_2b
from aeolus import level_2c
from aeolus import aux
from aeolus import aux_met

class OptimizationError(Exception):
    pass

NETCDF_VARIABLE_OPTIONS = {
    "fletcher32": True,
    #"compression": "zstd",
}


# range-type -> CODA file locations
CODA_LOCATIONS = {
    "ALD_U_N_1A": {
        "OBSERVATION_DATA": level_1a.OBSERVATION_LOCATIONS,
        "MEASUREMENT_DATA": level_1a.MEASUREMENT_LOCATIONS,
    },
    "ALD_U_N_1B": {
        "OBSERVATION_DATA": level_1b.OBSERVATION_LOCATIONS,
        "MEASUREMENT_DATA": level_1b.MEASUREMENT_LOCATIONS,
    },
    "ALD_U_N_2A": {
        "OBSERVATION_DATA": level_2a.OBSERVATION_LOCATIONS,
        "MEASUREMENT_DATA": level_2a.MEASUREMENT_LOCATIONS,
    },
    "ALD_U_N_2B": {
        "DATA": level_2b.locations,
    },
    "ALD_U_N_2C": {
        "DATA": level_2c.locations,
    },
    "AUX_ISR_1B": {
        "DATA": aux.AUX_ISR_LOCATIONS,
    },
    "AUX_MRC_1B": {
        "DATA": aux.AUX_MRC_LOCATIONS,
    },
    "AUX_RRC_1B": {
        "DATA": aux.AUX_RRC_LOCATIONS,
    },
    "AUX_ZWC_1B": {
        "DATA": aux.AUX_ZWC_LOCATIONS,
    },
    "AUX_MET_12": {
        "DATA": aux_met.LOCATIONS,
    }
}


def get_product_type(filename):
    """ Read product type from an Aeolus product file. """

    with CODAFile(filename) as codafile:

        if codafile.product_class != "AEOLUS":
            raise OptimizationError("Not an Aeolus product!")

        return codafile.product_type


def optimize_aeolus_product(input_filename, output_filename, fields=None,
                            logger=None):
    """ Convert native Aeolus product to a new optimized NetCDF file. """

    if logger is None:
        logger = logging.getLogger(__name__)

    product_type = get_product_type(input_filename)

    try:
        location_groups = CODA_LOCATIONS[product_type]
    except KeyError:
        raise OptimizationError(
            f"Unsupported Aeolus product type {product_type}!"
        ) from None

    # verify the requested fields
    if fields is not None:
        for field in fields:
            for locations in location_groups.values():
                if field in locations:
                    break
            else:
                raise OptimizationError(f"Invalid product field {field}!")

    logger.debug(
        "optimizing %s product %s => %s",
        product_type, input_filename, output_filename,
    )

    temporary_filename = os.path.join(
        os.path.dirname(output_filename),
        f".{os.path.basename(output_filename)}.tmp"
    )

    _remove_file(temporary_filename)

    try:
        with CODAFile(input_filename) as in_cf:
            with Dataset(temporary_filename, "w", format="NETCDF4") as out_ds:
                _optimize_fields(
                    location_groups, in_cf, out_ds, fields, logger=logger,
                )

        os.rename(temporary_filename, output_filename)

    except Exception as error:
        logger.error(
            "Failed to optimize Aeolus product %s! %s",
            input_filename, error
        )
        raise
    finally:
        _remove_file(temporary_filename)


def _remove_file(filename):
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass


def _optimize_fields(location_groups, in_cf, out_ds, fields, logger):

    for group_name, locations in location_groups.items():
        group = out_ds.createGroup(group_name)
        for name, location in locations.items():
            # if we have a dedicated list of fields to optimize, we skip if the
            # current field is not in that list
            if fields is not None and name not in fields:
                continue
            logger.debug("optimizing field %s/%s", group_name, name)

            try:
                _optimize_field(out_ds, in_cf, group, name, location, logger)
            except Exception as error:
                logger.error(
                    "Failed to optimize field %s/%s! %s",
                    group_name, name, error,
                )
                raise


def _optimize_field(out_ds, in_cf, group, name, location, logger):
    try:
        data = access_location(in_cf, location)
    except NoSuchFieldException:
        logger.warn(f"No such field {name}!")
        return

    data = _stack_nested_arrays(data)

    variable = _create_variable(
        out_ds=out_ds,
        group=group,
        variable_name=name,
        shape = data.shape,
        data_type = f"{data.dtype.kind}{data.dtype.itemsize}",
    )
    variable[...] = data


def _create_variable(out_ds, group, variable_name, shape, data_type):
    dimensions = [f"arr_{size}" for size in shape]
    for dimension, size in zip(dimensions, shape):
        if dimension not in out_ds.dimensions:
            out_ds.createDimension(dimension, size)
    return group.createVariable(
        variable_name, data_type,
        dimensions=dimensions,
        **NETCDF_VARIABLE_OPTIONS,
    )


def _stack_nested_arrays(data):
    if isinstance(data, np.ndarray) and data.dtype.kind == "O":
        shape = data.shape
        flat_data = data.ravel(order="C")
        stacked_data = np.stack(flat_data, axis=0)
        new_data = stacked_data.reshape(
            (*shape, *stacked_data.shape[1:]), order="C"
        )
        return _stack_nested_arrays(new_data)
    return data
