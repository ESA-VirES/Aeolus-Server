# ------------------------------------------------------------------------------
#
# Sampling of Albedo values for nadir/offnadir at specific points
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

import os

import numpy as np

from eoxserver.resources.coverages import models
from eoxserver.contrib import gdal


# we have to use this in order to make the albedo NetCDFs readable
os.environ.setdefault("GDAL_NETCDF_BOTTOMUP", "NO")


class AlbedoFileError(Exception):
    pass


def sample_offnadir(year, month, lons, lats):
    """ Sample the registered off-nadir values for the registered albedo files
        for the given year, month, and longitudes/latitudes.
    """
    return _sample_data_item(year, month, 1, lons, lats)


def sample_nadir(year, month, lons, lats):
    """ Sample the registered nadir values for the registered albedo files for
        the given year, month, and longitudes/latitudes.
    """
    return _sample_data_item(year, month, 2, lons, lats)


def _sample_data_item(year, month, index, lons, lats):
    identifier = 'ADAM_albedo_%d_%d' % (year, month)
    try:
        albedo = models.Coverage.objects.get(identifier=identifier)
    except models.Coverage.DoesNotExist:
        raise AlbedoFileError('No albedo file for %d/%d registered.' % (
            year, month)
        )

    try:
        # netCDFs are two data items
        data_item = albedo.arraydata_items.get(field_index=index)
    except models.ArrayDataItem.DoesNotExist:
        # when we only have a single TIFF with 2 bands
        data_item = albedo.arraydata_items.get()

    ds = gdal.Open(data_item.location)
    if ds.RasterCount == 1:
        band = ds.GetRasterBand(1)
    else:
        band = ds.GetRasterBand(index)

    o_x = -180.0
    o_y = 90.0

    res_x = 360.0 / ds.RasterXSize
    res_y = -180.0 / ds.RasterYSize

    out_data = np.empty((len(lons),))

    for i, coord in enumerate(zip(lons, lats)):
        lon, lat = coord

        px = int(round((lon - o_x) / res_x))
        py = int(round((lat - o_y) / res_y))
        out_data[i] = band.ReadAsArray(px, py, 1, 1)

    return out_data
