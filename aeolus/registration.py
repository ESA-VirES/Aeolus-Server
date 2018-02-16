# ------------------------------------------------------------------------------
#
#  Registration of Aeolus data and auxiliary files
#
# Project: VirES-Aeolus
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2017 EOX IT Services GmbH
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

from itertools import chain
from datetime import datetime, timedelta

from django.utils.timezone import utc
from django.contrib.gis.geos import (
    MultiLineString, LineString, MultiPolygon, Polygon
)
from eoxserver.backends import models as backends
from eoxserver.resources.coverages import models as coverages
from eoxserver.contrib import gdal

from aeolus import models
from aeolus.coda_utils import CODAFile
from aeolus import aux


class RegistrationError(Exception):
    pass


def _get_ground_path(codafile):
    """ Extracts the ground track of the product file as a multiline string.
        All points are translated to be within longitude -180 to +180.
    """

    product_type = codafile.product_type
    if product_type == 'ALD_U_N_1B':
        ground_points = zip(
            codafile.fetch(
                '/geolocation', -1,
                'observation_geolocation/geolocation_of_dem_intersection/'
                'longitude_of_dem_intersection'
            ),
            codafile.fetch(
                '/geolocation', -1,
                'observation_geolocation/geolocation_of_dem_intersection/'
                'latitude_of_dem_intersection'
            )
        )

    elif product_type == 'ALD_U_N_2A':
        ground_points = zip(
            chain.from_iterable(
                codafile.fetch(
                    '/geolocation', -1, 'measurement_geolocation', -1,
                    'longitude_of_dem_intersection'
                )
            ),
            chain.from_iterable(
                codafile.fetch(
                    '/geolocation', -1, 'measurement_geolocation', -1,
                    'latitude_of_dem_intersection'
                )
            )
        )

    elif product_type == 'ALD_U_N_2B':
        ground_points = zip(
            codafile.fetch('/mie_profile', -1, 'profile_lon_average'),
            codafile.fetch('/mie_profile', -1, 'profile_lat_average')
        )

    return _ground_points_to_path(ground_points)


def _ground_points_to_path(ground_points):
    ground_points = [
        (lon if lon <= 180 else lon - 360, lat)
        for lon, lat in ground_points
    ]
    strips = []
    last_lon = None
    last_jump = 0
    for i, point in enumerate(ground_points):
        if last_lon is not None and abs(last_lon - point[0]) > 180:
            strips.append(ground_points[last_jump:i])
            last_jump = i

        last_lon = point[0]

    strips.append(ground_points[last_jump:i])

    ground_path = MultiLineString([
        LineString(strip)
        for strip in strips
        if len(strip) > 1
    ])
    return ground_path


def get_dbl_metadata(codafile):
    """ Extracts the metadata from the specified coda file.
    """
    ground_path = _get_ground_path(codafile)

    return {
        "identifier": codafile.fetch('mph/product').strip(),
        "begin_time": codafile.fetch_date('mph/sensing_start'),
        "end_time": codafile.fetch_date('mph/sensing_stop'),
        "footprint": MultiPolygon(Polygon.from_bbox(ground_path.extent)),
        "ground_path": ground_path,

        "format": "DBL",
        "size_x": 1,
        "size_y": 1,

        "range_type_name": codafile.product_type,
    }


def get_eef_metadata(codafile):
    ground_points = aux.fetch_ground_points(codafile)

    ground_path = None
    if ground_points:
        ground_path = _ground_points_to_path(ground_points)
        footprint = MultiPolygon(
            Polygon.from_bbox(ground_path.extent)
        )
    else:
        footprint = MultiPolygon(
            Polygon.from_bbox([-180, -90, 180, 90])
        )

    if codafile.product_type[:7] == 'AUX_MET':
        metadata = {
            "identifier": codafile.fetch('/mph/product'),
            "begin_time": codafile.fetch_date('/mph/sensing_start'),
            "end_time": codafile.fetch_date('/mph/sensing_stop'),
        }
    else:
        metadata = {
            "identifier": codafile.fetch(
                '/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header'
                '/Main_Product_Header/Product'
            ),
            "begin_time": codafile.fetch_date(
                '/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header'
                '/Validity_Period/Validity_Start'
            ),
            "end_time": codafile.fetch_date(
                '/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header'
                '/Validity_Period/Validity_Stop'
            ),
        }

    return dict(
        footprint=footprint,
        ground_path=ground_path,

        format="EEF",
        size_x=1,
        size_y=1,

        range_type_name=codafile.product_type,
        **metadata
    )


def register_product(filename, overrides):
    """ Registers a DBL file as a :class:`aeolus.models.Product`. Metadata is
        extracted from the specified file or passed.
    """

    codafile = CODAFile(filename)

    assert codafile.product_class == 'AEOLUS'

    product_type = codafile.product_type

    if product_type.startswith('ALD'):
        metadata = get_dbl_metadata(codafile)
    elif product_type.startswith('AUX'):
        metadata = get_eef_metadata(codafile)
    else:
        raise AssertionError('Unsupported product type %r' % product_type)

    metadata.update(overrides)

    range_type = coverages.RangeType.objects.get(
        name=metadata.pop('range_type_name')
    )

    # Register the product
    product = models.Product()
    product.identifier = metadata['identifier']
    product.visible = False
    product.range_type = range_type
    product.srid = 4326
    product.extent = metadata['footprint'].extent
    for key, value in metadata.iteritems():
        setattr(product, key, value)

    product.full_clean()
    product.save()

    # storage, package, format_, location = _get_location_chain([data_file])
    data_item = backends.DataItem(
        location=filename, format=metadata['format'] or "",
        semantic="bands[1:%d]" % len(range_type),
        storage=None, package=None,
    )
    data_item.dataset = product
    data_item.full_clean()
    data_item.save()

    return product


def register_collection(identifier):
    pass


def register_albedo(filename, year, month, replace=False):
    """
    """
    try:
        ds = gdal.Open(filename)
    except Exception as e:
        raise RegistrationError(
            "Failed to open raster file '%s'. Error was: %s"
            % (filename, e)
        )

    subdatasets = ds.GetSubDatasets()

    nadir_location = offnadir_location = None
    for subdataset, _ in subdatasets:
        if subdataset.endswith('ADAM_albedo_nadir'):
            nadir_location = subdataset
        elif subdataset.endswith('ADAM_albedo_offnadir'):
            offnadir_location = subdataset

    if not nadir_location:
        raise RegistrationError(
            "File '%s' is missing the nadir subdataset" % filename
        )
    if not offnadir_location:
        raise RegistrationError(
            "File '%s' is missing the offnadir subdataset" % filename
        )

    sds = gdal.Open(nadir_location)

    begin_time = datetime(year, month, 1, tzinfo=utc)
    end_time = datetime(
        begin_time.year + (begin_time.month / 12),
        ((begin_time.month % 12) + 1), 1, tzinfo=utc
    ) - timedelta(milliseconds=1)

    extent = (-180, -90, 180, 90)

    try:
        range_type = coverages.RangeType.objects.get(name='ADAM_albedo')
    except coverages.RangeType.DoesNotExist:
        raise RegistrationError('Could not find Albedo range type.')

    identifier = 'ADAM_albedo_%d_%d' % (year, month)

    exists = coverages.RectifiedDataset.objects.filter(
        identifier=identifier
    ).exists()
    if exists:
        if replace:
            coverages.RectifiedDataset.objects.get(
                identifier=identifier
            ).delete()
        else:
            raise RegistrationError(
                'Albedo file for %d/%d already registered' % (year, month)
            )

    ds_model = coverages.RectifiedDataset.objects.create(
        identifier=identifier,
        begin_time=begin_time,
        end_time=end_time,
        footprint=MultiPolygon(Polygon.from_bbox(extent)),
        size_x=sds.RasterXSize,
        size_y=sds.RasterYSize,
        srid=4326,
        min_x=-180,
        min_y=-90,
        max_x=180,
        max_y=90,
        range_type=range_type,
    )

    for i, path in enumerate((offnadir_location, nadir_location), start=1):
        backends.DataItem.objects.create(
            location=path,
            format='NetCDF',
            semantic='bands[%d]' % i,
            dataset=ds_model,
        )

    return ds_model
