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
from eoxserver.resources.coverages import models as coverages
from eoxserver.contrib import gdal

from aeolus.coda_utils import CODAFile
from aeolus import aux

DBL_PRODUCT_TYPES = {
    "ALD_U_N_1A",
    "ALD_U_N_1B",
    "ALD_U_N_2A",
    "ALD_U_N_2B",
    "ALD_U_N_2C",
}

EEF_PRODUCT_TYPES = {
    "AUX_ISR_1B",
    "AUX_MET_12",
    "AUX_MRC_1B",
    "AUX_RRC_1B",
    "AUX_ZWC_1B",
}


class DataFormatError(Exception):
    pass


class RegistrationError(Exception):
    pass


class AlreadyRegistered(RegistrationError):
    pass


def _get_ground_path(codafile):
    """ Extracts the ground track of the product file as a multiline string.
        All points are translated to be within longitude -180 to +180.
    """

    product_type = codafile.product_type

    if product_type in ["ALD_U_N_1A", "ALD_U_N_1B"]:
        ground_points = zip(
            codafile.fetch(
                "/geolocation", -1,
                "observation_geolocation/geolocation_of_dem_intersection/"
                "longitude_of_dem_intersection"
            ),
            codafile.fetch(
                "/geolocation", -1,
                "observation_geolocation/geolocation_of_dem_intersection/"
                "latitude_of_dem_intersection"
            )
        )

    elif product_type == "ALD_U_N_2A":
        ground_points = zip(
            chain.from_iterable(
                codafile.fetch(
                    "/geolocation", -1, "measurement_geolocation", -1,
                    "longitude_of_dem_intersection"
                )
            ),
            chain.from_iterable(
                codafile.fetch(
                    "/geolocation", -1, "measurement_geolocation", -1,
                    "latitude_of_dem_intersection"
                )
            )
        )

    elif product_type in ["ALD_U_N_2B", "ALD_U_N_2C"]:
        ground_points = zip(
            codafile.fetch("/mie_profile", -1, "profile_lon_average"),
            codafile.fetch("/mie_profile", -1, "profile_lat_average")
        )

    else:
        raise DataFormatError(f"Unsupported product format {product_type}!")

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

    strips.append(ground_points[last_jump:i]) # FIXME: fails for empty ground_points

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
        "identifier": codafile.fetch("mph/product").strip(),
        "begin_time": codafile.fetch_date("mph/sensing_start"),
        "end_time": codafile.fetch_date("mph/sensing_stop"),
        "footprint": ground_path,
        "format": "DBL",
        "product_type_name": codafile.product_type,
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

    if codafile.product_type == "AUX_MET_12":
        metadata = {
            "identifier": codafile.fetch("/mph/product"),
            "begin_time": codafile.fetch_date("/mph/sensing_start"),
            "end_time": codafile.fetch_date("/mph/sensing_stop"),
        }
    else:
        metadata = {
            "identifier": codafile.fetch(
                "/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header"
                "/Main_Product_Header/Product"
            ),
            "begin_time": codafile.fetch_date(
                "/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header"
                "/Validity_Period/Validity_Start"
            ),
            "end_time": codafile.fetch_date(
                "/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header"
                "/Validity_Period/Validity_Stop"
            ),
        }

    return {
        "footprint": footprint,
        "ground_path": ground_path,
        "format": "EEF",
        "product_type_name": codafile.product_type,
        **metadata
    }


def read_aeolus_product_metadata(filename):
    """ Read metadata from an Aeolus product file. """

    with CODAFile(filename) as codafile:

        if codafile.product_class != "AEOLUS":
            raise DataFormatError("Not an Aeolus product!")

        product_type = codafile.product_type

        if product_type in DBL_PRODUCT_TYPES:
            return get_dbl_metadata(codafile)

        if product_type in EEF_PRODUCT_TYPES:
            return get_eef_metadata(codafile)

    raise DataFormatError(f"Unsupported Aeolus product type {product_type}!")


def simplify_footprint(footprint, simplification_tolerance=None):
    """ Simplify footprint using the given simplification factor. """

    if not footprint or simplification_tolerance is None:
        return footprint

    simplified_footprint = footprint.simplify(simplification_tolerance)

    # simplify reduces "Multi"-Geometries to simple ones.
    # force the same geometry type as the original footprint
    if simplified_footprint.geom_type != footprint.geom_type:
        simplified_footprint = type(footprint)(simplified_footprint)

    return simplified_footprint


def update_product(product, metadata):
    """ Update the Product database record. """

    if "product_type_name" in metadata:
        product_type_name = metadata["product_type_name"]
        if not product_type_name:
            if product.product_type:
                product.product_type = None
        else:
            if not product.product_type or product.product_type.name != product_type_name:
                product.product_type = coverages.ProductType.objects.get(
                    name=product_type_name
                )

    if "begin_time" in metadata:
        product.begin_time = metadata["begin_time"]

    if "end_time" in metadata:
        product.end_time = metadata["end_time"]

    if "footprint" in metadata:
        product.footprint = metadata["footprint"]

    product.full_clean()
    product.save()

    return product


def update_product_data_item(product, location, format_):
    """ Update data item. """

    insert_new_item = True
    for data_item in list(product.product_data_items.all()):
        if data_item.location == location and data_item.format == format_:
            insert_new_item = False
        else:
            data_item.delete()

    if insert_new_item:
        data_item = coverages.ProductDataItem(
            location=location, format=format_,
        )
        data_item.product = product
        data_item.full_clean()
        data_item.save()


def register_product(filename, overrides,
                     footprint_simplification_tolerance=None):
    """ Registers a DBL file as a :class:`aeolus.models.Product`. Metadata is
        extracted from the specified file or passed.
    """

    metadata = read_aeolus_product_metadata(filename)

    metadata.update(overrides)

    metadata["footprint"] = simplify_footprint(
        metadata["footprint"],
        simplification_tolerance=footprint_simplification_tolerance,
    )

    product = update_product(
        product=coverages.Product(identifier=metadata["identifier"]),
        metadata=metadata,
    )

    update_product_data_item(
        product=product,
        location=filename,
        format_=metadata["format"] or "",
    )

    return product


def register_collection(identifier):
    pass


def register_albedo(
    identifier, filename, year, month, coverage_type, grid_name, replace=False,
):

    try:
        ds = gdal.Open(filename)
    except Exception as e:
        raise RegistrationError(
            "Failed to open raster file '%s'. Error was: %s"
            % (filename, e)
        ) from e

    nadir_location = offnadir_location = None

    # check type of file:
    if ds.RasterCount == 2:
        # TIF file with 2 bands
        subdatasets = []
        nadir_location = offnadir_location = filename
    elif ds.RasterCount == 0:
        # netCDF file with subdatasets
        subdatasets = ds.GetSubDatasets()

        for subdataset, _ in subdatasets:
            if subdataset.endswith("ADAM_albedo_nadir"):
                nadir_location = subdataset
            elif subdataset.endswith("ADAM_albedo_offnadir"):
                offnadir_location = subdataset
    else:
        raise RegistrationError(
            "Cannot register Albedo file '%s'" % filename
        )

    if not nadir_location:
        raise RegistrationError(
            "File '%s' is missing the nadir subdataset" % filename
        )
    if not offnadir_location:
        raise RegistrationError(
            "File '%s' is missing the offnadir subdataset" % filename
        )

    nadir_ds = gdal.Open(nadir_location)
    begin_time = datetime(year, month, 1, tzinfo=utc)
    end_time = datetime(
        begin_time.year + (begin_time.month // 12),
        ((begin_time.month % 12) + 1), 1, tzinfo=utc
    ) - timedelta(milliseconds=1)

    extent = (-180, -90, 180, 90)

    exists = coverages.Coverage.objects.filter(
        identifier=identifier
    ).exists()
    if exists:
        if replace:
            coverages.Coverage.objects.get(
                identifier=identifier
            ).delete()
        else:
            raise AlreadyRegistered(
                "Albedo file for %d/%d already registered" % (year, month)
            )

    grid, _ = coverages.Grid.objects.get_or_create(
        name=grid_name,
        coordinate_reference_system="EPSG:4326",
        axis_1_name="x",
        axis_2_name="y",
        axis_1_type=0,
        axis_2_type=0,
        axis_1_offset=str(360 / nadir_ds.RasterXSize),
        axis_2_offset=str(-180 / nadir_ds.RasterYSize),
    )

    coverage = coverages.Coverage.objects.create(
        identifier=identifier,
        begin_time=begin_time,
        end_time=end_time,
        footprint=MultiPolygon(Polygon.from_bbox(extent)),
        grid=grid,
        axis_1_size=nadir_ds.RasterXSize,
        axis_2_size=nadir_ds.RasterYSize,
        axis_1_origin=-180,
        axis_2_origin=90,
        coverage_type=coverage_type,
    )

    if nadir_location == offnadir_location:
        coverages.ArrayDataItem.objects.create(
            location=nadir_location,
            format="image/tiff",
            coverage=coverage,
            field_index=0,
            band_count=2,
        )
    else:
        for i, path in enumerate((offnadir_location, nadir_location)):
            coverages.ArrayDataItem.objects.create(
                location=path,
                format="application/x-netcdf",
                coverage=coverage,
                field_index=i,
                band_count=1,
            )

    return coverage
