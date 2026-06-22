#-------------------------------------------------------------------------------
#
# Export Aeolus products
#
# Authors: Martin Paces <martin.paces@eox.at>
#-------------------------------------------------------------------------------
# Copyright (C) 2026 EOX IT Services GmbH
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
#-------------------------------------------------------------------------------
# pylint: disable=missing-docstring

import sys
import json
from django.core.exceptions import ObjectDoesNotExist
from eoxserver.resources.coverages.models import Product
from .._common import JSON_OPTS
from .._aeolus_product.common import ObjectSelectionSubcommand


class ExportProductSubcommand(ObjectSelectionSubcommand):
    name = "export"
    help = "Export Aeolus products"

    description = "Export Aeolus product in JSON format."

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-f", "--file", dest="filename", default="-", help=(
                "Optional file-name the output is written to. "
                "By default it is written to the standard output."
            )
        )
        parser.add_argument(
            "--footprint", dest="export_footprint",
            action="store_true", default=False,
            help="Use to export footprint geometry."
        )

    def handle(self, **kwargs):
        export_footprint = kwargs["export_footprint"]

        def _serialize(objects):
            for object_ in objects:
                has_collection = False
                for collection in object_.collections.all():
                    yield serialize_product(
                        collection, object_, export_footprint=export_footprint,
                    )
                    has_collection = True
                if not has_collection:
                    yield serialize_product(
                        None, object_, export_footprint=export_footprint,
                    )

        data = list(_serialize(
            self.select_objects(Product.objects.all(), **kwargs)
        ))
        filename = kwargs["filename"]
        with (sys.stdout if filename == "-" else open(filename, "w", encoding="utf-8")) as file_:
            json.dump(data, file_, **JSON_OPTS)


def serialize_product(collection, product, export_footprint=False):
    data = {
        "identifier": product.identifier,
        "collection": collection.identifier if collection else None,
        "productType": (
            product.product_type.name if product.product_type else None
        ),
        "beginTime": serialize_timestamp(product.begin_time),
        "endTime": serialize_timestamp(product.end_time),
        "created": serialize_timestamp(product.inserted),
        "updated": serialize_timestamp(product.updated),
        "dataItems": [
            serialize_data_item(data_item)
            for data_item in product.product_data_items.all()
        ],
    }

    try:
        data["dataItems"].append(
            serialize_data_item(product.optimized_data_item, item_type="optimized")
        )
    except ObjectDoesNotExist:
        pass

    if export_footprint:
        data["footprint"] = serialize_footprint(product.footprint)
    return data


def serialize_data_item(data_item, item_type="source"):
    return {
        "type": item_type,
        "format": data_item.format,
        "location": data_item.location,
    }

def serialize_timestamp(value):
    if value is None:
        return None
    return value.isoformat().replace("+00:00","Z")


def serialize_footprint(value):
    if value is None:
        return None
    return {
        "srid": value.srid,
        "geometry": json.loads(value.json)
    }
