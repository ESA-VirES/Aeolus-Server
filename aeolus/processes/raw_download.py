# ------------------------------------------------------------------------------
#
#  Data extraction from Level 1B ADM-Aeolus products
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

from datetime import datetime
import zipfile
import tarfile
import os.path
from itertools import izip

from django.contrib.gis.geos import Polygon
from eoxserver.core import Component, implements
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from eoxserver.services.ows.wps.parameters import (
    ComplexData, FormatJSON, BoundingBoxData, LiteralData,
    FormatBinaryRaw, Reference
)

from aeolus import models
from aeolus.processes.util.base import AsyncProcessBase


class RawDownloadProcess(AsyncProcessBase, Component):
    """ This process allows to download raw data files from registered Products
        in ZIP/TAR archives
    """
    implements(ProcessInterface)

    synchronous = False

    identifier = "aeolus:download:raw"
    metadata = {}
    profiles = ["vires-util"]

    inputs = AsyncProcessBase.inputs + [
        ("collection_ids", ComplexData(
            'collection_ids', title="Collection identifiers", abstract=(
                ""
            ), formats=FormatJSON()
        )),
        ("begin_time", LiteralData(
            'begin_time', datetime, optional=False, title="Begin time",
            abstract="Start of the selection time interval",
        )),
        ("end_time", LiteralData(
            'end_time', datetime, optional=False, title="End time",
            abstract="End of the selection time interval",
        )),
        ("bbox", BoundingBoxData(
            "bbox", crss=(4326, 3857), optional=True, title="Bounding box",
            abstract="Optional selection bounding box.", default=None,
        )),
    ]

    outputs = [
        ("output", ComplexData(
            "output", title="",
            formats=[
                FormatBinaryRaw('application/zip'),
                FormatBinaryRaw('application/tar'),
                FormatBinaryRaw('application/tar+gzip'),
                FormatBinaryRaw('application/tar+bz2'),
            ],
        )),
    ]

    def execute(self, collection_ids, begin_time, end_time, bbox,
                output, context=None, **kwargs):
        collections = [
            models.ProductCollection.objects.get(identifier=identifier)
            for identifier in collection_ids.data
        ]

        db_filters = dict(
            begin_time__lte=end_time,
            end_time__gte=begin_time,
        )

        if bbox:
            box = Polygon.from_bbox(
                (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            )

            db_filters['ground_path__intersects'] = box

        collection_products = [
            (collection, models.Product.objects.filter(
                collections=collection,
                **db_filters
            ).order_by('begin_time'))
            for collection in collections
        ]

        collection_iter = (
            (collection, (
                    (
                        product,
                        product.data_items.values_list('location', flat=True)
                    )
                    for product in products
                )
            )
            for collection, products in collection_products
        )

        collection_product_counts = dict(
            (collection.identifier, products.count())
            for collection, products in collection_products
        )
        total_product_count = sum(collection_product_counts.values())

        mime_type = output['mime_type']

        if mime_type == 'application/zip':
            extension = '.zip'
            out_filename = self.get_out_filename(
                "RAW", begin_time, end_time, extension
            )
            archive = zipfile.ZipFile(out_filename, 'w', zipfile.ZIP_DEFLATED)
            add_func = archive.write

        elif mime_type.startswith('application/tar'):
            compression = ''
            extension = '.tar'
            if mime_type == 'application/tar+gzip':
                compression = ':gz'
                extension = '.tar.gz'
            elif mime_type == 'application/tar+bz2':
                compression = ':bz2'
                extension = '.tar.bz2'

            out_filename = self.get_out_filename(
                "RAW", begin_time, end_time, extension
            )

            archive = tarfile.open(out_filename, 'w%s' % compression)
            add_func = archive.add

        with archive:
            product_count = 0
            for collection, products_iter in collection_iter:
                enumerated_data = izip(
                    enumerate(products_iter, start=1), products_iter
                )
                for product_idx, (product, filenames) in enumerated_data:
                    for filename in filenames:
                        add_func(
                            filename,
                            arcname=os.path.join(
                                collection.identifier,
                                product.identifier,
                                os.path.basename(filename)
                            )
                        )
                    # update progress on a per-product basis
                    context.update_progress(
                        (product_count * 100) // total_product_count,
                        "Filtering collection %s, product %d of %d." % (
                            collection.identifier, product_idx,
                            collection_product_counts[
                                collection.identifier
                            ]
                        )
                    )
                    product_count += 1

        return Reference(
            *context.publish(out_filename), **output
        )

    def get_out_filename(self, filetype, begin_time, end_time, extension):
        return "AE_OPER_%s_%s_%s_vires%s" % (
            filetype,
            begin_time.strftime("%Y%m%dT%H%M%S"),
            end_time.strftime("%Y%m%dT%H%M%S"),
            extension
        )
