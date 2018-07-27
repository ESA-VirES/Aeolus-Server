# ------------------------------------------------------------------------------
#
#  Base class for data extraction processes
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


from datetime import datetime, timedelta
from cStringIO import StringIO
import tempfile
import os.path
from uuid import uuid4
from itertools import izip

from django.utils.timezone import utc
from django.contrib.gis.geos import Polygon
from django.conf import settings
from eoxserver.services.ows.wps.parameters import (
    ComplexData, FormatJSON, CDObject, BoundingBoxData, LiteralData,
    FormatBinaryRaw, CDFile, Reference, RequestParameter
)
from eoxserver.services.ows.wps.exceptions import ServerBusy
import msgpack
from netCDF4 import Dataset, stringtochar
import numpy as np

from aeolus import models
from aeolus.processes.util.context import DummyContext
from aeolus.processes.util.auth import get_user, get_username
from aeolus.extraction.dsd import get_dsd


MAX_ACTIVE_JOBS = 2


class ExtractionProcessBase(object):
    """ Base class for data extraction processes
    """

    synchronous = True
    asynchronous = True

    inputs = [
        ("username", RequestParameter(get_username)),
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
        ("filters", ComplexData(
            'filters', title="Filters", abstract=(
                "JSON Object to set specific data filters."
            ), formats=FormatJSON(), optional=True
        )),
        ("dsd_info", LiteralData(
            'dsd_info', title="DSD Information", abstract=(
                "Whether to include each products ancestry information"
            ), optional=True, default=False,
        )),
    ]

    outputs = [
        ("output", ComplexData(
            "output", title="",
            formats=[
                FormatBinaryRaw('application/msgpack'),
                FormatBinaryRaw('application/netcdf'),
            ],
        )),
    ]

    @staticmethod
    def on_started(context, progress, message):
        """ Callback executed when an asynchronous Job gets started. """
        job = models.Job.objects.get(identifier=context.identifier)
        job.status = models.Job.STARTED
        job.started = datetime.now(utc)
        job.save()
        context.logger.info(
            "Job started after %.3gs waiting.",
            (job.started - job.created).total_seconds()
        )

    @staticmethod
    def on_succeeded(context, outputs):
        """ Callback executed when an asynchronous Job finishes. """
        job = models.Job.objects.get(identifier=context.identifier)
        job.status = models.Job.SUCCEEDED
        job.stopped = datetime.now(utc)
        job.save()
        context.logger.info(
            "Job finished after %.3gs running.",
            (job.stopped - job.started).total_seconds()
        )

    @staticmethod
    def on_failed(context, exception):
        """ Callback executed when an asynchronous Job fails. """
        job = models.Job.objects.get(identifier=context.identifier)
        job.status = models.Job.FAILED
        job.stopped = datetime.now(utc)
        job.save()
        context.logger.info(
            "Job failed after %.3gs running.",
            (job.stopped - job.started).total_seconds()
        )

    def initialize(self, context, inputs, outputs, parts):
        """ Asynchronous process initialization. """
        context.logger.info(
            "Received %s WPS request from %s.",
            self.identifier, inputs['\\username'] or "an anonymous user"
        )

        user = get_user(inputs['\\username'])
        active_jobs_count = models.Job.objects.filter(
            owner=user, status__in=(models.Job.ACCEPTED, models.Job.STARTED)
        ).count()

        if active_jobs_count >= MAX_ACTIVE_JOBS:
            raise ServerBusy(
                "Maximum number of allowed active asynchronous download "
                "requests exceeded!"
            )

        # create DB record for this WPS job
        job = models.Job()
        job.status = models.Job.ACCEPTED
        job.owner = user
        job.process_id = self.identifier
        job.identifier = context.identifier
        job.response_url = context.status_location
        job.save()

    def execute(self, collection_ids, begin_time, end_time, bbox, filters,
                output, context=None, **kwargs):
        """ The execution function of the process.
        """
        isasync = context is not None
        context = context or DummyContext()

        # TODO: time span from the actual product files?
        time_span = end_time - begin_time

        sync_span = getattr(
            settings, 'AEOLUS_EXTRACTION_SYNC_SPAN', timedelta(weeks=1)
        )
        async_span = getattr(
            settings, 'AEOLUS_EXTRACTION_ASYNC_SPAN', None
        )

        if isasync and async_span and time_span > async_span:
            raise Exception('Exceeding maximum allowed time span.')
        elif not isasync and time_span > sync_span:
            raise Exception('Exceeding maximum allowed time span.')

        # gather database filters
        db_filters = self.get_db_filters(
            begin_time, end_time, bbox, filters, **kwargs
        )

        collection_products = self.get_collection_products(
            collection_ids, db_filters
        )

        collection_product_counts = dict(
            (collection.identifier, products.count())
            for collection, products in collection_products
        )
        total_product_count = sum(collection_product_counts.values())

        data_filters = self.get_data_filters(
            begin_time, end_time, bbox, filters.data if filters else {}, **kwargs
        )

        mime_type = output['mime_type']

        # call the actual data extrction function
        out_data_iterator = self.extract_data(
            collection_products, data_filters, mime_type=mime_type, **kwargs
        )

        collection_products_dict = dict(
            (collection, products)
            for collection, products in collection_products
        )

        # encode as messagepack
        if mime_type == 'application/msgpack':
            if isasync:
                raise Exception(
                    'messagepack format is only available for synchronous '
                    'process invocation.'
                )

            out_data = self.accumulate_for_messagepack(out_data_iterator)

            if kwargs['dsd_info'] == 'true':
                for collection, products in collection_products_dict.items():
                    if collection.identifier in out_data:
                        out_data[collection.identifier]['dsd'] = dict(
                            (product.identifier, get_dsd(product))
                            for product in products
                        )

            encoded = StringIO(msgpack.dumps(out_data))
            return CDObject(
                encoded, filename=self.get_out_filename("mp"), **output
            )

        elif mime_type == 'application/netcdf':
            if isasync:
                outpath = self.get_out_filename("nc")
            else:
                uid = str(uuid4())
                outpath = os.path.join(tempfile.gettempdir(), uid) + '.nc'

            product_count = 0

            try:
                with Dataset(outpath, "w", format="NETCDF4") as ds:
                    for collection, data_iterator in out_data_iterator:
                        products = collection_products_dict[collection]
                        enumerated_data = izip(
                            enumerate(data_iterator, start=1), products
                        )
                        for (product_idx, file_data), product in enumerated_data:
                            # write the product data to the netcdf file
                            self.write_product_data_to_netcdf(ds, file_data)

                            if kwargs['dsd_info'] == 'true':
                                self.add_product_dsd(ds, product)

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
            except:
                # only cleanup file in sync processes, in async the files are
                # cleaned up seperately
                if not isasync:
                    os.remove(outpath)
                raise

            # result generation. For async processes, publish the file for the
            # webserver
            if isasync:
                return Reference(*context.publish(outpath), **output)

            # for sync cases, pass a reference to the file, which shall be
            # deleted afterwards
            return CDFile(
                outpath, filename=self.get_out_filename("nc"),
                remove_file=True, **output
            )

    def get_db_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        db_filters = dict(
            begin_time__lte=end_time,
            end_time__gte=begin_time,
        )

        if bbox:
            tpl_box = (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            box = Polygon.from_bbox(tpl_box)

            db_filters['ground_path__intersects'] = box

        return db_filters

    def get_data_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        return filters

    def get_collection_products(self, collection_ids, db_filters):
        collections = [
            models.ProductCollection.objects.get(identifier=identifier)
            for identifier in collection_ids.data
        ]

        return [
            (collection, models.Product.objects.filter(
                collections=collection,
                **db_filters
            ).order_by('begin_time'))
            for collection in collections
        ]

    def extract_data(self, collection_products, data_filters, mime_type, **kw):
        raise NotImplementedError

    def accumulate_for_messagepack(self, out_data_iterator):
        raise NotImplementedError

    def write_product_data_to_netcdf(self, ds, file_data):
        raise NotImplementedError

    def add_product_dsd(self, ds, product):
        dsd = get_dsd(product, strip=False)
        if dsd:
            grp = ds.createGroup('dsd').createGroup(product.identifier)
            grp.createDimension('dsd', len(dsd))
            first = dsd[0]

            for name in first.keys():
                values = [
                    item[name]
                    for item in dsd
                ]
                if isinstance(values[0], str):
                    dimname = "%s_nchars" % name
                    grp.createDimension(dimname, len(values[0]))
                    var = grp.createVariable(name, 'S1', ('dsd', dimname))

                    values = stringtochar(np.array(values))
                else:
                    var = grp.createVariable(name, 'i8', ('dsd',))

                var[:] = values

    def get_out_filename(self, extension):
        raise NotImplementedError
