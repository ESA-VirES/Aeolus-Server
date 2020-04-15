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
from io import BytesIO
import tempfile
import os.path
from uuid import uuid4
from logging import getLogger
import json

from django.utils.timezone import utc
from django.contrib.gis.geos import Polygon
from django.conf import settings

from eoxserver.core.util.timetools import isoformat
from eoxserver.services.ows.wps.parameters import (
    ComplexData, FormatJSON, CDObject, BoundingBoxData, LiteralData,
    FormatBinaryRaw, CDFile, Reference, RequestParameter
)
from eoxserver.services.ows.wps.exceptions import ServerBusy
from eoxserver.resources.coverages.models import Collection, Product
import msgpack
from netCDF4 import Dataset, stringtochar
import numpy as np

from aeolus import models
from aeolus.processes.util.context import DummyContext
from aeolus.processes.util.auth import get_user, get_username
from aeolus.extraction.dsd import get_dsd
from aeolus.util import cached_property


MAX_ACTIVE_JOBS = 2


class AsyncProcessBase(object):
    """
    """
    asynchronous = True

    inputs = [
        ("username", RequestParameter(get_username)),
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

    @cached_property
    def access_logger(self):
        """ Get access logger. """
        return getLogger(
            "access.wps.%s" % self.__class__.__module__.split(".")[-1]
        )

    def initialize(self, context, inputs, outputs, parts):
        """ Asynchronous process initialization. """
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


class ExtractionProcessBase(AsyncProcessBase):
    """ Base class for data extraction processes
    """

    synchronous = True

    range_type_name = None

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

    def execute(self, collection_ids, begin_time, end_time, bbox,
                filters, output, context=None, **kwargs):
        """ The execution function of the process.
        """
        isasync = context is not None
        context = context or DummyContext()

        # lenient handling of begin/end time swapping
        if begin_time > end_time:
            begin_time, end_time = end_time, begin_time

        # TODO: time span from the actual product files?
        time_span = end_time - begin_time

        sync_span = getattr(
            settings, 'AEOLUS_EXTRACTION_SYNC_SPAN', timedelta(weeks=1)
        )
        async_span = getattr(
            settings, 'AEOLUS_EXTRACTION_ASYNC_SPAN', None
        )

        if isasync and async_span and time_span > async_span:
            message = 'Exceeding maximum allowed time span.'
            self.access_logger.error(message)
            raise Exception(message)
        elif not isasync and time_span > sync_span:
            message = 'Exceeding maximum allowed time span.'
            self.access_logger.error(message)
            raise Exception(message)

        # log the request
        self.access_logger.info(
            "request parameters: user: %s, toi: (%s, %s), bbox: %s, "
            "collections: (%s), filters: %s, type: %s",
            kwargs["username"],
            begin_time.isoformat("T"), end_time.isoformat("T"),
            [bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1]] if bbox else None,
            ", ".join(collection_ids.data),
            json.dumps(filters.data) if filters else "None",
            "async" if isasync else "sync"
        )

        # gather database filters
        db_filters = self.get_db_filters(
            begin_time, end_time, bbox, filters, **kwargs
        )

        collection_products = self.get_collection_products(
            collection_ids, db_filters, kwargs["username"]
        )

        collection_product_counts = dict(
            (collection.identifier, products.count())
            for collection, products in collection_products
        )
        total_product_count = sum(collection_product_counts.values())

        collection_products_dict = dict(
            (collection, products)
            for collection, products in collection_products
        )

        data_filters = self.get_data_filters(
            begin_time, end_time, bbox, filters.data if filters else {},
            **kwargs
        )

        mime_type = output['mime_type']

        # call the actual data extrction function
        out_data_iterator = self.extract_data(
            collection_products, data_filters, mime_type=mime_type, **kwargs
        )

        # generate a nice filename for the output file
        extension = None
        if mime_type == 'application/msgpack':
            extension = 'mp'
        elif mime_type == 'application/netcdf':
            extension = 'nc'

        # TODO: for when multiple collections or none at all
        out_filename = self.get_out_filename(
            collection_products[0][0].identifier, begin_time, end_time, extension
        )

        # TODO: get selected fields
        # some result logging
        fields_for_logging = dict(
            (arg_name, value)
            for arg_name, value in kwargs.items()
            if arg_name.endswith('fields')
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

            encoded = BytesIO(msgpack.dumps(out_data))

            # some result logging
            self.access_logger.info(
                "response: count: %d files, mime-type: %s, fields: %s",
                total_product_count, mime_type, json.dumps(fields_for_logging)
            )

            return CDObject(
                encoded, filename=out_filename, **output
            )

        elif mime_type == 'application/netcdf':
            if not isasync:
                uid = str(uuid4())
                tmppath = os.path.join(tempfile.gettempdir(), uid) + '.nc'
            else:
                tmppath = out_filename

            product_count = 0
            identifiers = []

            try:
                with Dataset(tmppath, "w", format="NETCDF4") as ds:
                    for collection, data_iterator in out_data_iterator:
                        products = collection_products_dict[collection]
                        enumerated_data = zip(
                            enumerate(data_iterator, start=1), products
                        )
                        for (product_idx, file_data), product in enumerated_data:
                            # write the product data to the netcdf file
                            self.write_product_data_to_netcdf(ds, file_data)

                            identifiers.append(product.identifier)

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

                    ds.history = json.dumps({
                        'inputFiles': identifiers,
                        'filters': filters.data if filters else None,
                        'beginTime': (
                            isoformat(begin_time) if begin_time else None
                        ),
                        'endTime': isoformat(end_time) if end_time else None,
                        'bbox': [
                            bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1]
                        ] if bbox else None,
                        'created': isoformat(datetime.now()),
                    })

            except:
                # only cleanup file in sync processes, in async the files are
                # cleaned up seperately
                if not isasync:
                    os.remove(tmppath)
                raise

            # some result logging
            self.access_logger.info(
                "response: count: %d files, mime-type: %s, fields: %s",
                total_product_count, mime_type, json.dumps(fields_for_logging)
            )

            # result generation. For async processes, publish the file for the
            # webserver
            if isasync:
                return Reference(*context.publish(out_filename), **output)

            # for sync cases, pass a reference to the file, which shall be
            # deleted afterwards
            return CDFile(
                tmppath, filename=out_filename,
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

            db_filters['footprint__intersects'] = box

        if self.range_type_name:
            db_filters['product_type__name'] = self.range_type_name

        return db_filters

    def get_data_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        return filters

    def get_collection_products(self, collection_ids, db_filters, username):
        collections = [
            Collection.objects.get(identifier=identifier)
            for identifier in collection_ids.data
        ]

        # user = get_user(username)

        # if not user:
        #     raise PermissionDenied("Not logged in")

        # for collection in collections:
        #     if not user.has_perm("aeolus.access_%s" % collection.identifier):
        #         raise PermissionDenied(
        #             "No access to '%s' permitted" % collection.identifier
        #         )

        return [
            (collection, Product.objects.filter(
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

                    values = stringtochar(np.array([
                        bytes(v, 'ascii') for v in values
                    ]))
                else:
                    var = grp.createVariable(name, 'i8', ('dsd',))

                var[:] = values

    def get_out_filename(self, filetype, begin_time, end_time, extension):
        return "AE_OPER_%s_%s_%s_vires.%s" % (
            filetype,
            begin_time.strftime("%Y%m%dT%H%M%S"),
            end_time.strftime("%Y%m%dT%H%M%S"),
            extension
        )
