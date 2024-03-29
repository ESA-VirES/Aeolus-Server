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

import os
import os.path
from datetime import datetime, timedelta
from io import BytesIO
import tempfile
from uuid import uuid4
from logging import getLogger, LoggerAdapter
import json
import msgpack
from netCDF4 import Dataset, stringtochar
import numpy

from django.utils.timezone import utc
from django.contrib.gis.geos import Polygon
from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.db.models import F, Func

from eoxserver.core.util.timetools import isoformat
from eoxserver.services.ows.wps.parameters import (
    ComplexData, FormatJSON, CDObject, BoundingBoxData, LiteralData,
    FormatBinaryRaw, CDFile, Reference, RequestParameter
)
from eoxserver.services.ows.wps.exceptions import (
    InvalidInputValueError, InvalidOutputDefError, ServerBusy,
)
from eoxserver.resources.coverages.models import Collection, Product

from aeolus.models import Job
from aeolus.processes.util.context import DummyContext
from aeolus.processes.util.auth import get_user, get_username
from aeolus.extraction.dsd import get_dsd
from aeolus.extraction.mph import get_mph
from aeolus.util import cached_property

MAX_ACTIVE_JOBS = 2


def get_remote_addr(request):
    """ Extract remote address from the Django HttpRequest """
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        return x_forwarded_for.partition(',')[0]
    return request.META.get('REMOTE_ADDR')


class BaseProcess():
    """ Base process class
    """
    identifier = None

    inputs = [
        ("username", RequestParameter(get_username)),
        ("remote_addr", RequestParameter(get_remote_addr)),
    ]

    class AccessLoggerAdapter(LoggerAdapter):
        """ Logger adapter adding extra fields required by the access logger. """

        def __init__(self, logger, username=None, remote_addr=None, **kwargs):
            super().__init__(logger, {
                "remote_addr": remote_addr if remote_addr else "-",
                "username": username if username else "-",
            })

    def get_access_logger(self, *args, **kwargs):
        """ Get access logger wrapped by the AccessLoggerAdapter """
        return self.AccessLoggerAdapter(self._access_logger, *args, **kwargs)

    @cached_property
    def _access_logger(self):
        """ Get raw access logger. """
        return getLogger(
            "access.wps.%s" % self.__class__.__module__.split(".")[-1]
        )


class AsyncProcessBase(BaseProcess):
    """ Base asynchronous WPS process class.
    """
    asynchronous = True

    @staticmethod
    def on_started(context, progress, message):
        """ Callback executed when an asynchronous Job gets started. """
        try:
            job = Job.objects.get(identifier=context.identifier)
            job.status = Job.STARTED
            job.started = datetime.now(utc)
            job.save()
            context.logger.info(
                "Job started after %.3gs waiting.",
                (job.started - job.created).total_seconds()
            )
        except Job.DoesNotExist:
            context.logger.warning(
                "Failed to update the job status! The job does not exist!"
            )

    @staticmethod
    def on_succeeded(context, outputs):
        """ Callback executed when an asynchronous Job finishes. """
        try:
            job = Job.objects.get(identifier=context.identifier)
            job.status = Job.SUCCEEDED
            job.stopped = datetime.now(utc)
            job.save()
            context.logger.info(
                "Job finished after %.3gs running.",
                (job.stopped - job.started).total_seconds()
            )
        except Job.DoesNotExist:
            context.logger.warning(
                "Failed to update the job status! The job does not exist!"
            )

    @staticmethod
    def on_failed(context, exception):
        """ Callback executed when an asynchronous Job fails. """
        try:
            job = Job.objects.get(identifier=context.identifier)
            job.status = Job.FAILED
            job.stopped = datetime.now(utc)
            job.save()
            context.logger.info(
                "Job failed after %.3gs running.",
                (job.stopped - job.started).total_seconds()
            )
        except Job.DoesNotExist:
            context.logger.warning(
                "Failed to update the job status! The job does not exist!"
            )

    @staticmethod
    def discard(context):
        """ Callback discarding Job's resources """
        try:
            Job.objects.get(identifier=context.identifier).delete()
            context.logger.info("Job removed.")
        except Job.DoesNotExist:
            pass

    def initialize(self, context, inputs, outputs, parts):
        """ Asynchronous process initialization. """
        context.logger.info(
            "Received %s asynchronous WPS request from %s.",
            self.identifier, inputs['\\username'] or "an anonymous user"
        )

        user = get_user(inputs['\\username'])
        active_jobs_count = Job.objects.filter(
            owner=user, status__in=(Job.ACCEPTED, Job.STARTED)
        ).count()

        if active_jobs_count >= MAX_ACTIVE_JOBS:
            message = (
                "Maximum number of allowed active asynchronous download "
                "requests exceeded!"
            )
            context.logger.warning("Job rejected! %s", message)
            raise ServerBusy(message)

        # create DB record for this WPS job
        job = Job()
        job.status = Job.ACCEPTED
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
        ("token_auth", RequestParameter(
            lambda request: getattr(request, 'token_authentication', False)
        )),
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

    def execute(self, token_auth, collection_ids, begin_time,
                end_time, bbox, filters, output, context=None, **kwargs):
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
            settings, 'AEOLUS_EXTRACTION_SYNC_SPAN', timedelta(days=2)
        )
        async_span = getattr(
            settings, 'AEOLUS_EXTRACTION_ASYNC_SPAN', timedelta(weeks=4)
        )

        access_logger = self.get_access_logger(**kwargs)

        if isasync and async_span and time_span > async_span and not token_auth:
            message = '%s: Exceeding maximum allowed time span.' % (
                context.identifier,
            )
            access_logger.error(message)
            raise InvalidInputValueError('end_time', message)

        if not isasync and time_span > sync_span:
            message = '%s: Exceeding maximum allowed time span.' % (
                context.identifier,
            )
            access_logger.error(message)
            raise InvalidInputValueError('end_time', message)

        # log the request
        access_logger.info(
            "%s: request parameters: toi: (%s, %s), bbox: %s, "
            "collections: (%s), filters: %s, type: %s",
            context.identifier,
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

        try:
            collection_products = self.get_collection_products(
                collection_ids, db_filters, kwargs["username"]
            )
        except PermissionDenied as error:
            raise InvalidInputValueError('collection_ids', str(error)) from error

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
        # Setting default size limit to 1GB
        file_size_limit = getattr(
            settings, 'AEOLUS_DOWNLOAD_SIZE_LIMIT', 1000000000
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
            access_logger.info(
                "%s: response: count: %d files, mime-type: %s, fields: %s",
                context.identifier,
                total_product_count, mime_type, json.dumps(fields_for_logging)
            )

            return CDObject(
                encoded, filename=out_filename, **output
            )

        if mime_type == 'application/netcdf':
            if not isasync:
                uid = str(uuid4())
                tmppath = os.path.join(tempfile.gettempdir(), uid) + '.nc'
            else:
                tmppath = out_filename

            product_count = 0
            identifiers = []
            baselines = []
            software_vers = []

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
                            baselines.append(get_mph(product)["baseline"])
                            software_vers.append(get_mph(product)["software_ver"])

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

                            if file_size_limit is not None:
                                ds.sync()
                                if os.path.getsize(tmppath) > file_size_limit:
                                    raise Exception(
                                        'Downloadfile is exceeding maximum '
                                        'allowed size'
                                    )

                    ds.history = json.dumps({
                        'inputFiles': identifiers,
                        'baselines': baselines,
                        'software_vers': software_vers,
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
            access_logger.info(
                "%s: response: count: %d files, mime-type: %s, fields: %s",
                context.identifier,
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

        raise InvalidOutputDefError(
            'output', "Unexpected output format %r requested!" % mime_type
        )

    def get_db_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        db_filters = dict(
            begin_time__lte=end_time,
            end_time__gte=begin_time,
        )

        if bbox:
            tpl_box = (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            box = Polygon.from_bbox(tpl_box)

            db_filters['footprint_homogenized__intersects'] = box

        if self.range_type_name:
            db_filters['product_type__name'] = self.range_type_name

        return db_filters

    def get_data_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        return filters

    def _find_collection(self, identifier, user):
        # find collection for identifier
        collection = Collection.objects.get(identifier=identifier)
        if user.has_perm("coverages.access_%s" % collection.identifier):
            # if user has permission return this collection
            return collection

        # if user does not have permission check for _public collection
        try:
            p_collection = Collection.objects.get(identifier=identifier+"_public")
        except Collection.DoesNotExist:
            raise PermissionDenied(
                "No access to '%s' permitted" % collection.identifier
            )

        if user.has_perm("coverages.access_%s" % p_collection.identifier):
            return p_collection

        raise PermissionDenied(
            "No access to '%s' permitted" % collection.identifier
        )

    def get_collection_products(self, collection_ids, db_filters, username):
        user = get_user(username)
        if not user:
            raise PermissionDenied("Not logged in")
        collections = [
            self._find_collection(identifier, user)
            for identifier in collection_ids.data
        ]

        add_homogenized = any(
            lookup.startswith('footprint_homogenized')
            for lookup in db_filters.keys()
        )

        collection_products = []
        for collection in collections:
            qs = Product.objects.all()
            if add_homogenized:
                qs = qs.annotate(
                    footprint_homogenized=Func(
                        F('footprint'),
                        function='ST_CollectionHomogenize'
                    )
                )

            qs = qs.filter(
                collections=collection,
                **db_filters
            ).order_by('begin_time')

            collection_products.append((collection, qs))

        return collection_products

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

                    values = stringtochar(numpy.array([
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
