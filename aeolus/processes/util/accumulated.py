# ------------------------------------------------------------------------------
#
#  Base class for processes dealing with accumulated data (Level 2B/2C)
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
from collections import defaultdict

from django.conf import settings
from django.utils.timezone import utc
from django.contrib.gis.geos import Polygon
from eoxserver.services.ows.wps.parameters import (
    ComplexData, FormatJSON, CDObject, BoundingBoxData, LiteralData,
    FormatBinaryRaw, CDFile, Reference, RequestParameter
)
from eoxserver.services.ows.wps.exceptions import (
    ServerBusy,
)
import msgpack
from netCDF4 import Dataset

from aeolus import models
from aeolus.processes.util.bbox import translate_bbox
from aeolus.processes.util.context import DummyContext
from aeolus.processes.util.auth import get_user, get_username


MAX_ACTIVE_JOBS = 2


class AccumulatedDataExctractProcessBase(object):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level2C products of the specified collections.
    """

    # Override these values in the subclasses
    extraction_function = None
    level_name = None

    synchronous = True
    asynchronous = True

    # common inputs/outputs to satisfy ProcessInterface
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
        ("mie_grouping_fields", LiteralData(
            'mie_grouping_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("mie_profile_fields", LiteralData(
            'mie_profile_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("mie_wind_fields", LiteralData(
            'mie_wind_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("rayleigh_grouping_fields", LiteralData(
            'rayleigh_grouping_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("rayleigh_profile_fields", LiteralData(
            'rayleigh_profile_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("rayleigh_wind_fields", LiteralData(
            'rayleigh_wind_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
        ("measurement_fields", LiteralData(
            'measurement_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
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

        isasync = context is not None
        context = context or DummyContext()
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

        # input parsing
        if kwargs['mie_grouping_fields']:
            mie_grouping_fields = kwargs['mie_grouping_fields'].split(',')
        else:
            mie_grouping_fields = []

        if kwargs['mie_profile_fields']:
            mie_profile_fields = kwargs['mie_profile_fields'].split(',')
        else:
            mie_profile_fields = []

        if kwargs['mie_wind_fields']:
            mie_wind_fields = kwargs['mie_wind_fields'].split(',')
        else:
            mie_wind_fields = []

        if kwargs['rayleigh_grouping_fields']:
            rayleigh_grouping_fields = kwargs['rayleigh_grouping_fields'].split(',')
        else:
            rayleigh_grouping_fields = []

        if kwargs['rayleigh_profile_fields']:
            rayleigh_profile_fields = kwargs['rayleigh_profile_fields'].split(',')
        else:
            rayleigh_profile_fields = []

        if kwargs['rayleigh_wind_fields']:
            rayleigh_wind_fields = kwargs['rayleigh_wind_fields'].split(',')
        else:
            rayleigh_wind_fields = []

        if kwargs['measurement_fields']:
            measurement_fields = kwargs['measurement_fields'].split(',')
        else:
            measurement_fields = []

        # TODO: optimize this to make this in a single query
        collections = [
            models.ProductCollection.objects.get(identifier=identifier)
            for identifier in collection_ids.data
        ]

        db_filters = dict(
            begin_time__lte=end_time,
            end_time__gte=begin_time,
        )

        data_filters = dict(
            mie_profile_datetime_start={'min_value': begin_time},
            mie_profile_datetime_stop={'max_value': end_time},
            rayleigh_profile_datetime_start={'min_value': begin_time},
            rayleigh_profile_datetime_stop={'max_value': end_time},
            mie_wind_result_start_time={'min_value': begin_time},
            mie_wind_result_stop_time={'max_value': end_time},
            rayleigh_wind_result_start_time={'min_value': begin_time},
            rayleigh_wind_result_stop_time={'max_value': end_time},
            **(filters.data if filters else {})
        )

        if bbox:
            # TODO: assure that bbox is within -180,-90,180,90
            # TODO: when minlon > maxlon, make 2 bboxes
            tpl_box = (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])
            box = Polygon.from_bbox(tpl_box)

            db_filters['ground_path__intersects'] = box

            tpl_box = translate_bbox(tpl_box)
            data_filters['mie_profile_lon_of_DEM_intersection'] = {
                'min': tpl_box[0],
                'max': tpl_box[2],
            }
            data_filters['mie_profile_lat_of_DEM_intersection'] = {
                'min': tpl_box[1],
                'max': tpl_box[3],
            }
            data_filters['rayleigh_profile_lon_of_DEM_intersection'] = {
                'min': tpl_box[0],
                'max': tpl_box[2],
            }
            data_filters['rayleigh_profile_lat_of_DEM_intersection'] = {
                'min': tpl_box[1],
                'max': tpl_box[3],
            }
            data_filters['mie_wind_result_COG_longitude'] = {
                'min': tpl_box[0],
                'max': tpl_box[2],
            }
            data_filters['mie_wind_result_COG_latitude'] = {
                'min': tpl_box[1],
                'max': tpl_box[3],
            }
            data_filters['rayleigh_wind_result_COG_longitude'] = {
                'min': tpl_box[0],
                'max': tpl_box[2],
            }
            data_filters['rayleigh_wind_result_COG_latitude'] = {
                'min': tpl_box[1],
                'max': tpl_box[3],
            }

        # setup products for each collection, and hash their respective
        # product counts
        collection_products = [
            (collection, models.Product.objects.filter(
                collections=collection,
                **db_filters
            ).order_by('begin_time'))
            for collection in collections
        ]

        collection_product_counts = dict(
            (collection.identifier, products.count())
            for collection, products in collection_products
        )
        total_product_count = sum(collection_product_counts.values())

        mime_type = output['mime_type']

        # create the iterator: yielding collection + sub iterators
        # the sub iterators iterate over all data files and yield the selected
        # and filtered fields
        out_data_iterator = (
            (collection, self.extraction_function([
                product.data_items.filter(semantic__startswith='bands')
                .first().location
                for product in products
            ], data_filters,
                mie_grouping_fields=mie_grouping_fields,
                mie_profile_fields=mie_profile_fields,
                mie_wind_fields=mie_wind_fields,
                rayleigh_grouping_fields=rayleigh_grouping_fields,
                rayleigh_profile_fields=rayleigh_profile_fields,
                rayleigh_wind_fields=rayleigh_wind_fields,
                measurement_fields=measurement_fields,
                convert_arrays=(mime_type == 'application/msgpack'),
            ))
            for collection, products in collection_products
        )

        # output generation:

        # encode as messagepack
        if mime_type == 'application/msgpack':
            if isasync:
                raise Exception(
                    'messagepack format is only available for synchronous '
                    'process invocation.'
                )

            # serialize the nested iterators to dicts/lists for each collection
            out_data = {}
            for collection, data_iterator in out_data_iterator:
                accumulated_data = [
                    defaultdict(list),
                    defaultdict(list),
                    defaultdict(list),
                    defaultdict(list),
                    defaultdict(list),
                    defaultdict(list),
                    defaultdict(list),
                ]

                for data_kinds in data_iterator:
                    for data_kind, acc in zip(data_kinds, accumulated_data):
                        for field, values in data_kind.items():
                            acc[field].extend(values)

                collection_data = dict(
                    mie_grouping_data=accumulated_data[0],
                    rayleigh_grouping_data=accumulated_data[1],
                    mie_profile_data=accumulated_data[2],
                    rayleigh_profile_data=accumulated_data[3],
                    mie_wind_data=accumulated_data[4],
                    rayleigh_wind_data=accumulated_data[5],
                    measurement_data=accumulated_data[6],
                )
                out_data[collection.identifier] = collection_data

            encoded = StringIO(msgpack.dumps(out_data))
            return CDObject(
                encoded, filename="level_%s_data.mp" % self.level_name, **output
            )

        elif mime_type == 'application/netcdf':
            if isasync:
                outpath = "level_%s_data.nc" % self.level_name
            else:
                uid = str(uuid4())
                outpath = os.path.join(tempfile.gettempdir(), uid) + '.nc'

            product_count = 0

            try:
                with Dataset(outpath, "w", format="NETCDF4") as ds:
                    for collection, data_iterator in out_data_iterator:
                        enumerated_data = enumerate(data_iterator, start=1)
                        for product_idx, file_data in enumerated_data:
                            file_data = dict(
                                mie_grouping_data=file_data[0],
                                rayleigh_grouping_data=file_data[1],
                                mie_profile_data=file_data[2],
                                rayleigh_profile_data=file_data[3],
                                mie_wind_data=file_data[4],
                                rayleigh_wind_data=file_data[5],
                                measurement_data=file_data[6],
                            )

                            for kind_name, kind in file_data.items():
                                if not kind:
                                    continue

                                # get or create the group and a simple dimension
                                # for the data kind
                                group = ds.createGroup(kind_name)
                                if kind_name not in group.dimensions:
                                    group.createDimension(kind_name, None)

                                # iterate over the actual data from each kind
                                for name, values in kind.items():
                                    # if the variable does not yet exist,
                                    # create it
                                    if name not in group.variables:
                                        group.createVariable(
                                            name, '%s%i' % (
                                                values.dtype.kind,
                                                values.dtype.itemsize
                                            ), kind_name
                                        )[:] = values
                                    # if the variable already exists, append
                                    # data to it
                                    else:
                                        var = group[name]
                                        offset = var.shape[0]
                                        end = offset + values.shape[0]
                                        var[offset:end] = values

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
                outpath, filename="level_%s_data.nc" % self.level_name,
                remove_file=True, **output
            )
