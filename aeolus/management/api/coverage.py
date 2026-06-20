#-------------------------------------------------------------------------------
#
# Aeolus - coverage operations
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

import logging
from os.path import splitext, basename
from dataclasses import dataclass
from typing import List
from django.db import transaction
from eoxserver.resources.coverages.models import (
    Collection,
    Coverage,
    CoverageType,
    collection_insert_eo_object,
    collection_collect_metadata,
    ManagementError,
)
from .eo_object import deregister_object


def get_coverage_type(name):
    """ Get CoverageType object. """
    try:
        return CoverageType.objects.get(name=name)
    except CoverageType.DoesNotExist:
        raise ValueError("Invalid coverage type name!") from None


def get_coverage_collection(identifier):
    """ Get product collection by identifier. """
    try:
        return (
            Collection.objects
            .prefetch_related("collection_type__allowed_coverage_types")
            .get(identifier=identifier)
        )
    except Collection.DoesNotExist:
        raise ValueError("Invalid collection identifier!") from None


def get_allowed_coverage_types(collection):
    """ Read allowed coverage types from a given coverage collection. """
    collection_type = collection.collection_type
    if collection_type:
        return set(
            coverage_type.name
            for coverage_type in collection_type.allowed_coverage_types.all()
        )
    return None


def update_coverage_collection(collection, logger):
    """ Update coverage collection metadata after coverage change. """
    with transaction.atomic():
        collection_collect_metadata(
            collection,
            collect_footprint=True,
            collect_begin_time=True,
            collect_end_time=True,
            use_extent=True,
            product_summary=False,
            coverage_summary=True,
        )
    logger.info("collection %s updated", collection.identifier)


def deregister_coverage(coverage, logger, update_collections=True):
    """ Deregister Aeolus coverage. """
    with transaction.atomic():
        result = deregister_object(
            coverage,
            logger=logger,
            update_collections=update_collections,
            use_extent=True,
            product_summary=False,
            coverage_summary=True,
        )
    logger.info("coverage %s deregistered", coverage.identifier)
    return result
