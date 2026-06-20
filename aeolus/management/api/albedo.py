#-------------------------------------------------------------------------------
#
# Aeolus Albedo coverage registration
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
# pylint: disable=missing-docstring, too-few-public-methods

import sys
import json
import os.path
from dataclasses import dataclass
from typing import List
from django.db import transaction
from eoxserver.resources.coverages.models import (
#    Collection, Coverage, CoverageType,
    Coverage,
    collection_insert_eo_object,
    collection_collect_metadata,
    ManagementError,
)
from aeolus.registration import register_albedo


@dataclass
class AlbedoSource:
    month: int
    path: str

    @classmethod
    def parse_from_dict(cls, data, base_directory=None):
        if base_directory:
            base_directory = os.path.abspath(base_directory)
        month = int(data["month"])
        if month < 1 or month > 12:
            raise ValueError("Invalid month value.")
        path = os.path.normpath(os.path.join(
            base_directory or os.getcwd(), data["path"]
        ))
        return cls(month, path)


@dataclass
class AlbedoSources:
    start_year: int
    end_year: int
    datasets: List[AlbedoSource] # < Python 3.9

    @classmethod
    def load_from_json(cls, filename, base_directory=None):
        if filename == "-":
            return cls.parse_from_dict(
                json.load(sys.stdin),
                base_directory=base_directory
            )
        with open(filename, "rb") as file:
            return cls.parse_from_dict(
                json.load(file),
                base_directory=(
                    base_directory or os.path.dirname(filename)
                )
            )

    @classmethod
    def parse_from_dict(cls, data, base_directory=None):
        if data["type"] != "AlbedoSources":
            raise ValueError("Invalid Albedo specification.")
        start_year = int(data["years"]["start"])
        end_year = int(data["years"]["end"])
        if end_year < start_year:
            raise ValueError("Invalid Albedo specification.")
        return cls(
            start_year,
            end_year,
            datasets = [
                AlbedoSource.parse_from_dict(item, base_directory=base_directory)
                for item in data["monthlyDatasets"]
            ],
        )


@dataclass
class Result:
    """ Results of the Aeolus coverage registration. """
    coverage: Coverage
    created: bool
    removed: List[str] # < Python 3.9
    linked_to_collection: List[str] # < Python 3.9


def register_albedo_coverage(
    collection, coverage_type, identifier, filename, year, month, grid_name,
    update_existing=False,
    defer_collection_update=False,
    allowed_coverage_types=None,
    logger=None
):
    def _register_albedo_coverage():
        created = False
        removed = []
        linked_to_collection = []

        if update_existing:
            if _remove_existing_coverage(identifier):
                removed.append(identifier)
            coverage = None
        else:
            coverage = _get_existing_coverage(identifier)

        if not coverage:
            coverage = register_albedo(
                coverage_type=coverage_type,
                grid_name=grid_name,
                identifier=identifier,
                filename=filename,
                year=year,
                month=month,
                replace=False,
            )
            created = True

        if allowed_coverage_types is not None and coverage.coverage_type:
            if coverage.coverage_type.name not in allowed_coverage_types:
                raise ManagementError(
                    f"Coverage {coverage.identifier} is of invalid coverage type "
                    f"{coverage.coverage_type.name}!"
                  )

        if not coverage.collections.filter(pk=collection.pk).exists():
            if defer_collection_update:
                collection.coverages.add(coverage)
            else:
                collection_insert_eo_object(
                    collection,
                    coverage,
                    use_extent=True,
                )
                collection_collect_metadata(
                    collection,
                    collect_footprint=False,
                    collect_begin_time=False,
                    collect_end_time=False,
                    product_summary=False,
                    coverage_summary=True,
                )
            linked_to_collection.append(collection.identifier)

        return Result(
            coverage=coverage,
            created=created,
            removed=removed,
            linked_to_collection=linked_to_collection,
        )

    with transaction.atomic():
        result = _register_albedo_coverage()

    for coverage_id in result.removed:
        logger.info("coverage %s deregistered", coverage_id)

    if result.created:
        logger.info("coverage %s registered", identifier)
    else:
        logger.debug("coverage %s exists", identifier)

    for collection_id in result.linked_to_collection:
        logger.info(
            "coverage %s linked to collection %s",
            identifier, collection_id,
        )

    return result


def _remove_existing_coverage(identifier):
    count, _ = Coverage.objects.filter(identifier=identifier).delete()
    return count > 0


def _get_existing_coverage(identifier):
    try:
        return Coverage.objects.get(identifier=identifier)
    except Coverage.DoesNotExist:
        return None
