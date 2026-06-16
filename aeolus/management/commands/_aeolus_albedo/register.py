#-------------------------------------------------------------------------------
#
# Register Aeolus Albedo coverages
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
from typing import List
from dataclasses import dataclass
from django.db import transaction
from eoxserver.resources.coverages.models import (
    Collection, Coverage, CoverageType, collection_insert_eo_object,
)
from aeolus.registration import register_albedo
from aeolus.data import (
    ALBEDO_COVERAGE_ID_TEMPLATE,
    ALBEDO_COVERAGE_TYPE_ID,
    ALBEDO_COLLECTION_ID,
    ALBEDO_GRID_ID,
)
from .._common import Subcommand


class RegisterAlbedoSubcommand(Subcommand):
    name = "register"
    help = "Register Aeolus albedo products from the gives JSON specification."

    def add_arguments(self, parser):
        parser.add_argument(
            "albedo_spec",
            help="Input Albedo JSON sources specification.",
        )
        parser.add_argument(
            "-i", "--id-template",
            dest="id_template", default=ALBEDO_COVERAGE_ID_TEMPLATE,
            help="Optional coverage identifier template.",
        )
        parser.add_argument(
            "-c", "--collection",
            dest="collection_id", default=ALBEDO_COLLECTION_ID,
            help="Optional collection identifier.",
        )
        parser.add_argument(
            "-t", "--type", "--coverage-type",
            dest="coverage_type_id", default=ALBEDO_COVERAGE_TYPE_ID,
            help="Optional coverage type name.",
        )
        parser.add_argument(
            "-g", "--grid",
            dest="grid_id", default=ALBEDO_GRID_ID,
            help="Optional grid name.",
        )
        parser.add_argument(
            "-p", "--path", "--base-path",
            dest="base_path", default=None , help=(
                "Optional base path overriding the defaults."
            )
        )
        parser.add_argument(
            "--update", "--re-register", dest="ignore_registered",
            action="store_false", default=True, help=(
                "Update product record when the product is already registered.  "
                "By default, the registration is skipped."
            )
        )

    def handle(self, **kwargs):
        ignore_registered = kwargs["ignore_registered"]
        albedo_spec = AlbedoSources.load_from_json(
            kwargs["albedo_spec"], kwargs["base_path"]
        )
        collection = _get_collection(kwargs["collection_id"])
        coverage_type = _get_coverage_type(kwargs["coverage_type_id"])
        grid_name = kwargs["grid_id"]
        id_template = kwargs["id_template"]

        for year in range(albedo_spec.start_year, albedo_spec.end_year + 1):
            for dataset in albedo_spec.datasets:
                register_albedo_coverage(
                    collection=collection,
                    coverage_type=coverage_type,
                    grid_name=grid_name,
                    identifier=id_template.format(
                        year=year,
                        month=dataset.month,
                    ),
                    filename=dataset.path,
                    year=year,
                    month=dataset.month,
                    ignore_registered=ignore_registered,
                    logger=self.logger
                )

@dataclass
class Result:
    coverage: Coverage
    created: bool
    removed: List[str] # < Python 3.9
    linked_to_collection: List[str] # < Python 3.9


def register_albedo_coverage(
    collection, coverage_type, identifier, filename, year, month,
    grid_name, ignore_registered, logger,
):
    result = _register_albedo_coverage(
        collection=collection,
        coverage_type=coverage_type,
        identifier=identifier,
        filename=filename,
        year=year,
        month=month,
        grid_name=grid_name,
        ignore_registered=ignore_registered,
    )

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


@transaction.atomic
def _register_albedo_coverage(
    collection, coverage_type, identifier, filename, year, month,
    grid_name, ignore_registered,
):
    created = False
    removed = []
    linked_to_collection = []

    if not ignore_registered:
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

    if not coverage.collections.filter(pk=collection.pk).exists():
        collection_insert_eo_object(collection, coverage)
        linked_to_collection.append(collection.identifier)

    return Result(
        coverage=coverage,
        created=created,
        removed=removed,
        linked_to_collection=linked_to_collection,

    )


def _get_collection(identifier):
    try:
        return Collection.objects.get(identifier=identifier)
    except Collection.DoesNotExist:
        raise ValueError("Invalid collection identifier!") from None


def _get_coverage_type(name):
    try:
        return CoverageType.objects.get(name=name)
    except CoverageType.DoesNotExist:
        raise ValueError("Invalid coverage type name!") from None


def _remove_existing_coverage(identifier):
    count, _ = Coverage.objects.filter(identifier=identifier).delete()
    return count > 0


def _get_existing_coverage(identifier):
    try:
        return Coverage.objects.get(identifier=identifier)
    except Coverage.DoesNotExist:
        return None


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
