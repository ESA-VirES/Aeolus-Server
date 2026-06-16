#-------------------------------------------------------------------------------
#
# Import Aeolus coverage types
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
from traceback import print_exc
from django.db import transaction
from eoxserver.resources.coverages.models import CoverageType, FieldType
from aeolus.data import ALBEDO_COVERAGE_TYPE as DEFAULT_TYPES
from .._common import Subcommand


class ImportCoverageTypeSubcommand(Subcommand):
    name = "import"
    help = "Import Aeolus coverage types from a JSON file."

    def add_arguments(self, parser):
        parser.add_argument(
            "-f", "--file", dest="filename", default="-", help=(
                "Optional input JSON file-name. "
            )
        )
        parser.add_argument(
            "-d", "--default", dest="load_defaults", action="store_true",
            default=False, help="Import default coverage types."
        )

    def handle(self, **kwargs):
        if kwargs["load_defaults"]:
            filename = DEFAULT_TYPES
        else:
            filename = kwargs["filename"]
        with sys.stdin if filename == "-" else open(filename, "rb") as file_:
            self.save_coverage_types(json.load(file_), **kwargs)

    def save_coverage_types(self, data, **kwargs):
        total_count = 0
        failed_count = 0
        created_count = 0
        updated_count = 0

        for item in data:
            identifier = item.get("name")
            try:
                is_updated = save_coverage_type(item, logger=self.logger)
            except Exception as error:
                failed_count += 1
                if kwargs.get("traceback"):
                    print_exc(file=sys.stderr)
                self.error(
                    "Failed to create or update coverage type %s! %s",
                    identifier, error
                )
            else:
                updated_count += is_updated
                created_count += not is_updated
                self.logger.info(
                    "coverage type %s updated" if is_updated else
                    "coverage type %s created", identifier
                )
            finally:
                total_count += 1

        if created_count or total_count == 0:
            self.info(
                "%d of %d coverage type%s created.", created_count, len(data),
                "s" if created_count != 1 else ""
            )

        if updated_count:
            self.info(
                "%d of %d coverage type%s updated.", updated_count, len(data),
                "s" if updated_count != 1 else ""
            )

        if failed_count:
            self.info(
                "%d of %d coverage type%s failed ", failed_count, len(data),
                "s" if failed_count != 1 else ""
            )
        sys.exit(failed_count)


@transaction.atomic
def save_coverage_type(data, logger):

    identifier = data.get("name")
    if not identifier:
        raise ValueError("Missing coverage type name!")

    field_types = data.get("fieldTypes") or []

    is_updated, coverage_type = get_coverage_type(identifier)
    coverage_type.save()

    index = -1
    for field_type_definition in field_types:
        index += 1
        save_field_type(
            coverage_type,
            index,
            field_type_definition,
            logger=logger
        )
    remove_extra_field_types(
        coverage_type,
        index,
        logger=logger
    )

    return is_updated


def get_coverage_type(identifier):
    try:
        return True, CoverageType.objects.get(name=identifier)
    except CoverageType.DoesNotExist:
        return False, CoverageType(name=identifier)


DATA_TYPES = {
    "uint8": {"signed": False, "isFloat": False, "numbits": 8},
    "uint16": {"signed": False, "isFloat": False, "numbits": 16},
    "uint32": {"signed": False, "isFloat": False, "numbits": 32},
    "uint64": {"signed": False, "isFloat": False, "numbits": 64},
    "int8": {"signed": True, "isFloat": False, "numbits": 8},
    "int16": {"signed": True, "isFloat": False, "numbits": 16},
    "int32": {"signed": True, "isFloat": False, "numbits": 32},
    "int64": {"signed": True, "isFloat": False, "numbits": 64},
    "float32": {"signed": True, "isFloat": True, "numbits": 32},
    "float64": {"signed": True, "isFloat": True, "numbits": 64},
}

def save_field_type(coverage_type, index, data, logger):

    is_updated, field_type = get_field_type(
        coverage_type, index, data["identifier"]
    )

    field_type.description = data.get("description")
    field_type.definition = data.get("definition")
    field_type.unit_of_measure = data.get("uom")
    field_type.wavelength = data.get("wavelength")
    field_type.significant_figures = data.get("significantFigures")

    data_type = DATA_TYPES[data["dataType"]]

    field_type.numbits = data_type["numbits"]
    field_type.signed = data_type["signed"]
    field_type.is_float = data_type["isFloat"]

    field_type.full_clean()
    field_type.save()

    # TODO support for nill and allowed values

    logger.info(
        "field type %s[%d].%s %s",
        coverage_type.name,
        index,
        field_type.identifier,
        "updated" if is_updated else "created"
    )

def remove_extra_field_types(coverage_type, max_index, logger):
    for field_type in FieldType.objects.filter(
        coverage_type=coverage_type, index__gt=max_index,
    ):
        field_type.delete()
        logger.info(
            "field type %s[%d].%s removed",
            coverage_type.name,
            field_type.index,
            field_type.identifier,
        )


def get_field_type(coverage_type, index, identifier):
    try:
        field_type = FieldType.objects.get(
            coverage_type=coverage_type,
            index=index,
        )
        field_type.identifier = identifier
        return True, field_type
    except FieldType.DoesNotExist:
        return False, FieldType(
            coverage_type=coverage_type,
            index=index,
            identifier=identifier,
        )
