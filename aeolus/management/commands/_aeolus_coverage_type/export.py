#-------------------------------------------------------------------------------
#
# Export Aeolus coverage types
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
from eoxserver.resources.coverages.models import FieldType
from .._common import JSON_OPTS
from .common import CoverageTypeSelectionSubcommand


class ExportCoverageTypeSubcommand(CoverageTypeSelectionSubcommand):
    name = "export"
    help = "Export Aeolus coverage types"

    description = "Export registered coverage types in JSON format."

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-f", "--file", dest="filename", default="-", help=(
                "Optional file-name the output is written to. "
                "By default it is written to the standard output."
            )
        )

    def handle(self, **kwargs):
        data = [
            serialize_coverage_type(coverage_type)
            for coverage_type in self.select_coverage_types(**kwargs)
        ]
        filename = kwargs["filename"]
        with (sys.stdout if filename == "-" else open(filename, "w", encoding="utf-8")) as file_:
            json.dump(data, file_, **JSON_OPTS)


def serialize_coverage_type(coverage_type):
    """ Serialize coverage type object. """
    return {
        "name": coverage_type.name,
        "fieldTypes": serialize_field_types(coverage_type),
    }


def serialize_field_types(coverage_type):
    return [
        serialize_field_type(field_type) for field_type in (
            FieldType.objects
            .filter(coverage_type=coverage_type)
            .order_by("index")
        )
    ]


DATA_TYPES = {
    (False, False, 8): "uint8",
    (False, False, 16): "uint16",
    (False, False, 32): "uint32",
    (False, False, 64): "uint64",
    (True, False, 8): "int8",
    (True, False, 16): "int16",
    (True, False, 32): "int32",
    (True, False, 64): "int64",
    (True, True, 32): "float32",
    (True, True, 64): "float64",
}


def serialize_field_type(field_type):
    data_type = DATA_TYPES[(
        field_type.signed,
        field_type.is_float,
        field_type.numbits,
    )]
    return _remove_empty_fields({
        "identifier": field_type.identifier,
        "dataType": data_type,
        "description": field_type.description,
        "definition": field_type.definition,
        "uom": field_type.unit_of_measure,
        "wavelength": field_type.wavelength,
        "significantFigures": field_type.significant_figures,
    })


def _remove_empty_fields(data):
    return {
        key: value
        for key, value in data.items()
        if value is not None
    }
