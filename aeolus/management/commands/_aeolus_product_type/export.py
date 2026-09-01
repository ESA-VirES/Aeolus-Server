#-------------------------------------------------------------------------------
#
# Export Aeolus product types
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
from .._common import JSON_OPTS
from .common import ProductTypeSelectionSubcommand


class ExportProductTypeSubcommand(ProductTypeSelectionSubcommand):
    name = "export"
    help = "Export Aeolus product types"

    description = "Export registered product types in JSON format."

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
            serialize_product_type(product_type)
            for product_type in self.select_product_types(**kwargs)
        ]
        filename = kwargs["filename"]
        with (sys.stdout if filename == "-" else open(filename, "w", encoding="utf-8")) as file_:
            json.dump(data, file_, **JSON_OPTS)


def serialize_product_type(product_type):
    """ Serialize product type object. """
    return {
        "name": product_type.name,
        "allowedCoverageTypes": [
            coverage_type.name
            for coverage_type in product_type.allowed_coverage_types.all()
        ],
    }
