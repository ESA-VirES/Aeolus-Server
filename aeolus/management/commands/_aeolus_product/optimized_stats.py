#-------------------------------------------------------------------------------
#
# Get overview of the linked Aeolus optimized data files.
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

import os
import sys
from dataclasses import dataclass
from eoxserver.resources.coverages.models import Product
from aeolus.management.api.product import (
    DEF_OUTPUT_DIR_TEMPLATE,
    get_optimized_product_filename,
    get_optimized_data_item,
)
from .._aeolus_product.common import ProductSelectionSubcommand


class OptimizedStatsProductSubcommand(ProductSelectionSubcommand):
    name = "optimized_stats"
    help = "Overview of the Aeolus product optimization"

    SELECT_RELATED = ["product_type", "optimized_data_item"]
    PREFETCH_RELATED = ["collections"]

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--optimized-directory",
            required=(DEF_OUTPUT_DIR_TEMPLATE is None),
            default=DEF_OUTPUT_DIR_TEMPLATE,
            dest="optimized_directory_template",
            help="Optimized directory template.",
        )

    def handle(self, **kwargs):
        counter = Counter()
        optimized_directory_template = kwargs["optimized_directory_template"]

        products = self.select_products(Product.objects.all(), **kwargs)
        for product in products:
            for collection in product.collections.all():
                self._update_counter(
                    counter, collection, product, optimized_directory_template,
                )

        counter.print_report(lambda msg: print(msg, file=sys.stderr))

    def _update_counter(self, counter, collection, product, directory_template):
        counter.total += 1
        data_item = get_optimized_data_item(product)
        if data_item:
            counter.optimized += 1
            if not os.path.exists(data_item.location):
                counter.optimized_file_missing += 1
        else:
            counter.not_optimized += 1
            filename = get_optimized_product_filename(
                collection, product, directory_template=directory_template,
            )
            if os.path.exists(filename):
                counter.not_optimized_file_exists += 1

@dataclass
class Counter:
    total: int = 0
    optimized: int = 0
    optimized_file_missing: int = 0
    not_optimized: int = 0
    not_optimized_file_exists: int = 0

    def print_report(self, print_fcn):

        def _plural(value):
            return "s" if value != 1 else ""

        if self.optimized > 0 or self.total == 0:
            print_fcn(f"{self.optimized} of {self.total} product{_plural(self.total)} optimized.")

        if self.not_optimized > 0:
            print_fcn(
                f"{self.not_optimized} of {self.total} "
                f"product{_plural(self.total)} not optimized."
            )

        if self.optimized_file_missing > 0 or self.total == 0:
            print_fcn(
                f"{self.optimized_file_missing} of {self.optimized} "
                f"optimized products{_plural(self.optimized)} missing the data file!"
            )

        if self.not_optimized_file_exists > 0 or self.total == 0:
            print_fcn(
                f"{self.not_optimized_file_exists} of {self.not_optimized} "
                f"not optimized products{_plural(self.not_optimized)} "
                "unlinked data file found."
            )
