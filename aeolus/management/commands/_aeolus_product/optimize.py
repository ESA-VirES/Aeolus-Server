#-------------------------------------------------------------------------------
#
# Optimize Aeolus products
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
# pylint: disable=missing-docstring,too-many-arguments,too-many-positional-arguments
# pylint: disable=too-many-branches,too-many-locals,broad-exception-caught,line-too-long

import os
import sys
import hashlib
from dataclasses import dataclass
from traceback import print_exc
from eoxserver.resources.coverages.models import Product
from aeolus.optimize import optimize_aeolus_product
from aeolus.management.api.product import (
    DEF_OUTPUT_DIR_TEMPLATE,
    get_optimized_product_filename,
    link_optimized_data_file_to_product,
)
from .common import ProductSelectionSubcommandProtected

DEF_CHUNK_SIZE = 1048576 # 1MB
DEF_DIGEST_TYPE = "md5"


class OptimizeProductSubcommand(ProductSelectionSubcommandProtected):
    name = "optimize"
    help = "Optimize Aeolus products."

    SELECT_RELATED = ["optimized_data_item"]
    PREFETCH_RELATED = ["product_data_items"]

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-d", "--directory", "--output-directory",
            required=(DEF_OUTPUT_DIR_TEMPLATE is None),
            default=DEF_OUTPUT_DIR_TEMPLATE,
            dest="output_directory_template",
            help="Output directory template.",
        )
        parser.add_argument(
            "-f", "--force", "--force-rewrite",
            dest="force_rewrite", action="store_true", default=False,
            help="Force rewrite if the target file already exists."
        )
        parser.add_argument(
            "--do-not-link", dest="do_not_link", action="store_true",
            default=False,
            help="Do not link the target optimized data file."
        )
        parser.add_argument(
            "--link-only", dest="link_only", action="store_true",
            default=False,
            help=(
                "Skip creation of new optimized data files. "
                "Use on a read-only file-system."
            )
        )
        parser.add_argument(
            "--no-checksum", dest="digest_type", action="store_const",
            const=None, default=DEF_DIGEST_TYPE, help="Calculate no checksum.",
        )
        parser.add_argument(
            "--md5", dest="digest_type", action="store_const", const="md5",
            help="Calculate MD5 checksum.",
        )

    def handle(self, **kwargs):
        objects = self.select_products(Product.objects.all(), **kwargs)
        self.optimize_products(objects, **kwargs)

    def optimize_products(self, products, output_directory_template,
                          force_rewrite=False, do_not_link=False,
                          link_only=False, traceback=False, digest_type=None,
                          **kwargs):
        del kwargs

        counter = Counter()

        for product in products:
            for collection in product.collections.all():
                counter.total += 1
                output_filename = get_optimized_product_filename(
                    collection, product, output_directory_template,
                )
                output_exists = os.path.exists(output_filename)

                if output_exists:
                    self.logger.debug(
                        "existing optimized data file %s found",
                         output_filename
                    )

                if not link_only and (force_rewrite or not output_exists):

                    if output_exists:
                        self.logger.debug(
                            "rewriting existing optimized data file %s",
                            output_filename
                        )

                    input_filename = product.product_data_items.get().location

                    try:
                        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
                        optimize_aeolus_product(
                            input_filename, output_filename, logger=self.logger
                        )
                    except Exception as error:
                        counter.failed += 1
                        if traceback:
                            print_exc(file=sys.stderr)
                        self.logger.error(
                            "Failed create a new optimized data file %s! %s",
                            output_filename, error
                        )
                        continue
                    else:
                        counter.created += 1

                    if output_exists:
                        self.logger.info(
                            "existing optimized data file %s rewritten",
                            output_filename
                        )
                    else:
                        output_exists = True
                        self.logger.info(
                            "new optimized data file %s created",
                            output_filename
                        )

                    write_file_checksum(
                        filename=output_filename,
                        digest_type=digest_type,
                        logger=self.logger,
                    )

                elif output_exists:
                    counter.skipped += 1

                if not do_not_link and output_exists:
                    try:
                        linked = link_optimized_data_file_to_product(
                            product=product,
                            location=output_filename,
                            format="application/netcdf",
                            logger=self.logger,
                        )
                    except Exception as error:
                        counter.link_failed += 1
                        if traceback:
                            print_exc(file=sys.stderr)
                        self.logger.error(
                            "Failed to link optimized data file %s to product %s! %s",
                            output_filename, product.identifier, error
                        )
                        continue
                    else:
                        if linked:
                            counter.linked += 1

        counter.print_report(lambda msg: print(msg, file=sys.stderr))

        sys.exit(counter.total_failed > 0)


def write_file_checksum(filename, digest_type, logger):

    if not digest_type:
        logger.debug("Checksum calculation skipped.")
        return

    with open(filename, "rb") as file:
        checksum = calculate_file_checksum(file, digest_type)

    checksum_filename = f"{filename}.{digest_type}"
    with open(checksum_filename, "w", encoding="utf-8") as file:
        print(f"{checksum} *{os.path.basename(filename)}", file=file)

    logger.info(f"{digest_type} checksum written to {checksum_filename}")


def calculate_file_checksum(file, digest_type=DEF_DIGEST_TYPE,
                            chunk_size=DEF_CHUNK_SIZE):
    digest = hashlib.new(digest_type)
    while True:
        chunk = file.read(chunk_size)
        if not chunk:
            break
        digest.update(chunk)
    return digest.hexdigest()


@dataclass
class Counter:
    total: int = 0              # total matched products
    created: int = 0            # number of newly created optimized products
    skipped: int = 0            # number of skipped existing optimized products
    failed: int = 0             # number of failed product optimizations
    linked: int = 0             # number of linked optimized products
    link_failed: int = 0        # number of failed

    @property
    def total_failed(self):
        return self.failed + self.link_failed

    def print_report(self, print_fcn):

        def _plural(value):
            return "s" if value != 1 else ""

        if self.created > 0 or self.total == 0:
            print_fcn(f"{self.created} of {self.total} product{_plural(self.total)} optimized.")

        if self.skipped > 0:
            print_fcn(f"{self.skipped} of {self.total} product{_plural(self.total)} optimization skipped.")

        if self.linked > 0:
            print_fcn(f"{self.linked} optimised data files newly linked to a product.")

        if self.link_failed > 0:
            print_fcn(f"{self.link_failed} optimised data files failed to linked to a product.")
