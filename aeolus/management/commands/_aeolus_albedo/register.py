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
from traceback import print_exc
from dataclasses import dataclass
from aeolus.data import (
    ALBEDO_COVERAGE_ID_TEMPLATE,
    ALBEDO_COVERAGE_TYPE_ID,
    ALBEDO_COLLECTION_ID,
    ALBEDO_GRID_ID,
)
from aeolus.management.api.coverage import (
    get_coverage_type,
    get_coverage_collection,
    get_allowed_coverage_types,
    update_coverage_collection,
)
from aeolus.management.api.albedo import (
    AlbedoSources,
    register_albedo_coverage,
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
        parser.add_argument(
            "--defer-collection-update", dest="defer_collection_update",
            action="store_true", default=True,
            help="Defer collection updates once all objects are inserted."
        )
        parser.add_argument(
            "--instant-collection-update", dest="defer_collection_update",
            action="store_false",
            help="Perform collection when the objects are inserted."
        )

    def handle(self, **kwargs):
        update_existing = not kwargs["ignore_registered"]
        albedo_spec = AlbedoSources.load_from_json(
            kwargs["albedo_spec"], kwargs["base_path"]
        )
        collection = get_coverage_collection(kwargs["collection_id"])
        allowed_coverage_types = get_allowed_coverage_types(collection)
        coverage_type = get_coverage_type(kwargs["coverage_type_id"])
        grid_name = kwargs["grid_id"]
        id_template = kwargs["id_template"]
        defer_collection_update = kwargs["defer_collection_update"]

        if allowed_coverage_types is not None:
            if coverage_type.name not in allowed_coverage_types:
                raise ValueError(f"Invalid coverage type {coverage_type.name}")

        counter = Counter()

        for year in range(albedo_spec.start_year, albedo_spec.end_year + 1):
            for dataset in albedo_spec.datasets:
                coverage_id = id_template.format(year=year, month=dataset.month)
                try:
                    result = register_albedo_coverage(
                        collection=collection,
                        coverage_type=coverage_type,
                        grid_name=grid_name,
                        identifier=coverage_id,
                        filename=dataset.path,
                        year=year,
                        month=dataset.month,
                        update_existing=update_existing,
                        defer_collection_update=defer_collection_update,
                        allowed_coverage_types=allowed_coverage_types,
                        logger=self.logger
                    )
                except Exception as error:
                    if kwargs.get("traceback"):
                        print_exc(file=sys.stderr)
                    self.error(
                        "Registration of coverage %s/%s failed! %s",
                        collection.identifier, coverage_id, error
                    )
                    collection.refresh_from_db()
                    counter.failed += 1
                    result = None

                else:
                    counter.removed += len(result.removed)
                    if result.created:
                        counter.inserted += 1
                    else:
                        counter.skipped += 1
                finally:
                    counter.total += 1

        if defer_collection_update:
            try:
                update_coverage_collection(collection, logger=self.logger)
            except Exception as error:
                if kwargs.get("traceback"):
                    print_exc(file=sys.stderr)
                self.error(
                    "Failed to update collection %s! %s",
                    collection.identifier, error
                )

        counter.print_report(lambda msg: print(msg, file=sys.stderr))

        sys.exit(counter.failed > 0)


@dataclass
class Counter:
    total: int = 0
    inserted: int = 0
    #updated: int = 0
    removed: int = 0
    skipped: int = 0
    failed: int = 0

    def print_report(self, print_fcn):

        def _plural(value):
            return "s" if value != 1 else ""

        if self.inserted > 0 or self.total == 0:
            print_fcn(f"{self.inserted} of {self.total} coverage{_plural(self.total)} registered.")

        #if self.updated > 0:
        #    print_fcn(f"{self.updated} of {self.total} coverage{_plural(self.total)} updated.")

        if self.skipped > 0:
            print_fcn(f"{self.skipped} of {self.total} coverage{_plural(self.total)} skipped.")

        if self.removed > 0:
            print_fcn(f"{self.removed} coverage{_plural(self.removed)} de-registered.")

        if self.failed > 0:
            print_fcn(
                f"Failed to register {self.failed} of {self.total} coverage{_plural(self.total)}."
            )
