#-------------------------------------------------------------------------------
#
# Deregister Aeolus coverages
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
from traceback import print_exc
from eoxserver.resources.coverages.models import Coverage
from aeolus.management.api.coverage import (
    deregister_coverage,
    update_coverage_collection,
)
from .._aeolus_product.common import ObjectSelectionSubcommandProtected


class DeregisterCoverageSubcommand(ObjectSelectionSubcommandProtected):
    name = "deregister"
    help = "Deregister Aeolus coverages."

    def add_arguments(self, parser):
        super().add_arguments(parser)
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
        objects = self.select_objects(Coverage.objects.all(), **kwargs)
        self.deregister_coverages(objects, **kwargs)

    def deregister_coverages(self, coverages, **kwargs):
        defer_collection_update = kwargs["defer_collection_update"]

        total_count = 0
        failed_count = 0
        removed_count = 0

        collections = {}
        for coverage in coverages:
            identifier = coverage.identifier
            try:
                collections.update(deregister_coverage(
                    coverage,
                    logger=self.logger,
                    update_collections=(not defer_collection_update),
                ))
            except Exception as error:
                failed_count += 1
                if kwargs.get("traceback"):
                    print_exc(file=sys.stderr)
                self.error(
                    "Failed to de-register coverage %s! %s",
                    identifier, error
                )
            else:
                removed_count += 1
            finally:
                total_count += 1

        if defer_collection_update:
            for collection in collections.values():
                try:
                    update_coverage_collection(collection, logger=self.logger)
                except Exception as error:
                    if kwargs.get("traceback"):
                        print_exc(file=sys.stderr)
                    self.error(
                        "Failed to update collection %s! %s",
                        collection.identifier, error
                    )

        if removed_count or total_count == 0:
            self.info(
                "%d of %d matched coverage%s de-registered.",
                removed_count, total_count, "s" if removed_count != 1 else ""
            )

        if failed_count:
            self.info(
                "%d of %d matched coverage%s failed to be de-registered.",
                failed_count, total_count, "s" if failed_count != 1 else ""
            )

        sys.exit(failed_count)
