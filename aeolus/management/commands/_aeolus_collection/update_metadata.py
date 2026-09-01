#-------------------------------------------------------------------------------
#
# Aeolus collections metadata update
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
from django.db import transaction
from eoxserver.resources.coverages.models import (
    collection_collect_metadata,
)
from .common import CollectionSelectionSubcommandProtected


class UpdateMetadataCollectionSubcommand(CollectionSelectionSubcommandProtected):
    name = "update_metadata"
    help = "Update metadata of selected Aeolus collections"

    description = "Update metadata of selected Aeolus collections"

    def handle(self, **kwargs):

        total_count = 0
        failed_count = 0
        updated_count = 0

        for collection in self.select_collections(**kwargs):
            try:
                update_collection_metadata(collection, logger=self.logger)
            except Exception as error:
                failed_count += 1
                if kwargs.get("traceback"):
                    print_exc(file=sys.stderr)
                self.error(
                    "Failed to de-register product %s! %s",
                    collection.identifier, error
                )
            else:
                updated_count += 1
            finally:
                total_count += 1

        if updated_count or total_count == 0:
            self.info(
                "%d of %d matched collection%s updated.",
                updated_count, total_count, "s" if updated_count != 1 else ""
            )

        if failed_count:
            self.info(
                "%d of %d matched collection%s failed to be updated.",
                failed_count, total_count, "s" if failed_count != 1 else ""
            )

        sys.exit(failed_count)


def update_collection_metadata(collection, logger):
    with transaction.atomic():
        collection_collect_metadata(
            collection,
            collect_footprint=True,
            collect_begin_time=True,
            collect_end_time=True,
            use_extent=True,
            product_summary=True,
            coverage_summary=True,
        )
    logger.info("collection %s updated", collection.identifier)
