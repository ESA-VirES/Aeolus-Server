#-------------------------------------------------------------------------------
#
# Export Aeolus collections
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
from django.contrib.auth.models import Permission
from .._common import JSON_OPTS
from .common import CollectionSelectionSubcommand


class ExportCollectionSubcommand(CollectionSelectionSubcommand):
    name = "export"
    help = "Export Aeolus collections"

    description = "Export registered collections in JSON format."

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
            serialize_collection(collection_type)
            for collection_type in self.select_collections(**kwargs)
        ]
        filename = kwargs["filename"]
        with (sys.stdout if filename == "-" else open(filename, "w", encoding="utf-8")) as file_:
            json.dump(data, file_, **JSON_OPTS)


def serialize_collection(collection):
    """ Serialize collection object. """
    permission = Permission.objects.get(codename=f"access_{collection.identifier}")
    allowed_users = [user.username for user in permission.user_set.all()]
    allowed_groups = [group.name for group in permission.group_set.all()]
    data = {
        "identifier": collection.identifier,
        "collectionType": collection.collection_type.name,
    }
    if allowed_users:
        data["allowedUsers"] = allowed_users
    if allowed_groups:
        data["allowedGroups"] = allowed_groups
    return data
