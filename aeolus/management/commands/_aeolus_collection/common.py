#-------------------------------------------------------------------------------
#
# Aeolus collection management - common utilities
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
# pylint: disable=abstract-method, no-self-use

from eoxserver.resources.coverages.models import Collection
from .._common import Subcommand


class CollectionSelectionSubcommand(Subcommand):
    """ Aeolus collection type selection subcommand. """

    def add_arguments(self, parser):
        parser.add_argument("identifier", nargs="*")
        parser.add_argument(
            "-t", "--type", "--collection-type",
            dest="collection_type_name", default=None, nargs="*",
            help="Collection type name.",
        )
        parser.add_argument(
            "--exclude-type", "--exclude-collection-type",
            dest="excluded_collection_type_name", default=None, nargs="*",
            help="Excluded collection type names.",
        )

    def select_collections(self, **kwargs):
        """ Get list of matched collections types. """
        query = Collection.objects.all()
        identifiers = set(kwargs["identifier"])
        if identifiers:
            query = query.filter(identifier__in=identifiers)
        collection_type_names = set(kwargs.get("collection_type_name") or ())
        if collection_type_names:
            query = query.fileter(
                collection_type__name__in=collection_type_names
            )
        collection_type_names = set(kwargs.get("excluded_collection_type_name") or ())
        if collection_type_names:
            query = query.exclude(
                collection_type__name__in=collection_type_names
            )
        return query
