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
    """ Aeolus collection selection subcommand. """

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
        query = self._select_collections_by_id(query, **kwargs)

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

    def _select_collections_by_id(self, query, **kwargs):
        identifiers = set(kwargs['identifier'])
        if identifiers:
            query = query.filter(identifier__in=identifiers)
        return query


class CollectionSelectionSubcommandProtected(CollectionSelectionSubcommand):
    """ Aeolus collection selection subcommand requiring --all if no id given. """

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-a", "--all", dest="select_all", action="store_true", default=False,
            help="Select all objects."
        )

    def _select_collections_by_id(self, query, **kwargs):
        identifiers = set(kwargs['identifier'])
        if identifiers or not kwargs['select_all']:
            query = query.filter(identifier__in=identifiers)
            if not identifiers:
                self.warning(
                    "No identifier is specified and no object will be selected. "
                    "Use the --all option to select all matched items."
                )
        return query
