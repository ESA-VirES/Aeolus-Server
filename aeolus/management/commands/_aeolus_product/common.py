#-------------------------------------------------------------------------------
#
# Aeolus coverage and product management - common utilities
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

from django.db import transaction
from eoxserver.resources.coverages.models import collection_exclude_eo_object
from .._common import Subcommand, time_spec


class ObjectSelectionSubcommand(Subcommand):
    """ Aeolus product or coverage selection subcommand. """

    def add_arguments(self, parser):
        parser.add_argument("identifier", nargs="*")
        parser.add_argument(
            "-c", "--collection",
            dest="collection", action="append",
            help=(
                "Optional filter on the collection. "
                "Multiple ollections are allowed."
            )
        )
        parser.add_argument(
            "--after", type=time_spec, required=False,
            help="Select objects after the given date."
        )
        parser.add_argument(
            "--before", type=time_spec, required=False,
            help="Select objects before the given date."
        )
        parser.add_argument(
            "--created-after", type=time_spec, required=False,
            help="Select objects whose record has been created after the given date."
        )
        parser.add_argument(
            "--created-before", type=time_spec, required=False,
            help="Select objects whose record has been created before the given date."
        )
        parser.add_argument(
            "--updated-after", type=time_spec, required=False,
            help="Select objects whose record has been updated after the given date."
        )
        parser.add_argument(
            "--updated-before", type=time_spec, required=False,
            help="Select objects whose record has been updated before the given date."
        )

    def select_objects(self, query, **kwargs):
        """ Get list of matched objects. """

        query = query.prefetch_related("collections")

        query = self._select_objects_by_id(query, **kwargs)

        collections = set(kwargs["collection"] or [])
        if collections:
            query = query.filter(collections__identifier__in=collections)

        if kwargs["after"]:
            query = query.filter(begin_time__gte=kwargs["after"])

        if kwargs["before"]:
            query = query.filter(end_time__lt=kwargs["before"])

        if kwargs["created_after"]:
            query = query.filter(inserted__gte=kwargs["created_after"])

        if kwargs["created_before"]:
            query = query.filter(inserted__lt=kwargs["created_before"])

        if kwargs["updated_after"]:
            query = query.filter(updated__gte=kwargs["updated_after"])

        if kwargs["updated_before"]:
            query = query.filter(updated__lt=kwargs["updated_before"])

        return query

    def _select_objects_by_id(self, query, **kwargs):
        identifiers = set(kwargs["identifier"])
        if identifiers:
            query = query.filter(identifier__in=identifiers)
        return query


class ObjectSelectionSubcommandProtected(ObjectSelectionSubcommand):
    """ Aeolus product or coverage selection subcommand requiring --all if no id given. """

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-a", "--all", dest="select_all", action="store_true", default=False,
            help="Select all objects."
        )

    def _select_objects_by_id(self, query, **kwargs):
        identifiers = set(kwargs['identifier'])
        if identifiers or not kwargs['select_all']:
            query = query.filter(identifier__in=identifiers)
            if not identifiers:
                self.warning(
                    "No identifier is specified and no object will be removed. "
                    "Use the --all option to remove all matched items."
                )
        return query
