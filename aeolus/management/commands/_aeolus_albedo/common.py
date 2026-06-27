#-------------------------------------------------------------------------------
#
# Aeolus coverage management - common utilities
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
# pylint: disable=abstract-method

from os.path import isfile
from .._common import Subcommand, time_spec


class CoverageSelectionSubcommand(Subcommand):
    """ Aeolus coverage selection subcommand. """
    SELECT_RELATED = []
    PREFETCH_RELATED = []

    def add_arguments(self, parser):
        parser.add_argument("identifier", nargs="*")
        parser.add_argument(
            "-c", "--collection",
            dest="collection", action="append",
            help=(
                "Optional filter on the collection. "
                "Multiple collections are allowed."
            )
        )
        parser.add_argument(
            "-t", "--type", "--coverage-type",
            dest="type", action="append",
            help=(
                "Optional filter on the coverage type. "
                "Multiple coverage types are allowed."
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
        parser.add_argument(
            "--invalid-only", dest="invalid_only", action="store_true",
            default=False, help="Select invalid coverages missing a data-file."
        )

    def select_coverages(self, query, **kwargs):
        """ Get matched coverages. """

        if self.SELECT_RELATED:
            query = query.select_related(*self.SELECT_RELATED)

        if self.PREFETCH_RELATED:
            query = query.prefetch_related(*self.PREFETCH_RELATED)

        query = self._select_coverages_by_id(query, **kwargs)

        collections = set(kwargs["collection"] or [])
        if collections:
            query = query.filter(collections__identifier__in=collections)

        coverage_types = set(kwargs["type"] or [])
        if coverage_types:
            query = query.filter(coverage_type__name__in=coverage_types)

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

        if kwargs["invalid_only"]:
            query = filter_invalid(query, self.logger)

        return query

    def _select_coverages_by_id(self, query, **kwargs):
        identifiers = set(kwargs["identifier"])
        if identifiers:
            query = query.filter(identifier__in=identifiers)
        return query


class CoverageSelectionSubcommandProtected(CoverageSelectionSubcommand):
    """ Aeolus coverage selection subcommand requiring --all if no id given. """

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-a", "--all", dest="select_all", action="store_true", default=False,
            help="Select all objects."
        )

    def _select_coverages_by_id(self, query, **kwargs):
        identifiers = set(kwargs['identifier'])
        if identifiers or not kwargs['select_all']:
            query = query.filter(identifier__in=identifiers)
            if not identifiers:
                self.warning(
                    "No identifier is specified and no object will be removed. "
                    "Use the --all option to remove all matched items."
                )
        return query


def filter_invalid(coverages, logger=None):
    """ Filter invalid coverages. """
    for coverage in coverages:
        if is_invalid(coverage, logger):
            yield coverage


def is_invalid(coverage, logger=None):
    """ Return true is coverages is invalid. """
    count = 0
    for data_item in coverage.coverage_data_items.all():
        count += 1
        location = data_item.location
        if not (location and isfile(location)):
            logger.warning(
                "Invalid coverage %s detected! File %s does not exist!",
                coverage.identifier, location
            )
            return True

    if count == 0:
        logger.warning(
            "Invalid coverage %s detected! No data item!", coverage.identifier
        )
        return True

    if count > 1:
        logger.warning(
            "Invalid coverage %s detected! Multiple data items!",
            coverage.identifier
        )
        return True

    return False
