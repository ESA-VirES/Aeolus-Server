# ------------------------------------------------------------------------------
#
# Products management - fast registration
#
# Project: VirES
# Authors: Martin Paces <martin.paces@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2016 EOX IT Services GmbH
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
# ------------------------------------------------------------------------------
# pylint: disable=fixme, import-error, no-self-use, broad-except
# pylint: disable=missing-docstring, too-many-locals, too-many-branches
# pylint: disable=redefined-variable-type

import sys
from os.path import basename
from django.core.management.base import CommandError, BaseCommand
from django.db import transaction
from eoxserver.resources.coverages.models import (
    Product, Collection, collection_insert_eo_object
)
from eoxserver.resources.coverages.management.commands import CommandOutputMixIn

from aeolus.registration import register_product


class Command(CommandOutputMixIn, BaseCommand):

    help = (
        "Register one or more products. This command handles multiple "
        "product and requires minimal set of parameters."
    )
    args = "[<identifier> [<identifier> ...]]"

    def add_arguments(self, parser):
        parser.add_argument(
            "-f", "--file", dest="input_file", default=None,
            help=(
                "Optional file from which the inputs are read rather "
                "than form the command line arguments. Use dash to read "
                "file from standard input."
            )
        )
        parser.add_argument(
            "-c", "--collection", dest="collection_id", default=None,
            help="Optional collection the product should be linked to."
        )
        parser.add_argument(
            "--conflict", dest="conflict", choices=("IGNORE", "REPLACE"),
            default="IGNORE", help=(
                "Define how to resolve conflict when the product is already "
                "registered. By default the registration is skipped and the "
                "the passed product IGNORED. An alternative is to REPLACE the "
                "old product (remove the old one and insert the new one). "
                "In case of the REPLACE the collection links are NOT preserved."
            )
        )

        parser.add_argument(
            "--no-insert", dest="insert_into_collection",
            action="store_false", default=True
        )
        # conflict resolving option
        parser.add_argument(
            'FILES', nargs='*',
            help=(
                "The product files to register."
            )
        )

    @transaction.atomic
    def handle(self, **kwargs):
        def filer_lines(lines):
            for line in lines:
                line = line.partition("#")[0]  # strip comments
                line = line.strip()  # strip white-space padding
                if line:  # empty lines ignored
                    yield line

        # check collection
        # product generator
        if kwargs["input_file"] is None and kwargs['FILES']:
            # command line input
            product_filenames = kwargs['FILES']
        elif kwargs["input_file"] == "-":
            product_filenames = filer_lines(filename for filename in sys.stdin)
        elif kwargs["input_file"]:
            def file_reader(file_name):
                with open(file_name) as fin:
                    for line in fin:
                        yield line.strip()
            product_filenames = filer_lines(file_reader(kwargs["input_file"]))

        else:
            raise CommandError('Must provide either --file or list of files')

        count = 0
        success_count = 0  # success counter - counts finished registrations
        ignored_count = 0  # ignore counter - counts skipped registrations
        for product_filename in product_filenames:
            count += 1
            identifier = get_identifier(product_filename)
            self.print_msg(
                "Registering product %s [%s] ... " % (
                    identifier, product_filename
                )
            )

            is_registered = product_is_registered(identifier)

            product = None

            if is_registered and kwargs["conflict"] == "IGNORE":
                self.print_wrn(
                    "The product '%s' is already registered. The registration "
                    "of product '%s' is skipped!" % (identifier, product)
                )
                ignored_count += 1
                continue
            elif is_registered and kwargs["conflict"] == "REPLACE":
                self.print_wrn(
                    "The product '%s' is already registered. The product will "
                    "be replaced." % identifier
                )
                try:
                    product = product_update(identifier, product_filename)
                except Exception as exc:
                    self.print_traceback(exc, kwargs)
                    self.print_err(
                        "Update of product '%s' failed! Reason: %s" % (
                            identifier, exc,
                        )
                    )
                    continue
            else:  # not registered
                try:
                    product = product_register(identifier, product_filename)
                except Exception as exc:
                    self.print_traceback(exc, kwargs)
                    self.print_err(
                        "Registration of product '%s' failed! Reason: %s" % (
                            identifier, exc,
                        )
                    )
                    continue

            if product and kwargs['insert_into_collection']:
                try:
                    if kwargs.get('collection_id'):
                        collection = Collection.objects.get(
                            identifier=kwargs.get('collection_id')
                        )
                    else:
                        product_type = product.product_type
                        collection = Collection.objects.get(
                            collection_type__allowed_product_types=product_type
                        )
                    collection_link_product(collection, product)
                except Collection.DoesNotExist:
                    self.print_err(
                        'Could not find collection for product %s'
                        % identifier
                    )

            success_count += 1

        error_count = count - success_count - ignored_count
        if error_count > 0:
            self.print_msg("Failed to register %d product(s)." % error_count, 1)
        if ignored_count > 0:
            self.print_msg(
                "Skipped registrations of %d product(s)." % ignored_count, 1
            )
        if success_count > 0:
            self.print_msg(
                "Successfully registered %d of %s product(s)." %
                (success_count, count), 1
            )
        else:
            self.print_msg("No product registered.", 1)


def collection_exists(identifier):
    """ Return True if the product collection exists. """
    return Collection.objects.filter(identifier=identifier).exists()


@transaction.atomic
def collection_link_product(collection, product):
    """ Link product to a collection """

    collection_insert_eo_object(collection, product)


def product_is_registered(identifier):
    """ Return True if the product is already registered. """
    return Product.objects.filter(identifier=identifier).exists()


@transaction.atomic
def product_register(identifier, data_file):
    """ Register product. """

    product = register_product(data_file, overrides={'identifier': identifier})
    return product


@transaction.atomic
def product_deregister(identifier):
    """ De-register product. """
    product = Product.objects.get(identifier=identifier)
    product.delete()


@transaction.atomic
def product_update(identifier, *args, **kwargs):
    """ Update existing product. """
    product_deregister(identifier)
    return product_register(identifier, *args, **kwargs)


def get_identifier(data_file, metadata_file=None):
    """ Get the product identifier. """
    return basename(data_file).partition(".")[0]


def _split_location(item):
    """ Splits string as follows: <format>:<location> where format can be
        None.
    """
    idx = item.find(":")
    return (None, item) if idx == -1 else (item[:idx], item[idx + 1:])
