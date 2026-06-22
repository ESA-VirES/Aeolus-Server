#-------------------------------------------------------------------------------
#
# Aeolus - product operations
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

import logging
from os.path import splitext, basename
from dataclasses import dataclass
from typing import List
from django.db import transaction
from eoxserver.resources.coverages.models import (
    Collection,
    Product,
    collection_insert_eo_object,
    collection_collect_metadata,
    ManagementError,
)
from aeolus.registration import (
    read_aeolus_product_metadata,
    simplify_footprint,
    update_product,
    update_product_data_item,
)
from .eo_object import deregister_object

DEF_SIMPLIFICATION_TOLERANCE = 0.2


def get_product_id(filename):
    """ Get product identifier from a product filename. """
    return splitext(basename(filename))[0]


def get_product_collection(identifier):
    """ Get product collection by identifier. """
    try:
        return (
            Collection.objects
            .prefetch_related("collection_type__allowed_product_types")
            .get(identifier=identifier)
        )
    except Collection.DoesNotExist:
        raise ValueError("Invalid collection identifier!") from None


def get_allowed_product_types(collection):
    """ Read allowed product types from a given product collection. """
    collection_type = collection.collection_type
    if collection_type:
        return set(
            product_type.name
            for product_type in collection_type.allowed_product_types.all()
        )
    return None


def unlink_product_from_collection(collection, product, logger,
                                   update_collection=True):
    """ Unlink product from a collection. """
    with transaction.atomic():
        collection.products.remove(product)
        if update_collection:
            collection_collect_metadata(
                collection,
                collect_footprint=True,
                collect_begin_time=True,
                collect_end_time=True,
                use_extent=True,
                product_summary=True,
                coverage_summary=False,
            )

    logger.info(
        "product %s unlinked from collection %s",
        product.identifier, collection.identifier
    )


def update_product_collection(collection, logger):
    """ Update product collection metadata after product change. """
    with transaction.atomic():
        collection_collect_metadata(
            collection,
            collect_footprint=True,
            collect_begin_time=True,
            collect_end_time=True,
            use_extent=True,
            product_summary=True,
            coverage_summary=False,
        )
    logger.info("collection %s updated", collection.identifier)


def deregister_product(product, logger, update_collections=True):
    """ Deregister Aeolus product. """
    with transaction.atomic():
        result = deregister_object(
            product,
            logger=logger,
            update_collections=update_collections,
            use_extent=True,
            product_summary=True,
            coverage_summary=False,
        )
    logger.info("product %s deregistered", product.identifier)
    return result


@dataclass
class Result:
    """ Results of the Aeolus product registration. """
    product: Product
    inserted: bool
    updated: bool
    removed: List[str] # < Python 3.9
    linked_to_collection: List[str] # < Python 3.9


def register_product(
    collection, identifier, filename,
    update_existing=False,
    simplification_tolerance=DEF_SIMPLIFICATION_TOLERANCE,
    defer_collection_update=False,
    allowed_product_types=None,
    logger=None
):
    """ Register Aeolus product. """

    def _read_metadata(filename):
        metadata = read_aeolus_product_metadata(filename)
        metadata["footprint"] = simplify_footprint(
            metadata["footprint"],
            simplification_tolerance=simplification_tolerance,
        )
        return metadata

    def _register_product():
        inserted = False
        updated = False
        removed = []
        linked_to_collection = []

        product = _get_existing_product(identifier)

        if not product or update_existing:
            metadata = _read_metadata(filename)

            if not product:
                product = Product(identifier=identifier)
                inserted = True
            else:
                updated = True

            update_product(product=product, metadata=metadata)

            update_product_data_item(
                product=product,
                location=filename,
                format_=metadata["format"] or "",
            )

        if allowed_product_types is not None and product.product_type:
            if product.product_type.name not in allowed_product_types:
                raise ManagementError(
                    f"Product {product.identifier} is of invalid product type "
                    f"{product.product_type.name}!"
                  )

        if not product.collections.filter(pk=collection.pk).exists():
            if defer_collection_update:
                collection.products.add(product)
            else:
                collection_insert_eo_object(
                    collection,
                    product,
                    use_extent=True,
                )
                collection_collect_metadata(
                    collection,
                    collect_footprint=False,
                    collect_begin_time=False,
                    collect_end_time=False,
                    product_summary=True,
                    coverage_summary=False,
                )
            linked_to_collection.append(collection.identifier)

        return Result(
            product=product,
            inserted=inserted,
            updated=updated,
            removed=removed,
            linked_to_collection=linked_to_collection,
        )

    if not logger:
        logger = logging.getLogger(__name__)

    with transaction.atomic():
        result = _register_product()

    for product_id in result.removed:
        logger.info("product %s deregistered", product_id)

    if result.inserted:
        logger.info("product %s registered", identifier)
    elif result.updated:
        logger.info("product %s updated", identifier)
    else:
        logger.debug("product %s exists", identifier)

    for collection_id in result.linked_to_collection:
        logger.info(
            "product %s linked to collection %s",
            identifier, collection_id,
        )

    return result


def _remove_existing_product(identifier):
    count, _ = Product.objects.filter(identifier=identifier).delete()
    return count > 0


def _get_existing_product(identifier):
    try:
        return Product.objects.get(identifier=identifier)
    except Product.DoesNotExist:
        return None
