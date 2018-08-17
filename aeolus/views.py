# ------------------------------------------------------------------------------
#
#  View functions and utilities to upload user generated files
#
# Project: VirES-Aeolus
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2018 EOX IT Services GmbH
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


import os
from os.path import basename, join, dirname
import errno
import logging

from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django import forms
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from django.db import transaction

from aeolus.models import get_or_create_user_product_collection, Product
from aeolus.registration import register_product


logger = logging.getLogger(__name__)


class UploadFileForm(forms.Form):
    file = forms.FileField()

# Imaginary function to handle an uploaded file.
# from somewhere import handle_uploaded_file


@csrf_exempt
@login_required
def upload_user_file(request):
    user = request.user

    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            handle_uploaded_file(request.FILES['file'], user)
            return HttpResponseRedirect(request.path)
    else:
        form = UploadFileForm()
    return render_to_response('aeolus/upload_user_file.html', {'form': form})


def handle_uploaded_file(uploaded_file, user):
    out_path = join(settings.USER_UPLOAD_DIR, user.username, uploaded_file.name)
    user_file_limit = getattr(settings, 'USER_UPLOAD_FILE_LIMIT', 1)

    logger.info(
        "User '%s' uploaded file '%s'." % (user.username, uploaded_file.name)
    )

    # ensure that the upload directories are there
    try:
        os.makedirs(dirname(out_path))
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

    # get the identifier for the uploaded product
    identifier = get_identifier(out_path, user)

    with transaction.atomic():
        existing_product = Product.objects.filter(identifier=identifier).first()
        if existing_product:
            existing_product = existing_product.cast()
            filename = existing_product.data_items.filter(
                    semantic__startswith='bands'
            ).first().location
            logger.debug(
                "Product '%s' already registered. "
                "Deleting old product and file %s"
                % (identifier, filename)
            )
            os.unlink(filename)
            existing_product.delete()

    # actually upload and store file
    with open(out_path, 'wb+') as destination:
        for chunk in uploaded_file.chunks():
            destination.write(chunk)

    # register the newly created file and insert it into the user collection
    with transaction.atomic():
        collection = get_or_create_user_product_collection(user)

        try:
            product = register_product(
                out_path, overrides={'identifier': identifier}
            )
            collection.insert(product)
        except:
            logger.error(
                "Failed to register/insert file %s. Deleting it." % out_path
            )
            os.unlink(out_path)
            raise

    # check whether we have reached the upload limit
    current_count = collection.eo_objects.all().count()
    if current_count > user_file_limit:
        num_to_delete = current_count - user_file_limit
        logger.info(
            "User upload limit reached for user '%s': "
            "%d allowed, %d currently registered (with uploaded file). "
            "Deleting %d product%s." % (
                user.username,
                user_file_limit, current_count,
                num_to_delete,
                "" if num_to_delete == 1 else "s"
            )
        )

        # delete the first n products
        to_delete = collection.eo_objects.exclude(
            identifier=identifier
        )[:num_to_delete]
        for eo_object in to_delete:
            existing_product = eo_object.cast()
            filename = existing_product.data_items.filter(
                semantic__startswith='bands'
            ).first().location

            logger.debug("Deleting user uploaded file %s" % filename)
            os.unlink(filename)
            existing_product.delete()


def get_identifier(data_file, user):
    """ Get the product identifier. """
    return "user_upload_%s_%s" % (
        user.username,
        basename(data_file).partition(".")[0]
    )
