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

from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django import forms
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from eoxserver.resources.coverages.management.commands import (
    nested_commit_on_success
)

from aeolus.models import get_or_create_user_product_collection, Product
from aeolus.registration import register_product


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
        print request.POST, request.FILES
        if form.is_valid():
            handle_uploaded_file(request.FILES['file'], user)
            return HttpResponseRedirect(request.path)
    else:
        form = UploadFileForm()
    return render_to_response('aeolus/upload_user_file.html', {'form': form})


@nested_commit_on_success
def handle_uploaded_file(uploaded_file, user):
    out_path = join(settings.USER_UPLOAD_DIR, user.username, uploaded_file.name)

    try:
        os.makedirs(dirname(out_path))
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

    # actually upload and store file
    with open(out_path, 'wb+') as destination:
        for chunk in uploaded_file.chunks():
            destination.write(chunk)

    # TODO: upload/file limit

    # ingest the file into the users collection
    identifier = get_identifier(out_path, user)

    existing_product = Product.objects.filter(identifier=identifier).first()
    if existing_product:
        existing_product.cast().delete()

    collection = get_or_create_user_product_collection(user)
    product = register_product(out_path, overrides={'identifier': identifier})
    collection.insert(product)


def get_identifier(data_file, user):
    """ Get the product identifier. """
    return "user_upload_%s_%s" % (
        user.username,
        basename(data_file).partition(".")[0]
    )
