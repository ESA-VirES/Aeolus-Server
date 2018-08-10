import os
from os.path import basename, join, dirname
import errno

from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django import forms
from django.conf import settings
from eoxserver.core import env
from eoxserver.backends.models import Storage, Package, DataItem
from eoxserver.backends.component import BackendComponent
from eoxserver.resources.coverages.management.commands import (
    nested_commit_on_success
)

from aeolus.models import get_or_create_user_product_collection, Product
from aeolus.registration import register_product


class UploadFileForm(forms.Form):
    file = forms.FileField()

# Imaginary function to handle an uploaded file.
# from somewhere import handle_uploaded_file


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

    if product_is_registered(identifier):
        product_deregister(identifier)

    collection = get_or_create_user_product_collection(user)
    product = product_register(identifier, out_path)
    collection_link_product(collection, product)


@nested_commit_on_success
def collection_link_product(collection, product):
    """ Link product to a collection """
    collection.insert(product)


def product_is_registered(identifier):
    """ Return True if the product is already registered. """
    return Product.objects.filter(identifier=identifier).exists()


@nested_commit_on_success
def product_register(identifier, data_file):
    """ Register product. """

    product = register_product(data_file, overrides={'identifier': identifier})
    return product


@nested_commit_on_success
def product_deregister(identifier):
    """ De-register product. """
    product = Product.objects.get(identifier=identifier).cast()
    product.delete()


@nested_commit_on_success
def product_update(identifier, *args, **kwargs):
    """ Update existing product. """
    product_deregister(identifier)
    return product_register(identifier, *args, **kwargs)


def get_identifier(data_file, user):
    """ Get the product identifier. """
    return "user_upload_%s_%s" % (
        user.username,
        basename(data_file).partition(".")[0]
    )


def _split_location(item):
    """ Splits string as follows: <format>:<location> where format can be
        None.
    """
    idx = item.find(":")
    return (None, item) if idx == -1 else (item[:idx], item[idx + 1:])


def _get_location_chain(items):
    """ Returns the tuple
    """
    component = BackendComponent(env)
    storage = None
    package = None

    storage_type, url = _split_location(items[0])
    if storage_type:
        storage_component = component.get_storage_component(storage_type)
    else:
        storage_component = None

    if storage_component:
        storage, _ = Storage.objects.get_or_create(
            url=url, storage_type=storage_type
        )

    # packages
    for item in items[1 if storage else 0:-1]:
        type_or_format, location = _split_location(item)
        package_component = component.get_package_component(type_or_format)
        if package_component:
            package, _ = Package.objects.get_or_create(
                location=location, format=format,
                storage=storage, package=package
            )
            storage = None  # override here
        else:
            raise Exception(
                "Could not find package component for format '%s'"
                % type_or_format
            )

    format_, location = _split_location(items[-1])
    return storage, package, format_, location
