# ------------------------------------------------------------------------------
#
#  CODA utilities and functions
#
# Project: VirES-Aeolus
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2017 EOX IT Services GmbH
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

import datetime

import coda
from django.utils.timezone import utc


class CODAFile(object):
    """ Wrapper around the filehandles used in the :mod:`coda` library.
    """

    def __init__(self, filename):
        """ Initializes a new :class:`CODAFile` with the given filename.
        """
        self._handle = None  # in case the coda.open fails initialize
        self._handle = coda.open(filename.encode('ascii'))
        self.filename = filename

    @property
    def handle(self):
        """ Return the actual CODA file handle.
        """
        return self._handle

    def __getitem__(self, *args):
        return self.fetch(*args)

    def fetch(self, *args):
        """
        """
        return coda.fetch(self._handle, *args)

    def fetch_date(self, *args):
        """ Fetch a value and convert it to a :class:`datetime.datetime` with
            UTC Zulu.
        """
        return coda_time_to_datetime(self.fetch(*args))

    def get_size(self, *path):
        """ Returns the shape of the specified dataset.
        """
        return coda.get_size(self._handle, *path)

    @property
    def product_type(self):
        return coda.get_product_type(self._handle)

    @property
    def product_class(self):
        return coda.get_product_class(self._handle)

    def close(self):
        """ Closes the :class:`CODAFile` object, if it was not already closed.
        """
        if not self.closed:
            coda.close(self._handle)
            self._handle = None

    @property
    def closed(self):
        """ Checks whether or not the file was already closed.
        """
        return self._handle is None

    # Cleanup on close:

    def __del__(self):
        self.close()

    # Context manager protocol:

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


def datetime_to_coda_time(value):
    """ Utility function to translate a :class:`datetime.datetime` to the
        floating point numbers used in coda to display time values.
    """
    return coda.time_parts_to_double_utc(*value.utctimetuple()[:7])


def coda_time_to_datetime(value):
    """ Translate a coda time value to a :class:`datetime.datetime`.
    """
    return datetime.datetime(
        *coda.time_double_to_parts_utc(value), tzinfo=utc
    )


def access_location(cf, location):
    """
    """
    return location(cf) if callable(location) else cf.fetch(*location)


class UnknownFieldError(Exception):
    pass


def check_fields(requested, available, label=None):
    unavailable = set(requested) - set(available)

    if unavailable:
        raise UnknownFieldError(
            'Unknown %sfield%s: %s' % (
                label + ' ' if label else '',
                's' if len(unavailable) > 1 else '',
                ', '.join(unavailable)
            )
        )
