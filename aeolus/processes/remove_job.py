# ------------------------------------------------------------------------------
#
# WPS process removing asynchronous jobs.
#
# Project: VirES
# Authors: Martin Paces <martin.paces@eox.at>
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
# pylint: disable=no-self-use,missing-docstring, too-few-public-methods


from eoxserver.services.ows.wps.parameters import (
    RequestParameter, LiteralData,
)
from eoxserver.services.ows.wps.exceptions import InvalidInputValueError
from eoxserver.services.ows.wps.util import get_async_backends
from aeolus.models import Job


class RemoveJob(object):
    """ This utility process removes an asynchronous WPS  job owned
    by the current user.
    """

    identifier = "removeJob"
    metadata = {}
    profiles = ["vires-util"]

    inputs = [
        ('user', RequestParameter(lambda request: request.user)),
        ('job_id', LiteralData('job_id', str, title="Job Identifier")),
    ]

    outputs = [('is_removed', bool)]

    def execute(self, user, job_id, **kwargs):
        # find job removal candidates
        owner = user if user.is_authenticated else None
        try:
            job = Job.objects.get(owner=owner, identifier=job_id)
        except Job.DoesNotExist:
            raise InvalidInputValueError('job_id')

        get_async_backends()[0].purge(job.identifier, job.process_id)

        return True
