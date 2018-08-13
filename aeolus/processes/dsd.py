# ------------------------------------------------------------------------------
#
#  DSD metadata extraction for AUX and ADM products
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

from eoxserver.core import Component, implements
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from eoxserver.services.ows.wps.parameters import (
    ComplexData, FormatJSON, CDObject, LiteralData
)

from aeolus.extraction.dsd import get_dsd
from aeolus.processes.util.measurement import MeasurementDataExtractProcessBase


class DSDExtract(MeasurementDataExtractProcessBase, Component):
    """ This process extracts DSD metadata from the ADM-Aeolus and AUX products
    """
    implements(ProcessInterface)

    identifier = "aeolus:dsd"
    metadata = {}
    profiles = ["vires-util"]

    inputs = [
        ("product_id", LiteralData(
            'product_id', str, optional=False, title="Product identifier",
            abstract=(
                "The product identifier to retrieve the DSD information from"
            ),
        )),
        ("recursive", LiteralData(
            'recursive', str, optional=True, default="false", title="Recursive",
            abstract="Whether the DSD lookup shall be done recursively",
        )),
    ]
    outputs = [
        ("dsd", ComplexData(
            "dsd", title="",
            formats=[
                FormatJSON('application/json'),
            ],
        )),
    ]

    def execute(self, product_id, recursive, dsd, **kwargs):
        dsd_data = get_dsd(product_id, recursive == "true")
        return CDObject(
            dsd_data,
            filename="%s_dsd.json" % product_id,
            **dsd
        )
