# ------------------------------------------------------------------------------
#
#  Data extraction from Level 1B ADM-Aeolus products
#
# Project: VirES-Aeolus
# Authors: Daniel Santillan <daniel.santillan@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2022 EOX IT Services GmbH
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

from aeolus.level_1a import extract_data
from aeolus.processes.util.measurement import MeasurementDataExtractProcessBase


class Level1AExtract(MeasurementDataExtractProcessBase, Component):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level1A products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level1A"
    metadata = {}
    profiles = ["vires-util"]

    extraction_function = extract_data
    level_name = "1A"

    range_type_name = "ALD_U_N_1A"
