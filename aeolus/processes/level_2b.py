# ------------------------------------------------------------------------------
#
#  Data extraction from Level 2B ADM-Aeolus products
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

from eoxserver.core import Component, implements
from eoxserver.services.ows.wps.interfaces import ProcessInterface

from aeolus.level_2b import extract_data
from aeolus.processes.util.accumulated import AccumulatedDataExctractProcessBase


class Level2BExctract(AccumulatedDataExctractProcessBase, Component):
    """ This process extracts Observations and Measurements from the ADM-Aeolus
        Level2B products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level2B"
    metadata = {}
    profiles = ["vires-util"]

    extraction_function = extract_data
    level_name = "2B"

    range_type_name = "ALD_U_N_2B"

    def get_data_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        """ Overwritten function to get the exact data filters for L2B/C files
        """
        data_filters = super(Level2BExctract, self).get_data_filters(
            begin_time, end_time, bbox, filters, **kwargs
        )

        data_filters.update(dict(
            mie_grouping_start_time={'min_value': begin_time},
            mie_grouping_stop_time={'max_value': end_time},
            rayleigh_grouping_start_time={'min_value': begin_time},
            rayleigh_grouping_stop_time={'max_value': end_time},
        ))

        return data_filters
