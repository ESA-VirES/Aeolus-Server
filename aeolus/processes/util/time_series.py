#-------------------------------------------------------------------------------
#
# Data Source - base time-series class
#
# Project: VirES
# Authors: Martin Paces <martin.paces@eox.at>
#
#-------------------------------------------------------------------------------
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
#-------------------------------------------------------------------------------

from vires.cdf_util import CDF_EPOCH_TYPE
from vires.util import include, unique

class TimeSeries(object):
    """ Base time-series data source class. """

    @property
    def variables(self):
        """ Get list of the provided variables. """
        raise NotImplementedError

    def get_extracted_variables(self, variables):
        """ Expand/filter input variables into applicable variables. """
        if variables is None:
            return self.variables # get all available variables
        else:
            # get an applicable subset of the requested variables
            return list(include(unique(variables), self.variables))

    def subset(self, start, stop, variables=None):
        """ Generate a sequence of datasets holding the requested temporal
        subset of the time-series.
        Optionally, the returned variables can be restricted by the user defined
        list of variables.
        The start and stop UTC times should be instances of the
        datetime.datetime object.
        The output time-stamps are encoded as CDF-epoch times.
        """
        raise NotImplementedError

    def interpolate(self, times, variables=None, interp1d_kinds=None,
                    cdf_type=CDF_EPOCH_TYPE, valid_only=False):
        """ Get time-series interpolated from the provided time-line.
        Optionally, the returned variables can be restricted by the user defined
        list of variables.
        The default nearest neighbour interpolation method is used to
        interpolate the variables. Alternative interpolation methods
        can be specified for selected variables via the interp1d_kinds
        dictionary.
        Set valid_only to True to remove invalid records (NaNs due to the
        out-of-bounds interpolation).
        The input and output time-stamps are encoded as CDF-epoch times.
        """
        raise NotImplementedError
