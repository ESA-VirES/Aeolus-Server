# ------------------------------------------------------------------------------
#
#  Aeolus - Level 1B data extraction
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


from datetime import datetime

import numpy as np

from aeolus.coda_utils import datetime_to_coda_time


def make_mask(data, min_value=None, max_value=None, is_array=False, **kwargs):
    """ Utility function to generate a bitmask with the given filter values.
        When the data itself is an Array of arrays, the filter is broadcast to
        the sub-arrays and a summary value is used (if any of the sub-arrays
        values are ``True`)
    """

    # allow both min and min_value
    min_value = min_value if min_value is not None else kwargs.get('min')
    max_value = max_value if max_value is not None else kwargs.get('max')
    eps = kwargs.get('epsilon')

    if is_array:
        mask = np.empty(data.shape[0], dtype=bool)
        for i, array in enumerate(data):
            mask[i] = np.any(make_mask(array, min_value, max_value, False))

        return mask

    if isinstance(min_value, datetime):
        min_value = datetime_to_coda_time(min_value)
        eps = 0.0001 if eps is None else eps
    if isinstance(max_value, datetime):
        max_value = datetime_to_coda_time(max_value)
        eps = 0.0001 if eps is None else eps

    if min_value is not None and min_value == max_value:
        mask = data == min_value

    # special treatment when we get an epsilon value for accuracy
    elif eps is not None:
        if min_value is not None and max_value is not None:
            if min_value > max_value:
                mask = np.logical_or(
                    (data - max_value) <= eps,
                    (data - min_value) >= -eps,
                )
            else:
                mask = np.logical_and(
                    (data - max_value) <= eps,
                    (data - min_value) >= -eps,
                )
        elif min_value is not None:
            mask = (data - min_value) >= -eps
        elif max_value is not None:
            mask = (data - max_value) <= eps
        else:
            raise NotImplementedError

    # no epsilon, use fastest way possible
    elif min_value is not None and max_value is not None:
        if min_value > max_value:
            mask = np.logical_or(
                data <= max_value,
                data >= min_value
            )
        else:
            mask = np.logical_and(
                data <= max_value,
                data >= min_value
            )

    elif min_value is not None:
        mask = data >= min_value
    elif max_value is not None:
        mask = data <= max_value
    else:
        raise NotImplementedError

    if mask.ndim > 1:
        mask = np.any(mask, -1)

    return mask


def combine_mask(mask_a, mask_b=None):
    """ Combine two bit masks of the same shape. One may be unset (and thus
        ``None``).
    """
    if mask_b is None:
        return mask_a

    return np.logical_and(mask_a, mask_b)


def make_array_mask(data, min_value=None, max_value=None, is_array=False,
                    **kwargs):
    # allow both min and min_value
    min_value = min_value if min_value is not None else kwargs.get('min')
    max_value = max_value if max_value is not None else kwargs.get('max')
    eps = kwargs.get('epsilon')

    if isinstance(min_value, datetime):
        min_value = datetime_to_coda_time(min_value)
        eps = 0.0001 if eps is None else eps
    if isinstance(max_value, datetime):
        max_value = datetime_to_coda_time(max_value)
        eps = 0.0001 if eps is None else eps

    if min_value is not None and min_value == max_value:
        mask = data == min_value

    # special treatment when we get an epsilon value for accuracy
    elif eps is not None:
        if min_value is not None and max_value is not None:
            if min_value > max_value:
                mask = np.logical_or(
                    (data - max_value) <= eps,
                    (data - min_value) >= -eps,
                )
            else:
                mask = np.logical_and(
                    (data - max_value) <= eps,
                    (data - min_value) >= -eps,
                )
        elif min_value is not None:
            mask = (data - min_value) >= -eps
        elif max_value is not None:
            mask = (data - max_value) <= eps
        else:
            raise NotImplementedError

    # no epsilon, use fastest way possible
    elif min_value is not None and max_value is not None:
        if min_value > max_value:
            mask = np.logical_or(
                data <= max_value,
                data >= min_value
            )
        else:
            mask = np.logical_and(
                data <= max_value,
                data >= min_value
            )

    elif min_value is not None:
        mask = data >= min_value
    elif max_value is not None:
        mask = data <= max_value
    else:
        raise NotImplementedError
    return mask
