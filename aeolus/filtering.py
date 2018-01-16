from datetime import datetime

import numpy as np

from aeolus.coda_utils import datetime_to_coda_time


def make_mask(data, min_value=None, max_value=None, is_array=False):
    """ Utility function to generate a bitmask with the given filter values.
        When the data itself is an Array of arrays, the filter is broadcast to
        the sub-arrays and a summary value is used (if any of the sub-arrays
        values are ``True`)
    """
    if is_array:
        mask = np.empty(data.shape, dtype=bool)
        for i, array in enumerate(data):
            mask[i] = np.any(make_mask(array, min_value, max_value, False))
        return mask

    if isinstance(min_value, datetime):
        min_value = datetime_to_coda_time(min_value)
    if isinstance(max_value, datetime):
        max_value = datetime_to_coda_time(max_value)

    if min_value is not None and min_value == max_value:
        mask = data == min_value
    elif min_value is not None and max_value is not None:
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


def combine_mask(mask_a, mask_b=None):
    """ Combine two bit masks of the same shape. One may be unset (and thus
        ``None``).
    """
    if mask_b is None:
        return mask_a

    return np.logical_and(mask_a, mask_b)
