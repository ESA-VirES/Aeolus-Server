#-------------------------------------------------------------------------------
#
# Project: EOxServer <http://eoxserver.org>
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#          Daniel Santillan <daniel.santillan@eox.at>
#          Martin Paces <martin.paces@eox.at>
#
#-------------------------------------------------------------------------------
# Copyright (C) 2014 EOX IT Services GmbH
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
# pylint: disable=wrong-import-order, ungrouped-imports, unused-import

from os.path import dirname, join
from math import ceil, floor
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.cm import get_cmap
from itertools import ifilter, ifilterfalse

import vires.contrib.colormaps as cmaps

try:
    from numpy import full
except ImportError:
    from numpy import empty
    def full(shape, value, dtype=None, order='C'):
        """ Numpy < 1.8 workaround. """
        arr = empty(shape, dtype, order)
        arr.fill(value)
        return arr


def unique(iterable):
    """ Remove duplicates from an iterable preserving the order."""
    items = set()
    for item in ifilterfalse(items.__contains__, iterable):
        yield item
        items.add(item)


def exclude(iterable, excluded):
    """ Remove items from the `iterable` which are present among the
    elements of the `excluded` set.
    """
    return ifilterfalse(set(excluded).__contains__, iterable)


def include(iterable, included):
    """ Remove items from the `iterable` which are not present among the
    elements of the `included` set.
    """
    return ifilter(set(included).__contains__, iterable)


# NOTE: We deliberately break the python naming convention here.
class cached_property(object):
    """ Decorator converting a given method with a single self argument
     into a property cached on the instance.
    """
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, type=None):
        if instance is None:
            return self
        value = self.func(instance)
        instance.__dict__[self.func.__name__] = value
        return value


def between(data, lower_bound, upper_bound):
    """ Get mask of values within the given closed interval:
        lower_bound <= value <= upper_bound
    """
    return (data >= lower_bound) & (data <= upper_bound)


def between_co(data, lower_bound, upper_bound):
    """ Get mask of values within the given closed-open interval:
        lower_bound <= value < upper_bound
    """
    return (data >= lower_bound) & (data < upper_bound)


# TODO: To be removed.
#       Unnecessary as the Python 2.6 compatibility has been dropped.
def get_total_seconds(td_obj):
    """ Get `datetime.timedelta` as total number of seconds. """
    try:
        return td_obj.total_seconds()
    except AttributeError:
        return td_obj.microseconds*1e-6 + td_obj.seconds + td_obj.days*86400


def float_array_slice(start, stop, first, last, step, tolerance):
    """
        Get array index range for given sub-setting interval
        (`start`, `stop`), extent of the array (`first`, `last`),
        step of the regular sampling `step`, and selection tolerance
        `tolerance`.
    """
    rstep = 1.0 / step
    _first = first * rstep
    _tolerance = abs(tolerance * rstep)
    size = 1 + int(round(rstep * last - _first))
    low = int(ceil(rstep * start - _tolerance - _first))
    high = int(floor(rstep * stop + _tolerance - _first))
    if high < 0 or low >= size:
        return 0, 0
    else:
        return max(0, low), min(size, high + 1)


def datetime_array_slice(start, stop, first, last, step, tolerance):
    """
        Get array index range for given sub-setting time interval
        (`start`, `stop`), time extent of the array (`first`, `last`),
        step of the regular time sampling `step`, and selection tolerance
        time `tolerance`.
    """
    return float_array_slice(
        (start - first).total_seconds(), (stop - first).total_seconds(),
        0.0, (last - first).total_seconds(),
        step.total_seconds(), tolerance.total_seconds()
    )


def get_color_scale(name):
    """ Get named color-map. """

    def clist_to_colormap(label, colors, color_scale=1.0, alpha_scale=1.0):
        """ Convert list of colors to `LinearSegmentedColormap` object """
        reds, greens, blues = [], [], []

        for alpha, red, green, blue in colors:
            alpha *= alpha_scale
            red *= color_scale
            green *= color_scale
            blue *= color_scale

            reds.append((alpha, red, red))
            greens.append((alpha, green, green))
            blues.append((alpha, blue, blue))

        return LinearSegmentedColormap(
            label, {'red': reds, 'green': greens, 'blue': blues}
        )


    if name == "blackwhite":
        return clist_to_colormap(name, [
            (0.0, 0, 0, 0),
            (1.0, 255, 255, 255),
        ], 1.0 / 255.0)

    elif name == "coolwarm":
        return clist_to_colormap(name, [
            (0.0, 255, 0, 0),
            (0.5, 255, 255, 255),
            (1.0, 0, 0, 255),
        ], 1.0 / 255.0)

    elif name == "rainbow":
        return clist_to_colormap(name, [
            (0.0, 150, 0, 90),
            (0.125, 0, 0, 200),
            (0.25, 0, 25, 255),
            (0.375, 0, 152, 255),
            (0.5, 44, 255, 150),
            (0.625, 151, 255, 0),
            (0.75, 255, 234, 0),
            (0.875, 255, 111, 0),
            (1.0, 255, 0, 0),
        ], 1.0 / 255.0)

    elif name == "diverging_2":
        return clist_to_colormap(name, [
            (0.0, 0, 0, 0),
            (0.000000000001, 3, 10.1796, 255),
            (0.1, 32, 74.3152, 255),
            (0.2, 60, 138.451, 255),
            (0.3333, 119.72, 196.725, 255),
            (0.4666, 240, 255, 255),
            (0.5333, 240, 255, 255),
            (0.6666, 242.25, 255, 127.5),
            (0.8, 255, 255, 0),
            (0.9, 255, 131.99, 30.5294),
            (0.999999999999, 255, 8.98008, 61.0585),
            (1.0, 255, 0, 255),
        ], 1.0 / 255.0)


    elif name == "yignbu":
        return clist_to_colormap(name, [
            ( 0.0, 8,29,88),
            (.125, 37,52,148),
            (.25, 34,94,168),
            (.375, 29,145,192),
            (.5, 65,182,196),
            (.625, 127,205,187),
            (.75, 199,233,180),
            (.875, 237,248,217),
            (1.0, 255,255,217),
        ], 1.0 / 255.0)

    elif name == "greens":
        return clist_to_colormap(name, [
            (0.0, 0,68,27),
            (0.125, 0,109,44),
            (0.25, 35,139,69),
            (0.375, 65,171,93),
            (0.5, 116,196,118),
            (0.625, 161,217,155),
            (0.75, 199,233,192),
            (0.875, 229,245,224),
            (1.0, 247,252,245),
        ], 1.0 / 255.0)

    elif name == "yiorrd":
        return clist_to_colormap(name, [
            (0.0  ,128,0,38),
            (0.125,189,0,38),
            (0.25 ,227,26,28),
            (0.375,252,78,42),
            (0.5  ,253,141,60),
            (0.625,254,178,76),
            (0.75 ,254,217,118),
            (0.875,255,237,160),
            (1.0  ,255,255,204),
        ], 1.0 / 255.0)

    elif name == "bluered":
        return clist_to_colormap(name, [
            (0.0, 0,0,255),
            (1.0, 255,0,0),
        ], 1.0 / 255.0)

    elif name == "earth":
        return clist_to_colormap(name, [
            (0.0, 0,0,130),
            (0.1, 0,180,180),
            (0.2, 40,210,40),
            (0.4, 230,230,50),
            (0.6, 120,70,20),
            (1.0, 255,255,255),
        ], 1.0 / 255.0)


    elif name == "electric":
        return clist_to_colormap(name, [
            (0.0, 0,0,0),
            (0.15, 30,0,100), 
            (0.4, 120,0,100),
            (0.6, 160,90,0),
            (0.8, 230,200,0),
            (1.0, 255,250,220),
        ], 1.0 / 255.0)

    elif name == "portland":
        return clist_to_colormap(name, [
            (0.0, 12,51,131),
            (0.25, 10,136,186), 
            (0.5, 242,211,56),
            (0.75, 242,143,56), 
            ( 1.0, 217,30,30),
        ], 1.0 / 255.0)

    elif name == "blackbody":
        return clist_to_colormap(name, [
            (0.0, 0,0,0),
            (0.2, 230,0,0),
            (0.4, 230,210,0),
            (0.7, 255,255,255),
            (1.0, 160,200,255),
        ], 1.0 / 255.0)


    elif name == "diverging_1":
        return clist_to_colormap(name, [
            (0.0, 0.2510, 0, 0.2510),
            (0.01587301587, 0.2331, 0, 0.3045),
            (0.03174603174, 0.2151, 0, 0.3580),
            (0.04761904761, 0.1972, 0, 0.4115),
            (0.06349206348, 0.1793, 0, 0.4650),
            (0.07936507935, 0.1613, 0, 0.5185),
            (0.09523809522, 0.1434, 0, 0.5720),
            (0.11111111109, 0.1255, 0, 0.6255),
            (0.12698412696, 0.1076, 0, 0.6790),
            (0.14285714283, 0.0896, 0, 0.7325),
            (0.1587301587, 0.0717, 0, 0.7860),
            (0.17460317457, 0.0538, 0, 0.8395),
            (0.19047619044, 0.0359, 0, 0.8930),
            (0.20634920631, 0.0179, 0, 0.9465),
            (0.22222222218, 0, 0, 1.0000, ),
            (0.23809523805, 0.0091, 0.0909, 1.0),
            (0.25396825392, 0.0182, 0.1818, 1.0),
            (0.26984126979, 0.0273, 0.2727, 1.0),
            (0.28571428566, 0.0364, 0.3636, 1.0),
            (0.30158730153, 0.0455, 0.4545, 1.0),
            (0.3174603174, 0.0545, 0.5455, 1.0),
            (0.33333333327, 0.0636, 0.6364, 1.0),
            (0.34920634914, 0.0727, 0.7273, 1.0),
            (0.36507936501, 0.0818, 0.8182, 1.0),
            (0.38095238088, 0.0909, 0.9091, 1.0),
            (0.39682539675, 0.1000, 1.0000, 1.0),
            (0.41269841262, 0.2500, 1.0000, 1.0),
            (0.42857142849, 0.4000, 1.0000, 1.0),
            (0.44444444436, 0.5500, 1.0000, 1.0),
            (0.46031746023, 0.7000, 1.0000, 1.0),
            (0.4761904761, 0.8500, 1.0000, 1.0),
            (0.49206349197, 1.0, 1.0, 1.0000),
            (0.50793650784, 1.0, 1.0, 0.8333),
            (0.52380952371, 1.0, 1.0, 0.6667),
            (0.53968253958, 1.0, 1.0, 0.5000),
            (0.55555555545, 1.0, 1.0, 0.3333),
            (0.57142857132, 1.0, 1.0, 0.1667),
            (0.58730158719, 1.0, 1.0, 0),
            (0.60317460306, 1.0, 0.9333, 0),
            (0.61904761893, 1.0, 0.8667, 0),
            (0.6349206348, 1.0, 0.8000, 0),
            (0.65079365067, 1.0, 0.7333, 0),
            (0.66666666654, 1.0, 0.6667, 0),
            (0.68253968241, 1.0, 0.6000, 0),
            (0.69841269828, 1.0, 0.5333, 0),
            (0.71428571415, 1.0, 0.4667, 0),
            (0.73015873002, 1.0, 0.4000, 0),
            (0.74603174589, 1.0, 0.3333, 0),
            (0.76190476176, 1.0, 0.2667, 0),
            (0.77777777763, 1.0, 0.2000, 0),
            (0.7936507935, 1.0, 0.1333, 0),
            (0.80952380937, 1.0, 0.0667, 0),
            (0.82539682524, 1.0, 0, 0),
            (0.84126984111, 1.0, 0, 0.0909),
            (0.85714285698, 1.0, 0, 0.1818),
            (0.87301587285, 1.0, 0, 0.2727),
            (0.88888888872, 1.0, 0, 0.3636),
            (0.90476190459, 1.0, 0, 0.4545),
            (0.92063492046, 1.0, 0, 0.5455),
            (0.93650793633, 1.0, 0, 0.6364),
            (0.9523809522, 1.0, 0, 0.7273),
            (0.96825396807, 1.0, 0, 0.8182),
            (0.98412698394, 1.0, 0, 0.9091),
            (1.0, 1.0, 0, 1.0000),
        ])

    elif name=="viridis" or name=="magma" or name=="inferno" or name=="plasma":
        return getattr(cmaps, name)

    else:
        # get standard colour-map
        return get_cmap(name)
