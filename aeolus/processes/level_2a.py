# ------------------------------------------------------------------------------
#
#  Data extraction from Level 2A ADM-Aeolus products
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
from eoxserver.services.ows.wps.parameters import LiteralData

from aeolus.level_2a import extract_data
from aeolus.processes.util.measurement import MeasurementDataExtractProcessBase
from aeolus.processes.util.bbox import translate_bbox


class Level2AExtract(MeasurementDataExtractProcessBase, Component):
    """ This process extracts Observations, Measurements and Groups from the
        ADM-Aeolus Level2A products of the specified collections.
    """
    implements(ProcessInterface)

    identifier = "aeolus:level2A"
    metadata = {}
    profiles = ["vires-util"]

    extraction_function = extract_data
    level_name = "2A"

    range_type_name = "ALD_U_N_2A"

    inputs = MeasurementDataExtractProcessBase.inputs + [
        ("group_fields", LiteralData(
            'group_fields', str, optional=True, default=None,
            title="Data variables",
            abstract="Comma-separated list of the extracted data variables."
        )),
    ]

    def get_data_filters(self, begin_time, end_time, bbox, filters, **kwargs):
        """ Overwritten function to get the exact data filters for L1B/2A files
        """

        data_filters = dict(
            L1B_centroid_time_obs={'min': begin_time, 'max': end_time},
            L1B_time_meas={'min': begin_time, 'max': end_time},
            group_start_time={'min': begin_time},
            group_end_time={'max': end_time},
            **(filters if filters else {})
        )

        if bbox:
            # TODO: assure that bbox is within -180,-90,180,90
            # TODO: when minlon > maxlon, make 2 bboxes
            tpl_box = (bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])

            tpl_box = translate_bbox(tpl_box)
            data_filters['longitude_of_DEM_intersection_obs'] = {
                'min': tpl_box[0],
                'max': tpl_box[2]
            }
            data_filters['longitude_of_DEM_intersection_meas'] = {
                'min': tpl_box[0],
                'max': tpl_box[2]
            }
            data_filters['latitude_of_DEM_intersection_obs'] = {
                'min': tpl_box[1],
                'max': tpl_box[3]
            }
            data_filters['latitude_of_DEM_intersection_meas'] = {
                'min': tpl_box[1],
                'max': tpl_box[3]
            }

        return data_filters
