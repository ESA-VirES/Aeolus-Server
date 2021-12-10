# ------------------------------------------------------------------------------
#
#  Aeolus - Base class for data extraction facilities for AUX and actual data
#  products.
#
# Project: VirES-Aeolus
# Authors: Daniel Santillan <daniel.santilland@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2021 EOX IT Services GmbH
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

from aeolus.coda_utils import CODAFile

DATA_PATHS = [
    ['mph', 'product'],
    ['mph', 'proc_stage'],
    ['mph', 'ref_doc'],
    ['mph', 'acquisition_station'],
    ['mph', 'proc_center'],
    ['mph', 'proc_time'],
    ['mph', 'software_ver'],
    ['mph', 'baseline'],
    ['mph', 'sensing_start'],
    ['mph', 'sensing_stop'],
    ['mph', 'phase'],
    ['mph', 'cycle'],
    ['mph', 'rel_orbit'],
    ['mph', 'abs_orbit'],
    ['mph', 'state_vector_time'],
    ['mph', 'delta_ut1'],
    ['mph', 'x_position'],
    ['mph', 'y_position'],
    ['mph', 'z_position'],
    ['mph', 'x_velocity'],
    ['mph', 'y_velocity'],
    ['mph', 'z_velocity'],
    ['mph', 'vector_source'],
    ['mph', 'utc_sbt_time'],
    ['mph', 'sat_binary_time'],
    ['mph', 'clock_step'],
    ['mph', 'leap_utc'],
    # ['mph', 'gps_utc_time_difference'],
    ['mph', 'leap_sign'],
    ['mph', 'leap_err'],
    ['mph', 'product_err'],
    ['mph', 'tot_size'],
    ['mph', 'sph_size'],
    ['mph', 'num_dsd'],
    ['mph', 'dsd_size'],
    ['mph', 'num_data_sets'],
]

AUX_PATHS = [
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Product'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Proc_Stage'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Ref_Doc'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Acquisition_Station'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Proc_Center'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Proc_Time'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Software_Ver'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Baseline'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Sensing_Start'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Sensing_Stop'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Phase'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Cycle'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Rel_Orbit'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Abs_Orbit'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'State_Vector_Time'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Delta_UT1'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'X_Position'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Y_Position'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Z_Position'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'X_Velocity'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Y_Velocity'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Z_Velocity'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Vector_Source'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Utc_Sbt_Time'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Sat_Binary_Time'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Clock_Step'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Leap_Utc'],
    # ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Gps_Utc_Time_Difference'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Leap_Sign'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Leap_Err'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Product_Err'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Tot_Size'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Sph_Size'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Num_Dsd'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Dsd_Size'],
    ['Earth_Explorer_File', 'Earth_Explorer_Header', 'Variable_Header', 'Main_Product_Header', 'Num_Data_Sets'],
]

def get_mph(product,  strip=True):

    filename = product.product_data_items.first().location
    prod_name = product.product_type.name
    is_aux = prod_name.startswith('AUX') and not prod_name.startswith('AUX_MET')
    paths = AUX_PATHS if is_aux else DATA_PATHS

    with CODAFile(filename) as cf:
        data = [_convert_item(cf.fetch(*path), strip) for path in paths]
        keys = [path[-1].lower() for path in paths]
        out = dict(zip(keys, data))
        return out

def _convert_item(item, strip):
    "Helper to convert values of the mph parts for later encoding"
    if isinstance(item, str):
        return item.strip() if strip else item
    else:
        return str(item)
