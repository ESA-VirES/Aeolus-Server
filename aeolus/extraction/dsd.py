# ------------------------------------------------------------------------------
#
#  Aeolus - Base class for data extraction facilities for AUX and actual data
#  products.
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

from aeolus.coda_utils import CODAFile
from aeolus import models


DATA_PATHS = [
    ['dsd', -1, 'ds_name'],
    ['dsd', -1, 'ds_type'],
    ['dsd', -1, 'filename'],
    ['dsd', -1, 'ds_offset'],
    ['dsd', -1, 'ds_size'],
    ['dsd', -1, 'num_dsr'],
    ['dsd', -1, 'dsr_size'],
    ['dsd', -1, 'byte_order'],
]

AUX_PATHS = [
    ['Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/List_of_Dsds/Dsd', -1, 'Ds_Name'],
    ['Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/List_of_Dsds/Dsd', -1, 'Ds_Type'],
    ['Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/List_of_Dsds/Dsd', -1, 'Filename'],
    ['Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/List_of_Dsds/Dsd', -1, 'Ds_Offset'],
    ['Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/List_of_Dsds/Dsd', -1, 'Ds_Size'],
    ['Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/List_of_Dsds/Dsd', -1, 'Num_Dsr'],
    ['Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/List_of_Dsds/Dsd', -1, 'Dsr_Size'],
    ['Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Specific_Product_Header/List_of_Dsds/Dsd', -1, 'Byte_Order'],
]


def get_dsd(product_id, recursive=False):
    try:
        product = models.Product.objects.get(identifier=product_id)
    except models.Product.DoesNotExist:
        return None

    filename = product.data_items.filter(
        semantic__startswith='bands'
    ).first().location

    is_aux = product.range_type.name.startswith('AUX')

    paths = AUX_PATHS if is_aux else DATA_PATHS
    with CODAFile(filename) as cf:
        data = [
            [
                item.strip() if isinstance(item, str) else item.item()
                for item in cf.fetch(*path)
            ] for path in paths
        ]
        keys = [path[-1].lower() for path in paths]

        out = [
            dict(zip(keys, ds))
            for ds in zip(*data)
        ]

        if recursive:
            for item in out:
                sub_dsd = get_dsd(item['filename'], recursive)
                if sub_dsd:
                    item['sub_dsd'] = sub_dsd

        return out

if __name__ == '__main__':
    import sys
    from pprint import pprint
    x=DataExtractorBase().get_dsd(sys.argv[-1], True)

    print(json.dumps(x, indent=4))
