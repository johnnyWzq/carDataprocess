#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  1 18:24:21 2018

@author: wuzhiqiang
"""

import os
import pandas as pd

def main():
    file_dir = os.path.join(os.path.abspath('.'), 'data')
    file_name = 'UDC_data.xlsx'
    data_dict = pd.read_excel(os.path.join(file_dir, file_name), 
                                 sheet_name=None, encoding='gb18030')
    df = pd.DataFrame()
    for key, value in data_dict.items():
        df = df.append(value, ignore_index=True)
    df.to_csv(file_dir + '/UDC_data_concat.csv', encoding='gb18030')#, index=False)
    
if __name__ == '__main__':
    main()