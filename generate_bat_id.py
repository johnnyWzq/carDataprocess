#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  1 14:09:28 2018

@author: wuzhiqiang
"""

import hashlib
import datetime
import numpy as np
import pandas as pd


def create_bat_id(seeds):
    """
    #创建数据库系统里唯一电池编号
    x1~x4:w+
    x5~x36:md5(seed)
    
    """
    df = pd.DataFrame(columns=['sys_id'])
    seeds = seeds.apply(str)
    i = 0
    for seed in seeds:
        md5 = hashlib.md5()
        md5.update(seed.encode('utf-8'))
        x = 'w0' + str(datetime.datetime.now().microsecond)[-2:]
        df.loc[i, ['sys_id']] = x + md5.hexdigest()
        i = i + 1
    return df
    
def main():
    c_id = pd.DataFrame(np.random.randint(low=20, high=50, size=(5, 1)),
                         columns=['c_id'])
    df = create_bat_id(c_id['c_id'])
    df = df.join(c_id)
    
if __name__ == '__main__':
    main()