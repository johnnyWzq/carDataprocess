#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  7 16:45:13 2018

@author: wuzhiqiang
"""

import numpy as np
import pandas as pd


a = np.array([[1,2,3],[4,5,6],[7,8,9]])
b = a[1,:]
c=a[-2:]
print(a)
print(b)
cl = ['a','b','c']
c = pd.DataFrame(a, columns=cl)

c=c[(c['b']>=4) & (c['b']<=6)]
print(c.shape)

d=np.array([[1,2,3,4,5,6],[5,6,7,8,9,0]])

def get_columns(orgin_column, keyword):
    """
    """
    target_list = []
    for stg in orgin_column:
        if keyword in stg:
            target_list.append(stg)
    return target_list

orc=['sv1','sv2','st1','sv3','st2']
ss = get_columns(orc, 'sv')
print(ss)