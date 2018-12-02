#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 10:02:12 2018

@author: wuzhiqiang
"""

import os
import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt
from rw_statis_package import save_processed_data

def read_data(file_dir, file_name):
    print('reading processed_data...')
    start = time.time()
    data_dict = pd.read_excel(os.path.join(file_dir, file_name), 
                                 sheet_name=None, encoding='gb18030')
    end = time.time()
    print('Finished, it took %d seconds to read the data.'%(end-start))
    return data_dict

def contact_column_name(x, y, l=[]):
    pp = l
    for p1 in x:
        for p2 in y:
            pp.append(p1+p2)
    return pp

def abnormal_check(df):
    if df.empty is False:
        statistics = df.describe(include=[np.number])
        statistics.loc['range'] = statistics.loc['max'] - statistics.loc['min']#极差
        statistics.loc['var'] = statistics.loc['std'] - statistics.loc['mean']#变异系数
        statistics.loc['dis'] = statistics.loc['75%'] - statistics.loc['25%']#四分位数间距
        return statistics
    
def UDC_analysis(df):
    """
    urban driving cycle analysis
    """
    print('selecting and clearing the columns of data...')
    data = df.loc[:]
    data['operation_time'] = data['data_num'] // 10 #取整
    data['delta_soc'] = data['soc_max'] - data['soc_min']
    data['regular_disc_cur'] = data['current_mean'] - data['current_std']
    data = data.rename(columns = {'soc_max': 'start_soc', 'soc_min': 'end_soc',
                  'voltageb_max': 'max_volt', 'voltageb_min': 'min_volt',
                  'current_mean': 'mean_disc_cur', 'current_min': 'max_disc_cur',
                  'current_max': 'max_c_cur',
                  'max_sv_mean': 'mean_max_sv', 'min_sv_mean': 'mean_min_sv',
                  'max_st_mean': 'mean_max_st', 'min_st_mean': 'mean_min_st'})
    data = data[['operation_time', 'delta_soc',
                'start_soc', 'end_soc', 'max_volt', 'min_volt', 
                'regular_disc_cur', 'mean_disc_cur', 'max_disc_cur', 'max_c_cur',
                'mean_max_sv', 'mean_min_sv',
                'mean_max_st', 'mean_min_st']]
    MAX_VOLTAGE = 450000
    MIN_VOLTAGE = 290000
    MAX_SINGLE_VOLTAGE = 4500
    MIN_SINGLE_VOLTAGE = 2800
    MAX_TEMPERATURE = 200
    MIN_TEMPERSTURE = -40
    data = data[(data['max_volt'] >= MIN_VOLTAGE) & (data['max_volt'] <= MAX_VOLTAGE)]
    data = data[(data['min_volt'] >= MIN_VOLTAGE) & (data['min_volt'] <= MAX_VOLTAGE)]
    data = data[(data['mean_min_sv'] >= MIN_SINGLE_VOLTAGE) & (data['mean_min_sv'] <= MAX_SINGLE_VOLTAGE)]
    data = data[(data['mean_min_sv'] >= MIN_SINGLE_VOLTAGE) & (data['mean_min_sv'] <= MAX_SINGLE_VOLTAGE)]
    data = data[(data['mean_max_st'] >= MIN_TEMPERSTURE) & (data['mean_max_st'] <= MAX_TEMPERATURE)]
    data = data[(data['mean_min_st'] >= MIN_TEMPERSTURE) & (data['mean_min_st'] <= MAX_TEMPERATURE)]
    data = data[data['delta_soc'] > 0]
    func = lambda x: x.fillna(method='ffill').fillna(method='bfill').dropna()
    data = func(data)
    return data
    
def visualization(df):
    """
    """
    fig = plt.figure()#(figsize=(16,4))
    plt.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False #用来正常显示负号
    df['data_num'].plot(kind='box')
    fig.show()

def main():
    file_dir = os.path.join(os.path.abspath('.'), 'data')
    file_name = 'processed_data.xlsx'
    #"""
    #已保存
    data_dict = read_data(file_dir, file_name)
   # """
    """
    stat_dict = {}
    for key, value in data_dict.items():
        if value.empty is True:
            continue
        stat_dict[key] = abnormal_check(value)
        print(stat_dict[key])
    save_processed_data(stat_dict, 'statis_data', file_dir)
    """
    #"""
    udc_dict = {}
    for key, value in data_dict.items():
        if value.empty is True:
            continue
        udc_dict[key] = UDC_analysis(value)
    save_processed_data(udc_dict, 'UDC_data', file_dir)
   # """
    """
    stat_dict = read_data(file_dir, 'statis_data.xlsx')
    p1_list = ['voltageb', 'current', 
                 'min_sv', 'max_sv', 'mean_sv', 'median_sv',
                 'min_st', 'max_st', 'mean_st', 'median_st']
    p2_list = ['_min', '_max', '_mean', '_median']
    para_list = contact_column_name(p1_list, p2_list, ['data_num'])
    print(para_list)
    for key, value in stat_dict.items():
        if value.empty is True:
            continue
        visualization(value[para_list])
    """
    
if __name__ == '__main__':
    main()