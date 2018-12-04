#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 09:43:33 2018
#计算每一次过程的电池性能评分值，作为标签
@author: wuzhiqiang
"""

import os
import pandas as pd
import numpy as np
from analysis_data import read_data
import time
import utils as ut

from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
    
def calc_per_degradation(data):
    """
    #计算每次过程性能衰减量，%，
    """
    DOD_X_100_RATIO = 0.0167 #DOD大于60%则视为等价于一次100%DOD的衰减，否则等于x*r/100
    X_1C_RATIO = 0.06 #系数
    X_1C_IDENTIFY = 0.5 #0.5C为一个变化量
    TEMP_RATIO = 0.1 #系数
    TEMP_IDENTIFY = 10 #10度为一个变化量
    TEMP_STANDARD = 35
    C_RATIO = 1
    PER_DEGRADATION = 0.001
    START_SOH = 0.99
    
    #calculate degradation by one c/d process
    #DOD
    temp = data['delta_soc']
    temp = temp * DOD_X_100_RATIO
    temp[temp>=1] = 1
    data['dod_score'] = temp.round(4)
    
    #single voltage
    temp = data['max_sv_mean']
    temp = 0.5 ** ((4200 - temp) / 100)
    data['high_v_score'] = temp.round(4)
    
    temp = data['min_sv_mean']
    temp = 0.15 ** ((temp - 2900) / 100)
    data['low_v_score'] = temp.round(4)
    
    #temperature
    temp = data['max_st_mean']
    temp.loc[temp<TEMP_STANDARD] = 1 - (((TEMP_STANDARD - temp) // TEMP_IDENTIFY) * TEMP_RATIO)#小于必须放前面，否则因为符合判定条件后面的计算会将本次计算的结果再一次计算
    temp.loc[temp>=TEMP_STANDARD] = 1 + (((temp - TEMP_STANDARD) // TEMP_IDENTIFY) * TEMP_RATIO)#温度高，容量大
    data['temp_score'] = temp.round(4)
    
    #c/d ratio
    temp = data['dis_c']
    temp.loc[temp<C_RATIO] = 1 + (((C_RATIO - temp) // X_1C_IDENTIFY) * X_1C_RATIO)
    temp.loc[temp>=C_RATIO] = 1 - (((temp - C_RATIO) // X_1C_IDENTIFY) * X_1C_RATIO)
    data['c_score'] = temp.round(4)
    
    ##calculate score
    print('calculating the degradation score of per process...')
    start = time.time()
    score_ratio = {'dod': 0.05, 'high_v': 0.4, 'low_v': 0.4, 'temp': 0.1, 'dis_c': 0.05}
    temp = data['dod_score'] * score_ratio['dod'] + \
          data['high_v_score'] * score_ratio['high_v'] + \
          data['low_v_score'] * score_ratio['low_v'] + \
          data['temp_score'] * score_ratio['temp'] + \
          data['c_score'] * score_ratio['dis_c']
    data['degradation_score'] = temp.round(4)
    
    data['score'] = START_SOH
    data['score'].iloc[0] = data['score'].iloc[0] - \
                            PER_DEGRADATION * data['degradation_score'].iloc[0]
    for i in range(1, len(data)):
        data['score'].iloc[i] = data['score'].iloc[i - 1] - \
                            PER_DEGRADATION * data['degradation_score'].iloc[i]
    end = time.time()
    print('Finished, it took %d seconds to read the data.'%(end-start))
    
    return data
    
def calc_score(data):
    """
    #计算每次性能得分
    """
    
    MIN_DELTA_SOC = 1
    RATE_CAPACITY = 101750
    
    data['delta_soc'] = data['soc_max'] - data['soc_min']
    data = data[data['delta_soc'] > MIN_DELTA_SOC]
    data['regular_disc_cur'] = data['current_mean'] - data['current_std']
    data['dis_c'] = data['regular_disc_cur'] / RATE_CAPACITY
    data['dis_c'] = data['dis_c'].abs().round(2)
    
    data = calc_per_degradation(data)

    return data

def calc_feature_data(data_dict):
    """
    """
    data = pd.DataFrame()
    for key, value in data_dict.items():
        if value.empty is True:
            continue
        data_dict[key] = calc_score(value)
        data_dict[key]['id'] = key 
        data = data.append(data_dict[key], ignore_index=True)
        
    data = calc_feature(data)
    
    data_x = data[[i for i in data.columns if 'feature_' in i]]
    data_y = data['score']
    data = data['id']
    
    data = pd.concat([data, data_x, data_y], axis=1)
    
    return data

def calc_feature(data):
    """
    """
    invaid_thresh = data.shape[0] // 4 * 3
    data = data.dropna(axis='columns', how='all')
    print(data.shape)
    data = data.dropna(axis='columns', thresh=invaid_thresh)
    print(data.shape)
    data = data.T[~data.isin([-np.inf, np.inf]).all().values]#删除所有行均为-inf,inf的列
    data = data[data.T.all().values]
    data = data.T
    print(data.shape)
    data = data.replace(np.inf, np.nan)
    data = data.replace(-np.inf, np.nan)
    data = data.dropna(axis='columns', thresh=invaid_thresh)
    print(data.shape)
    data = data.fillna(data.mean())
    
    col_names = []
    for i in data.columns:
        for j in ['data_num', 'voltageb_', 'current_', '_sv_', '_st_', 'dqdv_']:
            if j in i:
                col_names.append(i)
                break
    for i in col_names:
        tmp = data[i]
        data['feature_' + i] = tmp
        
    return data

def select_feature(data_x, data_y, feature_num=40, method='f_regression'):
    features_chosen = data_x.columns
    feature_num = min(len(features_chosen), feature_num)
    
    #根据特征工程的方法选择特征参数数量
    from sklearn.feature_selection import SelectKBest
    from sklearn.feature_selection import f_regression
    from sklearn.decomposition import PCA
    from sklearn.feature_selection import mutual_info_regression
    
    if method == 'f_regression' or method == 'mutual_info_regression':
        if method == 'f_regression':
            select_model = SelectKBest(f_regression, k=feature_num)
        else:
            select_model = SelectKBest(mutual_info_regression, k=feature_num)
        select_model.fit(data_x.values, data_y.values.ravel())
        feature_mask = select_model.get_support(indices=True)
        feature_chosen = data_x.columns[feature_mask]
        print('feature_chosen: ', feature_chosen)
        data_x = data_x[feature_chosen]
    elif method == 'PCA':
        pca_model = PCA(n_components=feature_num)
        data_x_pc = pca_model.fit(data_x.values).transform(data_x.values)
        data_x = pd.DataFrame(data=data_x_pc,
                      columns=['PCA_' + str(i) for i in range(feature_num)])
    else:
        raise Exception('In select_feature(): invalid parameter method.')
    
    return data_x

def build_model(file_dir, data=None, split_mode='test',
                feature_method='f_regression', feature_num=40):
    if data is None:
        print('reading feature_data...')
        start = time.time()
        data = pd.read_excel(os.path.join(file_dir, 'feature_data.xlsx'), 
                                     encoding='gb18030')
        end = time.time()
        print('Finished, it took %d seconds to read the data.'%(end-start))

    data_x = data[[i for i in data.columns if 'feature_' in i]]
    data_y = data['score']
    
     # standardize
    data_x = pd.DataFrame(data=preprocessing.scale(data_x.values, axis=0), columns=data_x.columns)

    # select features
    # feature_num: integer that >=0
    # method: ['f_regression', 'mutual_info_regression', 'pca']
    data_x = select_feature(data_x, data_y, method=feature_method, feature_num=feature_num)
    
    # start building model
    np_x = np.nan_to_num(data_x.values)
    np_y = np.nan_to_num(data_y.values)
    print('train_set.shape=%s, test_set.shape=%s' %(np_x.shape, np_y.shape))

    pkl_dir = os.path.join(os.path.abspath('.'), 'pkl')
    res = {}
    if split_mode == 'test':
        x_train, x_val, y_train, y_val = train_test_split(data_x, data_y, test_size=0.2,
                                                          shuffle=True)
        model = LinearRegression()
        res['lr'] = ut.test_model(model, x_train, x_val, y_train, y_val)
        ut.save_model(model, data_x.columns, pkl_dir)
        model = DecisionTreeRegressor()
        res['dt'] = ut.test_model(model, x_train, x_val, y_train, y_val)
        ut.save_model(model, data_x.columns, pkl_dir, depth=3)
        model = RandomForestRegressor()
        res['rf'] = ut.test_model(model, x_train, x_val, y_train, y_val)
        ut.save_model(model, data_x.columns, pkl_dir, depth=3)
        model = GradientBoostingRegressor()
        res['gbdt'] = ut.test_model(model, x_train, x_val, y_train, y_val)
        ut.save_model(model, data_x.columns, pkl_dir)
    elif split_mode == 'cv':
        model = LinearRegression()
        res['lr'] = ut.cv_model(model, np_x, np_y)
        model = DecisionTreeRegressor()
        res['dt'] = ut.cv_model(model, np_x, np_y)
        model = RandomForestRegressor()
        res['rf'] = ut.cv_model(model, np_x, np_y)
        model = GradientBoostingRegressor()
        res['gbdt'] = ut.cv_model(model, np_x, np_y)
    else:
        print('parameter mode=%s unresolved' % (model))
        
    return res
    
def main():
    file_dir = os.path.join(os.path.abspath('.'), 'data')
    """
    #已完成
    file_name = 'processed_data.xlsx'#'test_data.xlsx'
    data_dict = read_data(file_dir, file_name)

    feature_data = calc_feature_data(data_dict)
    feature_data.to_excel(os.path.join(file_dir, 'feature_data.xlsx'))
    """  
    mode = 'test'
    res = build_model(file_dir, split_mode=mode)
    if mode == 'test':
        d = {'lr':'线性回归(LR)', 'dt':'决策树回归', 'rf':'随机森林', 'gbdt':'GBDT',
            'eva':'评估结果'}
        writer = pd.ExcelWriter(os.path.join(file_dir, 'result.xlsx'))
        eva = pd.DataFrame()
        for s in res:
            print(s)
            res[s]['train'].to_excel(writer, d[s])
            res[s]['test'].to_excel(writer, d[s], startcol=3)
            eva = eva.append(res[s]['eva'])
        eva = eva[['type', 'EVS', 'MAE', 'MSE', 'R2']]
        eva.to_excel(writer, d['eva'])
    
if __name__ == '__main__':
    main()