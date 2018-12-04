#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 22 11:25:10 2018
#将电池包当作对象进行数据预处理
#每条数据形成新的一行数据
@author: wuzhiqiang
"""

import pymysql
import os
import pandas as pd
import numpy as np
import sql_operation as sql
import re
import time
from dateutil import parser
from read_bat_data import read_bat_info

def get_columns(orgin_column, keyword):
    """
    从原有的列表中选出匹配kwyword的列表
    """
    target_list = []
    for stg in orgin_column:
        if re.match(r'%s\d+'%keyword, stg):
            target_list.append(stg)
    return target_list

def calc_statis(df, keyword):
    """
    按行计算df的统计值
    """
    #求统计值
    min_lst = df.T.min()
    max_lst= df.T.max()
    mean_lst = df.T.mean()
    std_lst = df.T.std()
    median_lst = df.T.median()
    
    tmp = pd.DataFrame({'min_'+keyword: min_lst, 'max_'+keyword: max_lst,
                        'mean_'+keyword: mean_lst, 'std_'+keyword: std_lst,
                        'median_'+keyword: median_lst})
    
    return tmp

def calc_ic(df):
    """
    #计算dq/dv值
    #由于没有dq值，因此使用i代替
    #计算dq/dv/sv_
    """
    #func=lambda x: x+1 if x==0 else x
    df['dqdv'] = df['current'] / df['voltageb'].diff()
    df['dqdv_min_sv'] = df['dqdv'] / df['min_sv'].diff()
    df['dqdv_max_sv'] = df['dqdv'] / df['max_sv'].diff()
    df['dqdv_mean_sv'] = df['dqdv'] / df['mean_sv'].diff()
    df['dqdv_median_sv'] = df['dqdv'] / df['median_sv'].diff()
    df['dqdv_std_sv'] = df['dqdv'] / df['std_sv'].diff()
    
    
def split_data(file_dir, data_dict=None):
    """
    #将预处理后的数据按一次充放电过程进行分割合并和处理
    #大于VALID_TIMEGAP的为新的过程，小于INVALID_TIMEGAP为无效过程
    """
    CHARGE_TIMEGAP = 300  # 300 seconds = 5 minutes
    CHARGING_TIMEGAP = 60 #1分钟的舍弃
    DROPNA_THRESH = 12
    
    if data_dict is None:
        print('reading processed_data...')
        start = time.time()
        data_dict = pd.read_excel(os.path.join(file_dir, 'preprocessed_data.xlsx'), 
                                     sheet_name=None, encoding='gb18030')
        end = time.time()
        print('Finished, it took %d seconds to read the data.'%(end-start))
    for key, df in data_dict.items():
        #filt samples on rows, if a row has too few none-nan value, drop it
        df = df.dropna(thresh=DROPNA_THRESH)
        
        df['time'] = df['time'].apply(str)
        df['time'] = df['time'].apply(lambda x: parser.parse(x))
         # group by bms_id and sort by time
        #data = pd.DataFrame(columns=['bms_id', 'start_time', 'end_time',
        #                             'charger_id', 'data_num'])
        data = pd.DataFrame()
        cnt = 0  
        df = df.sort_values('time')
        j_last = 0
        for j in range(1, len(df) + 1):
            if j >= len(df) or (df.iloc[j]['time'] - df.iloc[j - 1]['time']).seconds > CHARGE_TIMEGAP:
                    
                if j >= len(df):
                    cur_df = df.iloc[j_last:]
                elif (df.iloc[j]['time'] - df.iloc[j - 1]['time']).seconds > CHARGE_TIMEGAP:
                    cur_df = df.iloc[j_last:j]
                    #j_last = j
    
                func = lambda x: x.fillna(method='ffill').fillna(method='bfill').dropna()
                cur_df = func(cur_df)
                
                print('clip %d : j: %d -> %d, the length of cur_df: %d.'
                      %(cnt, j_last, j, len(cur_df)))
                #print('j:', j_last, '->', j, 'len(cur_df):', tmp_len, '->', len(cur_df), 'cnt=', cnt)
                j_last = j
                if len(cur_df) <= 0 or (cur_df['time'].iloc[-1] - cur_df['time'].iloc[0]).seconds < CHARGING_TIMEGAP:
                    continue
                
                calc_ic(cur_df)
                data = data.append(transfer_data(cnt, cur_df))
                cnt += 1
        #data.drop(['median_sv_diff_median', 'median_sv_diff2_median'], axis=1) #全是0
        
        data_dict[key] = data
    #data.to_csv(p_data_dir + 'data.csv', encoding='gb18030', index=False)
    return data_dict

def transfer_data(cnt, cur_df):
    """
    将2维的df转换为1维
    """
    df = pd.DataFrame(columns=['start_time', 'end_time',
                               'data_num'])
    df.loc[cnt, 'start_time'] = cur_df['time'].iloc[0]
    df.loc[cnt, 'end_time'] = cur_df['time'].iloc[-1]
    df.loc[cnt, 'data_num'] = len(cur_df)
    
    for col_name in cur_df.columns:
        for fix in ['soc', 'voltageb', 'current', '_sv', '_st', 'dqdv']:
            if fix in col_name:
                cal_stat_row(cnt, cur_df[col_name], col_name, df)
                cal_stat_row(cnt, cur_df[col_name].diff(), col_name + '_diff', df)
                cal_stat_row(cnt, cur_df[col_name].diff().diff(), col_name + '_diff2', df)
                cal_stat_row(cnt, cur_df[col_name].diff() / cur_df[col_name], col_name + '_diffrate', df)
    return df

def cal_stat_row(cnt, ser, col_name, df):
    """
    按每一行求统计值
    """
    df.loc[cnt, col_name + '_mean'] = ser.mean(skipna=True)
    df.loc[cnt, col_name + '_min'] = ser.min(skipna=True)
    df.loc[cnt, col_name + '_max'] = ser.max(skipna=True)
    df.loc[cnt, col_name + '_median'] = ser.median(skipna=True)
    df.loc[cnt, col_name + '_std'] = ser.std(skipna=True)
    
def process_data(data, *kwg):
    """
    #对data中的各项值进行初步清洗，
    #并且计算单体的统计值替换原序列值
    #和soc等一起放入data0
    """
    data = data.rename(columns={'save_time': 'time'})
    #对每个column进行清洗
    print('cleaning data...')
    data = data[data['soc'] >= 0] #删除soc小于0的行
    data = data[data['soh'] >= 0]
    data = data[data['voltageb'] >= 0]
    data = data[data['voltagep'] >= 0]
    #data = data[data['current'] <= 300]
    func = lambda x: x.fillna(method='ffill').fillna(method='bfill').dropna()
    data = func(data)
    data0 = data[['time', 'soc', 'soh', 'voltageb', 'current']]
    
    print('preporcessing data...')
    if 'sv' in kwg:
        sv = data[get_columns(data.columns.tolist(), 'sv')]
        st_sv = calc_statis(sv, 'sv')
        data0 = pd.concat([data0, st_sv], axis=1)
    if 'st' in kwg:
        st = data[get_columns(data.columns.tolist(), 'st')]
        st_st = calc_statis(st, 'st')
        data0 = pd.concat([data0, st_st], axis=1)
    
    return data0

def preprocess_package_data(config, bat_list, starttime, endtime):
    """
    从数据库中读取原始数据，再按id分别进行处理
    放入data_dict中
    """
    conn = sql.create_connection(config)
    data_dict = {}
    for bat_id in bat_list:
        start = time.time()
        if(sql.table_exists(conn, bat_id) != 1):
            print("There isn't a table named %s."%bat_id)
            continue
        sql_cmd = "select * from %s where save_time between " \
                "'%s' and '%s'" % (bat_id, starttime, endtime)
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        print('Reading the database...')
        cursor.execute(sql_cmd)
        #temp = cursor.fetchall()
        rowcount = cursor.rowcount
        if rowcount == 0:
            print("There is no data in table named %s."%bat_id)
            continue
        df0 = pd.DataFrame(cursor.fetchall())
        print('Finishing reading the database, and the rows of data is %d.'%rowcount)
        df0 = process_data(df0, 'sv', 'st')
        data_dict[bat_id] = df0
        end = time.time()
        print('The process of preprocessing the %s is complete, whick takes %d '\
              'secondes.' %(bat_id, (end-start)))
    return data_dict

def save_processed_data(data_dict, file_name, output_dir=None):
    if output_dir:
        writer = pd.ExcelWriter(os.path.join(output_dir, '%s.xlsx'%file_name))
    for key, value in data_dict.items():
        #保存处理后的数据
        if output_dir:
            value.to_excel(writer, key[-31:])
    writer.save()
            
def main():
    file_dir = os.path.join(os.path.abspath('.'), 'data')
    config = {'s': 'localhost', 'u': 'root', 'p': 'wzqsql', 'db': 'batterybase'}
    bat_list = read_bat_info(config, 'bat_info')
    id_list = bat_list['sys_id'].tolist()
    print(id_list)
    #preprocessed_data = preprocess_package_data(config, id_list, '2018-1-1', '2018-12-30')
    #save_processed_data(preprocessed_data, 'preprocessed_data', file_dir)
    processed_data = split_data(file_dir)
    save_processed_data(processed_data, 'processed_data', file_dir)
    
if __name__ == '__main__':
    main()