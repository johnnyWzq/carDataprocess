#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  6 14:10:45 2018

@author: wuzhiqiang
"""

import pymysql
import os
import pandas as pd
import sql_opertaion as sql
import re
import time


def read_bat_info(config, *kwg):
    """
    读取电池基本信息
    """
    conn = sql.create_connection(config)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    rows = {}
    for table_name in kwg:
        if 'bat_info' in table_name:
            query = 'sys_id, code, c_id '
        sql_cmd = 'select ' + query + 'from %s' %table_name
        cursor.execute(sql_cmd)
        rows[table_name] = pd.DataFrame(cursor.fetchall())   
        rowcount = cursor.rowcount
        print(rowcount)
    bat_list = rows['bat_info'][['sys_id','code','c_id']]
    cursor.close()
    sql.close_connection(conn)
    return bat_list

def r_sbat_data(df, *kwg):
    """
    读取电池的单体电压与温度等详细数据
    """
    print('reading the data of single cell...')
    start = time.time()
    if 'uncompress(cell_volt)' in kwg:
        arry0 = df[['uncompress(cell_volt)']].values
        df = df.drop(['uncompress(cell_volt)'], axis=1)
        sv = list()
        for i in range(len(df)):
            #x = str(df0.values[0,:])
            pattern = re.compile(r'\d+')
            num_regex = pattern.findall(str(arry0[i,:]))
            sv.append(num_regex)
        df0 = pd.DataFrame(sv).convert_objects(convert_numeric=True)
        sv = list()
        for i in range(df0.shape[1]):
            sv.append('sv'+str(i))
        df0.columns = sv
        df = pd.concat([df, df0], axis=1)
    if 'uncompress(cell_temp)' in kwg:
        arry0 = df[['uncompress(cell_temp)']].values
        df = df.drop(['uncompress(cell_temp)'], axis=1)
        sv = list()
        for i in range(len(df)):
            #x = str(df0.values[0,:])
            pattern = re.compile(r'\d+')
            num_regex = pattern.findall(str(arry0[i,:]))
            sv.append(num_regex)
        df0 = pd.DataFrame(sv).convert_objects(convert_numeric=True)
        sv = list()
        for i in range(df0.shape[1]):
            sv.append('st'+str(i))
        df0.columns = sv
        df = pd.concat([df, df0], axis=1)
    end = time.time()
    print('The process of reading data is complete, which takes %d seconds.'
              %(end - start))
    return df
        
def rw_bat_data(config, r_table_name, targe_base, id_list):
    """
    读取电池详细的工作数据，并将读出的数据存入batterybase中
    bat_id为数据库定义的电池编码，
    cid为原系统定义的电池编号
    """
    conn = sql.create_connection(config)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    rows = {}
    count = {}
    max_num = 5000000
    query = 'save_time, soc, soh, voltageb, voltagep, current, positive_insulation_resistance, ' + \
            'cathode_insulation_resistance, current_charge_quantity, current_loop_charge_quantity, ' +\
            'current_discharge_quantity, current_loop_discharge_quantity, charge_times, ' + \
            'loop_times, endurance_time, endurance_mileage, ' + \
            'uncompress(cell_volt), uncompress(cell_temp)'#, uncompress(cell_soc)'#, ' + \
            #'uncompress(cell_resistance), uncompress(cell_balance_status)'
    
    columns = 'current', 'voltagep', 'voltageb', 'soh', 'soc', 'save_time'
    """
            'positive_insulation_resistance', 
            'cathode_insulation_resistance', 'current_charge_quantity', 'current_loop_charge_quantity',
            'current_discharge_quantity', 'current_loop_discharge_quantity', 'charge_times',
            'loop_times', 'endurance_time', 'endurance_mileage']
    """
    #for bat_id, cid in v.items():
    start = time.time()
    for i in range(len(id_list)):
        bat_id = id_list.loc[i]['sys_id']
        cid = id_list.loc[i]['c_id']
        if cid == 21 or cid == 22 or cid == 23 or cid == 24:#测试数据
            continue
        sql_cmd = "select count(*) from %s where car_id=%d"%(r_table_name, cid)
        cursor.execute(sql_cmd)#获取数据行数
        row = cursor.fetchall()
        count[bat_id] = row[0]['count(*)']
        print('The number of rows named %s-%s is %d.'%(bat_id, cid , count[bat_id]))
        sql_cmd = 'select ' + query + ' from %s where car_id=%d'%(r_table_name, cid)
        cursor.execute(sql_cmd)
        while count[bat_id] > 0:       
            num = min(max_num, count[bat_id])
            rows[r_table_name] = pd.DataFrame(cursor.fetchmany(num))
            column_list = ['uncompress(cell_volt)', 'uncompress(cell_temp)']#,
                           #'uncompress(cell_soc)',  'uncompress(cell_resistance)',
                           #'uncompress(cell_balance_status)']
            rows[r_table_name] = r_sbat_data(rows[r_table_name], *column_list)
            count[bat_id] = count[bat_id] - num
            engine = sql.create_sql_engine(targe_base, 'root', 'wzqsql', 'localhost', '3306')
            dtypedict = sql.mapping_df_types(rows[r_table_name])
            column_list = rows[r_table_name].columns.tolist()
            for column in columns:
                column_list.remove(column)
                column_list.insert(0, column)
            '''
            df = rows[r_table_name][columns]
            rows[r_table_name] = rows[r_table_name].drop(columns, axis=1)
            rows[r_table_name] = rows[r_table_name].insert(0, columns, df)
            '''
            rows[r_table_name] = rows[r_table_name][column_list]
            rows[r_table_name].to_sql(bat_id, engine, index=False,
                                        if_exists='append', dtype=dtypedict)
            print('writing the data which num is %d in a talbe named %s'%(num, bat_id))
    end = time.time()
    print('The process of reading and writing data is complete, which takes' + \
          '%s seconds.'%(end - start))
    cursor.close()
    sql.close_connection(conn)
        

def main():
    file_dir = os.path.join(os.path.abspath('.'), 'data')
    config = {'s': 'localhost', 'u': 'root', 'p': 'wzqsql', 'db': 'batterybase'}
    bat_list = read_bat_info(config, 'bat_info')
    bat_list.to_csv(os.path.join(file_dir, 'bat_info.csv'), encoding='gb18030')
    config = {'s': 'localhost', 'u': 'root', 'p': 'wzqsql', 'db': 'car_operation_evs_2018'}
    id_list = bat_list[['sys_id', 'c_id']]
    rw_bat_data(config, 'bms_alarm_info', 'batterybase', id_list)
if __name__ == '__main__':
    main()