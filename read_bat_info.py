#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  1 19:59:43 2018

@author: wuzhiqiang
"""

import pymysql
import os
import pandas as pd
import sql_opertaion as sql
import generate_bat_id as gbid

def read_car_info(conn, *kwg):
    """
    读取车辆基本信息和电池基本信息
    """
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    rows = {}
    for table_name in kwg:
        if 'car_info' in table_name:
            query = 'c_id, plate_number, vin_code, car_type, factory, ' + \
                'battery_type, energy_type, capacity, charge_times, mileage,' + \
                'uses, run_days, run_time '
        if 'bms_info' in table_name:
            query = 'sid, code, power_type '
            
        sql_cmd = 'select ' + query + 'from %s' %table_name
        cursor.execute(sql_cmd)
        rows[table_name] = pd.DataFrame(cursor.fetchall())   
        rowcount = cursor.rowcount
        print(rowcount)
    if 'car_info' in kwg and 'bms_info' in kwg:
        df = rows['car_info'].merge(rows['bms_info'], left_on='c_id', right_on='sid')
        df['sys_id'] = gbid.create_bat_id(df['c_id'])
        df = df[['sys_id', 'code', 'c_id', 'plate_number', 'vin_code', 'capacity', 
                 'battery_type', 'factory', 'charge_times', 'run_days', 'mileage']]
    else:
        df = None 
    cursor.close()
    return df
    

def main():
    file_dir = os.path.join(os.path.abspath('.'), 'data')
    db_name = 'car_operation_evs_2018'
    config = {'s': 'localhost', 'u': 'root', 'p': 'wzqsql', 'db': db_name}
    conn = sql.create_connection(config)
    '''
    create_table_sql = """CREATE TABLE test(
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                code VARCHAR(50) UNIQUE,c_id INT NOT NULL UNIQUE,
                                plate_number VARCHAR(50) UNIQUE,
                                capacity INT,
                                battery_type INT,
                                factory VARCHAR(200),
                                charge_times INT,
                                run_days INT,
                                mileage INT)"""
    create_table(conn, create_table_sql, 'test')
    '''
    res = read_car_info(conn, 'car_info', 'bms_info')
    if res.empty != True:
        res.to_csv(os.path.join(file_dir, 'bat_info_detail.csv'), encoding='gb18030')
        mybatbase = 'batterybase'
        engine = sql.create_sql_engine(mybatbase, 'root', 'wzqsql', 'localhost', '3306')
        dtypedict = sql.mapping_df_types(res)
        res.to_sql('bat_info', engine, if_exists='replace', dtype=dtypedict)
    sql.close_connection(conn)
    
if __name__ == '__main__':
    main()