#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/12/3 19:45
@Author  : Liu Yusheng
@File    : sg_utils.py
@Description: 通用
"""
import product.state_govern.parameters as sg_para
from utils.db_base import TableName


def insert_data(data_df, db_obj, table_name, get_conn=False):

    if table_name == TableName.PWTreeData:
        sqlstr_head = sg_para.treedata_insert_sqlstr
    elif table_name == TableName.PWMapColor:
        sqlstr_head = sg_para.mapcolor_insert_sqlstr

    else:
        return False

    sqlstr_list = [sqlstr_head.format(**row) for index, row in data_df.iterrows()]

    if get_conn:
        db_obj.get_conn()

    row_num = len(sqlstr_list)

    for i in range(0, row_num, 10000):
        db_obj.execute_any_sql("".join(sqlstr_list[i: i+10000]))
        print("&&&&&&&&&&&&&&&&&&已插入{}：{}条&&&&&&&&&&&&&&&&&".format(table_name, len(sqlstr_list[i: i + 10000])), flush=True)

    if get_conn:
        db_obj.disconnect()
