#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/12/3 17:21
@Author  : Liu Yusheng
@File    : parameters.py
@Description: 一些参数配置 - SG
"""
import pandas as pd
from datetime import datetime

import utils.path_manager as pm
from utils.db_base import TableName, DBShortName, DBObj

sg_file_path = pm.LOCAL_STABLE_PROJECT_STORAGE + "state_govern/" + "files/"
sg_data_path = pm.LOCAL_STABLE_PROJECT_STORAGE + "state_govern/" + "data/"

leaf_conf = "SG_leaf_conf.csv"
index_conf = "SG_index_conf.csv"

df_leaf_conf = pd.read_csv(sg_file_path+leaf_conf, encoding="GBK", index_col="index_code")
df_index_conf = pd.read_csv(sg_file_path+index_conf, encoding="GBK", index_col="index_code")


PWDataTestObj = DBObj(DBShortName.ProductPWDataTest).obj
PWDataFormalObj = DBObj(DBShortName.ProductPWDataFormal).obj

db_conf = {True: PWDataTestObj, False: PWDataFormalObj}

treedata_insert_sqlstr = "INSERT INTO %s (gov_id, product_name, type_code, data, version, update_time) VALUES ({gov_id}, '{product_name}', '{type_code}', '{data}',  '{version}', '%s') ON CONFLICT (gov_id, product_name, type_code) DO UPDATE SET data = '{data}', version = '{version}', update_time = '%s';"%(TableName.PWTreeData, datetime.now(), datetime.now())

mapcolor_insert_sqlstr = "INSERT INTO %s (gov_id, node_name, value, version) VALUES ({gov_id}, '{node_name}', '{value}', '{version}') ON CONFLICT (gov_id, node_name) DO UPDATE SET value = '{value}', version = '{version}';"%(TableName.PWMapColor)


# 灰 - 红
colors_s = ['#808080', '#A9A9A9', '#BCBCBC', '#DEDEDE', '#EBEBEB', '#FEF2F2', '#FCADAF', '#FB777B', '#FA3F48', '#FF0000']
legends = [{"value": 0, "color": colors_s[3], "name": "差"},
           {"value": 25, "color": colors_s[5], "name": "普通"},
           {"value": 50, "color": colors_s[7], "name": "较好"},
           {"value": 75, "color": colors_s[9], "name": "很好"},
           ]


county_missing_leafs = ['X0137', 'X0052', '3020300506', 'X0040', 'X0060', 'X0063', 'X0066', '9030200287']
county_n_city_missing_leafs = ['X0137', '3020300506', 'X0040']
# county_n_city_n_prov_missing_leafs = ['3020300506']


if __name__ == "__main__":
    db_obj = db_conf[False]

    print(db_obj)