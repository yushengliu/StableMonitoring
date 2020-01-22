#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/11/26 10:10
@Author  : Liu Yusheng
@File    : sychron_tree_data.py
@Description: 同步树状结构数据
"""
import json
import pandas as pd
from datetime import datetime

from utils.db_base import DBObj, TableName, DBShortName
from utils.get_2861_gaode_geo_gov_id_info import df_2861_gaode_geo_all

table_name = "pw_tree_data"
db_247 = DBObj(DBShortName.ProductPWFormal).obj
db_46 = DBObj(DBShortName.ProductPWDataTest).obj
db_83 = DBObj(DBShortName.ProductPWDataFormal).obj


def get_tree_data_from_247(product_name, gov_id, get_conn=False):
    # db_247 = DBObj(DBShortName.ProductPWFormal).obj

    sqlstr = "SELECT * FROM {} WHERE product_name = '{}' AND gov_id = {};".format(table_name, product_name, int(gov_id))

    if get_conn:
        db_247.get_conn()

    datas = db_247.read_from_table(sqlstr)

    if get_conn:
        db_247.disconnect()

    return datas


def insert_tree_data_into_db(datas, db_obj, db_short, get_conn=False):

    sqlstr_head = "INSERT INTO %s (gov_id, product_name, type_code, data, version, update_time) VALUES ({gov_id}, '{product_name}', '{type_code}', '{data}', '{version}', '%s') ON CONFLICT (gov_id, product_name, type_code) DO UPDATE SET update_time = '%s';"%(table_name, datetime.now(), datetime.now())
    sqlstr_list = [sqlstr_head.format(**row) for row in datas]

    # db_obj = DBObj(db_short).obj

    if get_conn:
        db_obj.get_conn()

    row_num = len(sqlstr_list)

    for i in range(0, row_num, 10000):
        db_obj.execute_any_sql("".join(sqlstr_list[i: i+10000]))
        print("&&&&&&&&&&&&&&&&&&已插入{}：{}条&&&&&&&&&&&&&&&&&".format(db_short, len(sqlstr_list[i: i+10000])), flush=True)

    if get_conn:
        db_obj.disconnect()


def sychron_main():
    product_names = ["MONG", "SinoUS", "EVENT", "OPINION"]
    gov_ids = [0] + df_2861_gaode_geo_all.index.values.tolist()
    # print(len(gov_ids))

    for gov_id in gov_ids:
        if gov_id <= 2702:
            continue
        db_247.get_conn()
        db_46.get_conn()
        db_83.get_conn()
        for product_name in product_names:
            for retry in range(3):
                datas = get_tree_data_from_247(product_name, gov_id)
                if isinstance(datas, list):
                    break
                else:
                    db_247.disconnect()
                    db_247.get_conn()
                    continue
            if not len(datas):
                print("product={}   gov_id={} 没有tree_data！".format(product_name, gov_id), flush=True)
                continue
            data_df = pd.DataFrame(datas)
            data_df["data"] = data_df["data"].apply(lambda x: json.dumps(x))
            datas_ = data_df.to_dict(orient="records")
            if len(datas):
                insert_tree_data_into_db(datas_, db_46, DBShortName.ProductPWDataTest)
                insert_tree_data_into_db(datas_, db_83, DBShortName.ProductPWDataFormal)
        db_247.disconnect()
        db_46.disconnect()
        db_83.disconnect()
        print("gov_id={} 数据同步完毕！".format(gov_id), flush=True)


if __name__ == "__main__":
    # datas = get_tree_data_from_247("EVENT", 0, True)
    #
    # print(datas)

    sychron_main()
