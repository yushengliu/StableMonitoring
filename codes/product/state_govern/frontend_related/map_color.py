#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/12/3 17:47
@Author  : Liu Yusheng
@File    : map_color.py
@Description: 地图染色
"""
import json
import pandas as pd

from utils.db_base import TableName
from product.state_govern.parameters import db_conf, df_index_conf, legends
from product.state_govern.sg_utils import insert_data
from utils.get_2861_gaode_geo_gov_id_info import df_2861_gaode_geo_all


def get_index_score(index_code, db_obj, get_conn=False):
    """
    @功能：获取某指标的指数值
    :param index_code:
    :param db_obj:
    :param get_conn:
    :return:
    """
    # 取pct_rank来渲染地图
    sqlstr_= "SELECT gov_id, (data ->> 'pctr') ::FLOAT AS score, version FROM {} WHERE product_name = 'SG' AND type_code = '{}'".format(TableName.PWTreeData, index_code)

    if get_conn:
        db_obj.get_conn()

    datas = db_obj.read_from_table(sqlstr_)

    if get_conn:
        db_obj.disconnect()

    return datas


def get_grade_name(color_value):
    """
    @功能：根据分值，判断所处等级
    :param color_value:
    :return:
    """
    legends_df = pd.DataFrame(legends)
    legends_df = legends_df.sort_values(by="value", ascending=False)

    for index, row in legends_df.iterrows():
        if color_value >= row["value"]:
            # print(row["name"])
            return row["name"]
        else:
            continue


def get_map_color(index_code, index_name, db_obj):

    index_scores = get_index_score(index_code, db_obj, False)

    df_index = pd.DataFrame(index_scores)

    df_index = df_index.set_index("gov_id", drop=True)
    df_index["full_name"] = df_2861_gaode_geo_all["full_name"]
    df_index["region_type"] = df_2861_gaode_geo_all["gov_type"]
    df_index["region_type"] = df_index["region_type"].apply(lambda x: "省（直辖市）" if x in [0,2] else "市" if x in [1, 31] else "区县")

    node_name = index_code if index_code == "sg" else "sg_{}".format(index_code)
    df_index["node_name"] = node_name.upper()

    df_index["grade"] = df_index["score"].apply(lambda x: get_grade_name(x))
    df_index["value"] = df_index.apply(lambda x: json.dumps({"full_name": x["full_name"], "color": x["score"], "grade": "[{}] - {}".format(index_name+"成效",x["grade"]), "desc": "领先全国{}%的{}".format(x["score"], x["region_type"])}, ensure_ascii=False), axis=1)

    # 把gov_id放出来
    df_index = df_index.reset_index()

    insert_data(df_index, db_obj, TableName.PWMapColor, False)
    print("mapcolor数据插入完毕！index_code={}".format(index_code),flush=True)


def map_color_main(test_mode=True):
    db_obj = db_conf[test_mode]
    df_index_conf_ = df_index_conf[df_index_conf.index_level.isin([0,1])]

    db_obj.get_conn()
    for index, row in df_index_conf_.iterrows():
        get_map_color(index, row["index_name"], db_obj)
    db_obj.disconnect()


if __name__ == "__main__":
    map_color_main(test_mode=False)




