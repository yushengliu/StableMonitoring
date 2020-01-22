#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/12/3 15:29
@Author  : Liu Yusheng
@File    : upper_indexes.py
@Description: 中间及顶层指数计算，存储
"""
import sys
import json
import pandas as pd

from utils.db_base import TableName
from product.state_govern.basic_data.get_leaf_score import insert_tree_data, get_pct_rank_regionally
from product.state_govern.parameters import df_leaf_conf, df_index_conf, db_conf

# PWDataTestObj = DBObj(DBShortName.ProductPWDataTest).obj
# PWDataFormalObj = DBObj(DBShortName.ProductPWDataFormal).obj


def get_upper_indexes_into_tree_data(upper_index_code, df_sub_conf, db_obj, get_conn=False):
    """
    @功能：计算某个非叶子节点的指数，并写入tree_data表
    :param upper_index_code: 目标节点code
    :param df_sub_conf: 含其子节点的配置文件
    :param db_obj:对应操作的数据库
    :return:
    """

    sub_codes = df_sub_conf[df_sub_conf.upper_index == upper_index_code].index.values.tolist()

    sub_codes_ = ["\'" + code + "\'" for code in sub_codes]

    sqlstr_ = "SELECT gov_id, type_code AS index_code, data ->> 'score' AS score, version FROM {} WHERE product_name = 'SG' AND type_code IN ({})".format(TableName.PWTreeData, ", ".join(sub_codes_))

    # print(sqlstr_, flush=True)

    if get_conn:
        db_obj.get_conn()

    datas = db_obj.read_from_table(sqlstr_)

    if not len(datas):
        print("没有取到数据！upper_index={}  sub_codes={}".format(upper_index_code, sub_codes), flush=True)
        if get_conn:
            db_obj.disconnect()
        sys.exit()

    data_df = pd.DataFrame(datas)

    version_ = data_df["version"].values[0]

    # data_df = data_df.sort_values(by=["gov_id", "index_code"])

    data_df = data_df.set_index("index_code", drop=True)
    data_df["weight"] = df_sub_conf["index_weight"]
    data_df["score"] = data_df["score"].astype(float)
    data_df["index_val"] = data_df["score"]*data_df["weight"]

    new_data = data_df.groupby(by="gov_id").agg({"index_val": "sum"})  # , as_index=False

    new_data = new_data.rename(columns={"index_val": "value"})
    new_data["product_name"] = "SG"
    new_data["type_code"] = upper_index_code
    new_data["version"] = version_

    new_data = get_pct_rank_regionally(new_data, rank_ascending=True, with_hitec=False, with_pctr_desc=True)

    new_data["data"] = new_data.apply(lambda x: json.dumps({"score": round(x["value"],2), "value": round(x["value"],2), "rank": int(x["rank"]), "pctr": round(x["pct_rank"], 2), "pctr_desc": x["pctr_desc"]}, ensure_ascii=False), axis=1)

    # 放出gov_id
    new_data = new_data.reset_index()

    insert_tree_data(new_data, db_obj, False)

    print("tree_data中间节点数据插入完毕！index_code={}   version={}".format(upper_index_code, version_), flush=True)

    if get_conn:
        db_obj.disconnect()


def upper_indexes_main(test_mode=True, upper_codes=[]):
    """
    @功能：计算中间层级指数
    :param test_mode:
    :param upper_codes:
    :return:
    """

    db_obj = db_conf[test_mode]

    df_index_conf_ = df_index_conf.sort_values(by="index_level", ascending=False)

    columns = df_index_conf_.columns.tolist()
    df_whole_conf = pd.concat([df_index_conf_, df_leaf_conf[columns]], axis=0)

    db_obj.get_conn()

    df_index_conf_ = df_index_conf_[df_index_conf_.index.isin(upper_codes)] if upper_codes else df_index_conf_

    for index_code, row in df_index_conf_.iterrows():
        get_upper_indexes_into_tree_data(index_code, df_whole_conf, db_obj, get_conn=False)

    db_obj.disconnect()


if __name__ == "__main__":
    upper_indexes_main(test_mode=True, upper_codes=[])





