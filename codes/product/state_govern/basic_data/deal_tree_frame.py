#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/11/28 11:59
@Author  : Liu Yusheng
@File    : deal_tree_frame.py
@Description: 生成 sg_tree_frame.csv
"""
import json
import pandas as pd

import utils.path_manager as pm

sg_file_path = pm.LOCAL_STABLE_PROJECT_STORAGE + "state_govern/" + "files/"


def add_leaf_weight():
    leaf_file = "SG_leaf_conf.csv"
    df_leaf = pd.read_csv(sg_file_path + leaf_file, encoding="GBK", index_col="index_code")

    upper_index_list = df_leaf["upper_index"].tolist()
    df_leaf["index_weight"] = df_leaf["upper_index"].apply(lambda x: round(1/upper_index_list.count(x), 2))

    df_leaf.to_csv(sg_file_path+leaf_file, encoding="GBK")


def transform_conf_to_tree_frame_csv():
    leaf_conf = "SG_leaf_conf.csv"
    index_conf = "SG_index_conf.csv"

    df_leaf = pd.read_csv(sg_file_path+leaf_conf, encoding="GBK", index_col="index_code")
    df_index = pd.read_csv(sg_file_path+index_conf, encoding="GBK", index_col="index_code")

    need_columns = df_index.columns.tolist()
    df_whole = pd.concat([df_index, df_leaf[need_columns]])

    print(df_whole)

    df_frame = pd.DataFrame(index=df_whole.index, columns=["id", "fid_id", "frame", "type_code", "action", "rank", "tree_name", "weight"])

    df_frame["frame"] = df_whole["index_name"]
    df_frame["frame"] = df_frame["frame"].apply(lambda x: json.dumps({"name": "region"+x if x == "国家治理" else x, "text": "", "title": "", "tree_title": ""}, ensure_ascii=False))

    df_frame["weight"] = df_whole["index_weight"]
    df_frame["type_code"] = df_frame.index
    df_frame["tree_name"] = "sg_tree"

    df_whole.loc["sg", "rank"] = "88"
    for index, row in df_whole.iterrows():
        if index == "sg":
            continue

        upper_index = df_whole.loc[index, "upper_index"]
        level = df_whole.loc[index, "index_level"]

        df_same_upper = df_whole[(df_whole["index_level"]==level)&(df_whole["upper_index"]==upper_index)&(df_whole["rank"] == df_whole["rank"])]

        if df_same_upper.shape[0]:
            max_rank = df_same_upper["rank"].max()
            max_rank = int(max_rank)
            last_digit = int(str(max_rank)[-1])
            df_whole.loc[index, "rank"] = "{}{}".format(str(max_rank)[:-1], last_digit+1) if last_digit != 9 else "{}{}{}".format(str(max_rank)[:-2], int(str(max_rank)[-2])+1, 0)

        else:
            df_whole.loc[index, "rank"] = "{}01".format(df_whole.loc[upper_index,"rank"])

    df_frame["rank"] = df_whole["rank"]
    df_frame.to_csv(sg_file_path + "sg_tree_frame.csv", encoding="GBK", index=False)


if __name__ == "__main__":
    transform_conf_to_tree_frame_csv()