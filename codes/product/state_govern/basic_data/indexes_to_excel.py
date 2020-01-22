#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/12/5 10:17
@Author  : Liu Yusheng
@File    : indexes_to_excel.py
@Description: 中间指标转为excel，含指数，排名，百分比
"""
import os
import pandas as pd
from copy import deepcopy

import utils.path_manager as pm
from product.state_govern.parameters import db_conf, TableName, df_leaf_conf, df_index_conf
import utils.get_2861_gaode_geo_gov_id_info as gov_utils

file_path = pm.LOCAL_STABLE_PROJECT_STORAGE + "state_govern/" + "data/"


def get_score(version, test_mode, store=True):
    db_obj = db_conf[test_mode]
    db_obj.get_conn()

    sqlstr_ = "SELECT gov_id, type_code AS index_code, (data ->> 'score'):: FLOAT AS pct_rank, (data ->> 'value'):: FLOAT AS value, version FROM {} WHERE product_name = 'SG' ORDER BY (gov_id, type_code);".format(TableName.PWTreeData)

    datas = db_obj.read_from_table(sqlstr_)

    db_obj.disconnect()

    if store:
        df_score = pd.DataFrame(datas)
        # version_ = df_score["version"].values[0]

        # df_score.pop("version")
        df_score.to_csv(file_path+"basic_score_{}.csv".format(version), encoding="utf8")   # .format(version_)

    return pd.DataFrame(datas)


def store_df_into_multi_sheets_xlsx(data_df_dict, file_path):
    excel_writer = pd.ExcelWriter(file_path)

    for key, data_df in data_df_dict.items():

        data_df.to_excel(excel_writer, sheet_name=key)

        print("%s 已存入sheet表，文件位置：%s"%(key, file_path), flush=True)

    excel_writer.save()


def get_type_data(score_data, region_type, index_code):
    """
    @功能：从原表score_data中，返回需要的一张表，一个gov一行，columns含：[gov_name, rank, value]， 以gov_id为索引
    :param score_data:
    :param region_type:
    :param index_code:
    :return:
    """

    if region_type == "prov":
        aim_gov_ids = gov_utils.get_all_province_gov_id_info().index.values.tolist()
    elif region_type == "city":
        aim_gov_ids = gov_utils.get_all_city_gov_id_info().index.values.tolist()
    else:
        aim_gov_ids = gov_utils.get_all_county_gov_id_info().index.values.tolist()

    aim_data_df = deepcopy(score_data[(score_data["gov_id"].isin(aim_gov_ids))&(score_data.index_code == index_code)])

    aim_data_df["rank"] = aim_data_df["value"].rank(ascending=False, method="max")

    # 设gov_id为index
    aim_data_df = aim_data_df.set_index("gov_id", drop=True)
    aim_data_df["gov_name"] = gov_utils.df_2861_gaode_geo_all["full_name"]

    aim_data_df = aim_data_df.sort_values(by="rank", ascending=True)

    return aim_data_df[["gov_name", "rank", "value"]]


def get_indexes_excel(score_data, region_type, version):
    """
    @功能：只取前两级指标的指数+排名展示
    :param score_data:
    :param region_type:
    :param version:
    :return:
    """

    conf_cols = df_index_conf.columns.tolist()
    indexes_conf = pd.concat([df_index_conf, df_leaf_conf[df_leaf_conf.index_level.isin([2])][conf_cols]])

    indexes_conf = indexes_conf.sort_values(by="index_level", ascending=True)

    sheets_dict = dict()

    for index_code, row in indexes_conf.iterrows():
        index_level = row["index_level"]
        if index_level >= 2:
            break

        sheet_name = row["index_name"]
        sheet_data = get_type_data(score_data, region_type, index_code)
        sheet_data = sheet_data.rename(columns={"gov_name": "地区", "rank": "排名", "value": "指数"})

        sub_codes_conf = indexes_conf[indexes_conf.upper_index == index_code]
        for sub_code, sub_row in sub_codes_conf.iterrows():
            sub_name = sub_row["index_name"]
            sub_data = get_type_data(score_data, region_type, sub_code)
            sheet_data[sub_name] = sub_data["value"]

        sheets_dict[sheet_name] = sheet_data

    excel_name = "{}_indexes_{}.xlsx".format(region_type, version)
    store_df_into_multi_sheets_xlsx(sheets_dict, file_path+excel_name)


def get_leafs_excel(score_data, region_type, version):

    main_indexes = ["eco", "soc", "env", "cul", "pol", "pub"]

    sheets_dict = dict()

    for index_code in main_indexes:
        sheet_name = df_index_conf.loc[index_code, "index_name"]
        sheet_data = get_type_data(score_data, region_type, index_code)
        sheet_data = sheet_data.rename(columns={"gov_name": "地区", "rank": "排名", "value": "指数"})

        leaf_codes_conf = df_leaf_conf[df_leaf_conf["upper_index"].str.startswith(index_code)]
        for leaf_code, leaf_row in leaf_codes_conf.iterrows():
            leaf_name = leaf_row["index_name"]
            leaf_data = get_type_data(score_data, region_type, leaf_code)
            sheet_data[leaf_name] = leaf_data["value"]

        sheets_dict[sheet_name] = sheet_data

    excel_name = "{}_leafs_{}.xlsx".format(region_type, version)
    store_df_into_multi_sheets_xlsx(sheets_dict, file_path+excel_name)


def excel_main(version, test_mode=True, store=True):
    if os.path.exists(file_path+"basic_score_{}.csv".format(version)):
        score_data_ = pd.read_csv(file_path+"basic_score_{}.csv".format(version), encoding="utf8")
    else:
        score_data_ = get_score(version, test_mode, store)

    print("获取数据完毕！", flush=True)
    version_ = version

    for region_type_ in ["prov", "city", "county"]:
        get_indexes_excel(score_data_, region_type_, version_)
        print("region_type={} 前两级指标EXCEL生成完毕！".format(region_type_), flush=True)
        get_leafs_excel(score_data_, region_type_, version_)
        print("region_type={} 叶子节点EXCEL生成完毕！".format(region_type_), flush=True)


if __name__ == "__main__":
    version = "2019-10-31"
    excel_main(version)



























