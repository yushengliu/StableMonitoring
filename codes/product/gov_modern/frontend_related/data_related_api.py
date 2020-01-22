#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/10/30 16:30
@Author  : Liu Yusheng
@File    : data_related_api.py
@Description: 前端数据获取、预处理等模块
"""
import pandas as pd

import utils.get_2861_gaode_geo_gov_id_info as gov_info
from utils.db_base import TableName, DBShortName, DBObj


def get_score_from_db(idx_type, gov_ids, test_mode=True):
    """
    @功能：从数据库取数据
    :param idx_type:
    :param gov_ids:
    :param test_mode:
    :return:
    """
    if test_mode:
        db_obj = DBObj(DBShortName.ProductPWDataTest).obj
    else:
        db_obj = DBObj(DBShortName.ProductPWDataFormal).obj

    table_name = "gov_modern_score"

    gov_ids_ = [str(int(i)) for i in gov_ids]
    gov_ids_str = ", ".join(gov_ids_)

    db_obj.get_conn()

    sqlstr_ = "SELECT {0}.gov_id, {0}.score_info, {0}.version FROM {0}, (SELECT MAX(version) AS version FROM {0} WHERE index_type = '{2}') aa WHERE {0}.index_type='{2}' AND {0}.version = aa.version AND {0}.gov_id in ({1});".format(table_name, gov_ids_str, idx_type)

    datas = db_obj.read_from_table(sqlstr_)

    db_obj.disconnect()

    # print(datas, flush=True)

    return datas


def preprocess_data(db_datas):
    """
    @功能：对数据库取回的数据预处理，添加gov_name，展开score_info字段
    :param db_datas:
    :return:
    """
    data_df = pd.DataFrame(db_datas)
    data_df = data_df.set_index(["gov_id"], drop=True)

    data_df["gov_name"] = gov_info.df_2861_gaode_geo_all["full_name"]

    score_cols = ["people_data", "official_data", "people_rltpct", "official_rltpct", "people_score", "official_score"]

    for score_col in score_cols:
        data_df[score_col] = data_df["score_info"].apply(lambda x: x[score_col])

    return data_df


def get_score_df_data(idx_type, gov_ids, test_mode):
    """
    @功能：获取前端处理需要的dataframe数据
    :param idx_type:
    :param gov_ids:
    :param test_mode:
    :return:
    """

    datas = get_score_from_db(idx_type, gov_ids, test_mode)
    data_df = preprocess_data(datas)

    return data_df


def update_map_data(test_mode=True):
    """
    @功能：写地图数据
    :param test_mode:
    :return:
    """
    if test_mode:
        db_obj = DBObj(DBShortName.ProductPWTest)


if __name__ == "__main__":
    pass
