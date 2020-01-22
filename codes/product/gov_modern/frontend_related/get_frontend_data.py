#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/11/20 20:05
@Author  : Liu Yusheng
@File    : get_frontend_data.py
@Description:  前端数据支撑 // 树，地图
"""
import json
from copy import deepcopy

from utils.get_2861_gaode_geo_gov_id_info import df_2861_gaode_geo_all
from product.gov_modern.frontend_related.data_related_api import get_score_df_data
from product.gov_modern.frontend_related import scatter_areas_api as scatter_api, parameters as para

from utils.db_base import DBObj, DBShortName


def get_map_data(idx_type="mong", test_mode=False):
    """
    @功能：点击地图染色
    :param idx_type:
    :param test_mode:
    :return:
    """

    total_gov_ids = df_2861_gaode_geo_all.index.values.tolist()
    score_df = get_score_df_data(idx_type, total_gov_ids, test_mode)

    score_df["area_eng"] = score_df.apply(lambda x: scatter_api.judge_area(x["people_score"], x["official_score"], para.DataParas.X_CTHD, para.DataParas.Y_CTHD), axis=1)

    score_df["area"] = score_df["area_eng"].apply(lambda x: para.AREAS_NAMES_DICT[x])
    score_df["color"] = score_df["area_eng"].apply(lambda x: para.AREAS_COLOR_VALUES_DICT[x])
    score_df["people_desc"] = score_df["people_rltpct"].apply(lambda x: scatter_api.get_scatter_point_desc(x, "民众抱怨"))
    score_df["official_desc"] = score_df["official_rltpct"].apply(lambda x: scatter_api.get_scatter_point_desc(x, "官方宣传"))

    final_cols = ["gov_name", "color", "area", "people_desc", "official_desc"]
    score_df["value"] = score_df.apply(lambda x: {col: x[col] for col in final_cols}, axis=1)
    score_df["value"] = score_df["value"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    score_df["node_name"] = "MONG"

    # 清洗掉gov_id = 232 / 1450 的值
    score_df = score_df.reset_index()
    score_df = score_df[~score_df["gov_id"].isin([232, 233, 249, 250, 1450])]

    final_score_df = deepcopy(score_df[["gov_id", "node_name", "value", "version"]])
    data_list = final_score_df.to_dict(orient="records")

    if test_mode:
        db_obj = DBObj(DBShortName.ProductTest).obj
    else:
        db_obj = DBObj(DBShortName.ProductPWFormal).obj

    db_obj.get_conn()
    table_name = "pw_map_color"

    # , version - map_color没必要存多个version，每次更新就好
    sqlstr_head = "INSERT INTO %s (gov_id, node_name, value, version) VALUES ({gov_id}, '{node_name}', '{value}', '{version}') ON CONFLICT (gov_id, node_name) DO UPDATE SET value='{value}', version='{version}';"%(table_name)

    sqlstr_list = [sqlstr_head.format(**data_row) for data_row in data_list]

    row_num = len(sqlstr_list)

    for i in range(0, row_num, 1000):
        db_obj.execute_any_sql("".join(sqlstr_list[i: i+1000]))
        print("&&&&&&&&&&已插入{}条&&&&&&&&&&&".format(len(sqlstr_list[i:i + 1000])), flush=True)

    db_obj.disconnect()


def get_tree_data(idx_type, test_mode=False):
    total_gov_ids = df_2861_gaode_geo_all.index.values.tolist()
    score_df = get_score_df_data(idx_type, total_gov_ids, test_mode)

    score_df["area_eng"] = score_df.apply(
        lambda x: scatter_api.judge_area(x["people_score"], x["official_score"], para.DataParas.X_CTHD, para.DataParas.Y_CTHD), axis=1)

    score_df["area"] = score_df["area_eng"].apply(lambda x: para.AREAS_NAMES_DICT[x])
    score_df["color"] = score_df["area_eng"].apply(lambda x: para.AREAS_COLORS_DICT[x])

    score_df["data"] = score_df["color"].apply(lambda x: {"ratio": 100,  "node_bg_conf": {"node_bg_color": [x, x], "node_bg_time": 0}})
    score_df["data"] = score_df["data"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    score_df["type_code"] = idx_type
    score_df["product_name"] = "MONG"

    # 清洗掉gov_id = 232 / 1450 的值
    score_df = score_df.reset_index()
    score_df = score_df[~score_df["gov_id"].isin([232, 233, 249, 250, 1450])]

    final_score_df = deepcopy(score_df[["gov_id", "product_name", "type_code", "data", "version"]])
    data_list = final_score_df.to_dict(orient="records")

    if test_mode:
        db_obj = DBObj(DBShortName.ProductTest).obj
    else:
        db_obj = DBObj(DBShortName.ProductPWFormal).obj

    db_obj.get_conn()
    table_name = "pw_tree_data"

    # version,  -没必要多个version，每次更新覆盖就好
    sqlstr_head = "INSERT INTO %s (gov_id, product_name, type_code, data, version) VALUES ({gov_id}, '{product_name}', '{type_code}', '{data}', '{version}') ON CONFLICT (gov_id, type_code, product_name) DO UPDATE SET data='{data}', version='{version}', update_time=now();" % (table_name)

    sqlstr_list = [sqlstr_head.format(**data_row) for data_row in data_list]

    row_num = len(sqlstr_list)

    for i in range(0, row_num, 1000):
        db_obj.execute_any_sql("".join(sqlstr_list[i: i + 1000]))
        print("&&&&&&&&&&已插入{}条&&&&&&&&&&&".format(len(sqlstr_list[i:i + 1000])), flush=True)

    db_obj.disconnect()


if __name__ == "__main__":
    get_map_data(idx_type="mong", test_mode=False)

    for idx_type in para.UNIFORM_DISPOSAL_INDEXES:
        get_tree_data(idx_type, test_mode=False)




