#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/12/24 20:47
@Author  : Liu Yusheng
@File    : gov_leaf_func.py
@Description:
"""
import pandas as pd

"""
注：
① 20200102 - 省级直辖县(gov_type=5)当做市处理（仅限打榜平台，方便地图下钻，同级排名bar图）
"""

df_2861_gaode_geo_all = pd.read_csv("df_2861_gaode_geo_new_20191129.csv", index_col='gov_id',encoding="utf8")


def get_all_gov_id_info(with_hitec=False):
    """
    @功能：获取所有行政区划列表
    :param with_hitec:
    :return:
    """
    if with_hitec:
        df_geo = df_2861_gaode_geo_all
    else:
        df_geo = df_2861_gaode_geo_all[df_2861_gaode_geo_all["gov_type"].isin([0, 1, 2, 3, 4, 5, 31])]

    return df_geo


def get_all_county_gov_id_info(with_hitec=True):
    """
    @功能：获取所有行政区划-区县的信息表
    :return:
    """
    if with_hitec:
        df_county = df_2861_gaode_geo_all[~df_2861_gaode_geo_all["gov_type"].isin([0, 1, 2, 5, 31])]
    else:
        df_county = df_2861_gaode_geo_all[df_2861_gaode_geo_all["gov_type"].isin([3, 4])]

    return df_county


def get_all_city_gov_id_info():
    """
    @功能：获取所有行政区划-市的信息表   // 市+省辖地级市（市辖无区县）
    :return:
    """
    df_city = df_2861_gaode_geo_all[df_2861_gaode_geo_all["gov_type"].isin([1, 31, 5])]

    return df_city


def get_all_province_gov_id_info():
    """
    @功能：获取所有行政区划-省的信息表   // 省+直辖市
    :return:
    """
    df_province = df_2861_gaode_geo_all[df_2861_gaode_geo_all["gov_type"].isin([0, 2])]

    return df_province


def value2index(value_dict, rank_ascending):
    """
    @功能：将底层叶子节点的值，补齐、标记缺失、排名后返回
    // 根据2019-12-24与郑哥讨论，处理原则：所有缺失都不补齐；将已有的值，在同等级区域中归一后，用某一个值（50）替换所有缺失值
    :param value_dict:
    :param rank_ascending: True - 越大越好；False - 越小越好
    :return:
    """
    value_series = pd.Series(value_dict)
    value_df = pd.DataFrame(value_series, columns=["value"])
    value_df.index.name = "gov_id"
    value_df.index = value_df.index.astype(int)

    df_county = get_all_county_gov_id_info(with_hitec=False)
    df_city = get_all_city_gov_id_info()  # 20191224 - 省级直辖县当做市处理
    df_prov = get_all_province_gov_id_info()

    county_valid_ids = list(value_df.index.intersection(df_county.index))
    city_valid_ids = list(value_df.index.intersection(df_city.index))
    prov_valid_ids = list(value_df.index.intersection(df_prov.index))

    valid_ids_dict = {"区县": county_valid_ids, "市": city_valid_ids, "省": prov_valid_ids}

    df_all = get_all_gov_id_info(with_hitec=False)
    value_df_ = value_df.reindex(df_all.index)  # 补全gov_id

    for region, valid_ids in valid_ids_dict.items():
        if len(valid_ids):
            value_df_.loc[valid_ids, "pctr"] = value_df_.loc[valid_ids, "value"].rank(method="max", pct=True, ascending=rank_ascending)*100
            value_df_.loc[valid_ids, "pctr"] = value_df_.loc[valid_ids, "pctr"].apply(lambda x: round(x, 2))
            value_df_.loc[valid_ids, "pctr_desc"] = value_df_.loc[valid_ids, "pctr"].apply(lambda x: "{}%的{}".format(x, region))
            value_df_.loc[valid_ids, "rank"] = value_df_.loc[valid_ids, "pctr"].rank(method="min", ascending=False)
            value_df_.loc[valid_ids, "rank"] = value_df_.loc[valid_ids, "rank"].apply(lambda x: int(x))

            # 得分 - 将pctr归一到 60 - 100 （因为：之后缺失值补50）
            value_df_.loc[valid_ids, "score"] = value_df_.loc[valid_ids, "pctr"].apply(lambda x: round(60 + x*40/100, 2))
            value_df_.loc[valid_ids, "actual_lack"] = False

    # 将缺失的区域，score都补50
    value_df_["score"] = value_df_["score"].fillna(50)
    value_df_["actual_lack"] = value_df_["actual_lack"].fillna(True)

    data_columns = ["score", "value", "pctr", "rank", "pctr_desc"]
    # 其他的说明项，补 ‘-’
    for data_col in data_columns[1:]:
        value_df_[data_col] = value_df_[data_col].fillna('-')

    value_df_["data"] = value_df_.apply(lambda x: {key: int(x[key]) if (key == "rank" and x[key] != '-') else x[key] for key in data_columns}, axis=1)
    # 放出gov_id
    value_df_ = value_df_.reset_index()
    result_list = value_df_[["gov_id", "data", "actual_lack"]].to_dict(orient="records")

    return result_list