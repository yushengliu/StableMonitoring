#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/12/2 14:18
@Author  : Liu Yusheng
@File    : get_leaf_score.py
@Description: 计算叶子节点分数，写入数据库
"""
import json
import pandas as pd
from datetime import datetime
from copy import deepcopy

import utils.path_manager as pm
from utils.utilities import operate_es_knowledge
from utils.db_base import DBObj, TableName, DBShortName
from utils.get_2861_gaode_geo_gov_id_info import complement_prov_city_data_with_counties
import utils.get_2861_gaode_geo_gov_id_info as gov_utils
from utils.MyModule import TimeDispose
from product.state_govern.parameters import df_leaf_conf, sg_data_path, county_missing_leafs, county_n_city_missing_leafs

GovIndexDB = DBObj(DBShortName.GovIndexLocal).obj


def get_gov_idx_data(idx_code, version, get_conn=False):
    sqlstr_ = "SELECT idx_code AS code, idx_value AS value, idx_version AS version FROM {} WHERE idx_code = '{}' AND idx_version <= '{}' ORDER BY idx_version DESC LIMIT 1;".format(TableName.GovIndex, idx_code, version + ' 00:00:00')

    if get_conn:
        GovIndexDB.get_conn()

    datas = GovIndexDB.read_from_table(sqlstr_)

    if get_conn:
        GovIndexDB.disconnect()

    return datas


def get_knowledge_base_data(type_code, version):
    query_dict = {"type_code": type_code, "version": version}
    result = operate_es_knowledge(query_dict, es_type="base", operation_type="read")

    return result["content"]


def insert_tree_data(data_df, db_obj, get_conn=False):

    sqlstr_head = "INSERT INTO %s (gov_id, product_name, type_code, data, version, update_time) VALUES ({gov_id}, '{product_name}', '{type_code}', '{data}', '{version}', '%s') ON CONFLICT (gov_id, product_name, type_code) DO UPDATE SET data = '{data}', version = '{version}', update_time = '%s';"%(TableName.PWTreeData, datetime.now(), datetime.now())
    sqlstr_list = [sqlstr_head.format(**row) for index, row in data_df.iterrows()]

    # db_obj = DBObj(db_short).obj

    if get_conn:
        db_obj.get_conn()

    row_num = len(sqlstr_list)

    for i in range(0, row_num, 10000):
        db_obj.execute_any_sql("".join(sqlstr_list[i: i+10000]))
        print("&&&&&&&&&&&&&&&&&&已插入{}：{}条&&&&&&&&&&&&&&&&&".format(TableName.PWTreeData, len(sqlstr_list[i: i+10000])), flush=True)

    if get_conn:
        db_obj.disconnect()


def get_pct_rank_regionally(data_df, rank_ascending=True, with_hitec=False, with_pctr_desc=False):
    df_county = gov_utils.get_all_county_gov_id_info(with_hitec=with_hitec)
    df_city = gov_utils.get_all_city_gov_id_info()
    df_prov = gov_utils.get_all_province_gov_id_info()

    data_df.loc[df_county.index, "pct_rank"] = data_df.loc[df_county.index, "value"].rank(method="max", pct=True, ascending=rank_ascending)*100
    data_df.loc[df_county.index, "rank"] = data_df.loc[df_county.index, "pct_rank"].rank(method="min", ascending=False)

    data_df.loc[df_city.index, "pct_rank"] = data_df.loc[df_city.index, "value"].rank(method="max", pct=True, ascending=rank_ascending)*100
    data_df.loc[df_city.index, "rank"] = data_df.loc[df_city.index, "pct_rank"].rank(method="min", ascending=False)

    data_df.loc[df_prov.index, "pct_rank"] = data_df.loc[df_prov.index, "value"].rank(method="max", pct=True, ascending=rank_ascending)*100
    data_df.loc[df_prov.index, "rank"] = data_df.loc[df_prov.index, "pct_rank"].rank(method="min", ascending=False)

    if with_pctr_desc:  # 增加领先的描述
        data_df.loc[df_county.index, "pctr_desc"] = data_df.loc[df_county.index, "pct_rank"].apply(lambda x: "{}%的区县".format(round(x,2)))
        data_df.loc[df_city.index, "pctr_desc"] = data_df.loc[df_city.index, "pct_rank"].apply(lambda x: "{}%的市".format(round(x, 2)))
        data_df.loc[df_prov.index, "pctr_desc"] = data_df.loc[df_prov.index, "pct_rank"].apply(lambda x: "{}%的省".format(round(x, 2)))

    return data_df


def get_leaf_score(version, test_mode=True, index_codes=[]):
    # file_path = pm.LOCAL_STABLE_PROJECT_STORAGE + "state_govern/" + "files/"
    # leaf_conf_csv = "SG_leaf_conf.csv"
    # df_leaf_conf = pd.read_csv(file_path+leaf_conf_csv, encoding="GBK", index_col="index_code")

    df_leaf_conf_ = df_leaf_conf[df_leaf_conf.index.isin(index_codes)] if index_codes else df_leaf_conf

    if test_mode:
        tree_data_obj = DBObj(DBShortName.ProductPWDataTest).obj
    else:
        tree_data_obj = DBObj(DBShortName.ProductPWDataFormal).obj

    tree_data_obj.get_conn()

    for index_code, row in df_leaf_conf_.iterrows():
        if row["idx_rank"] == 1:
            rank_ascending = True
        else:
            rank_ascending = False

        # 政府指标
        if row["source"] == "gov":
            result = get_gov_idx_data(index_code, version, get_conn=True)
            data = result[0]
            idx_version = data["version"]

            value_dict = data["value"]
            value_series = pd.Series(value_dict)
            value_df = pd.DataFrame(value_series, columns=["value"])
            value_df.index.name = "gov_id"
            value_df.index = value_df.index.astype(int)
        # 知识库
        else:
            result = get_knowledge_base_data(index_code, version)
            idx_version = result["version"][index_code]

            value_df = pd.DataFrame.from_dict(result["datas"]).T
            value_df = value_df.rename(columns={index_code: "value"})
            value_df.index.name = "gov_id"
            value_df.index = value_df.index.astype(int)

        # 特殊指标，特殊补齐
        # 区县较全的指标 - 底层区县补中位值，上层（市/省）都用底层区县指数补全
        if index_code not in county_missing_leafs:
            value_df_ = complement_prov_city_data_with_counties(value_df, columns=["value"], by=row["complement_by_subgovs"], verify_non_counties_with_median=True, with_hitec=False)
        # 区县缺失十分严重的指标 - 底层区县补中位值，市/省再具体判断
        else:
            if index_code not in county_n_city_missing_leafs:   # 市级数据较全的情况， 用市数据填充
                value_df_ = complement_prov_city_data_with_counties(value_df, columns=["value"], by=row["complement_by_subgovs"], verify_non_counties_with_median=True, city_by_median=True, prov_by_sub_citys=True)
            else:       # 区县+市缺失都很严重的情况，各级自己补充自己的。
                value_df_ = complement_prov_city_data_with_counties(value_df, columns=["value"], by=row["complement_by_subgovs"], verify_non_counties_with_median=True, city_by_median=True, prov_by_median=True)

        value_df_["type_code"] = index_code
        value_df_["product_name"] = "SG"
        value_df_["version"] = version

        df_county = gov_utils.get_all_county_gov_id_info(with_hitec=False)
        df_city = gov_utils.get_all_city_gov_id_info()
        df_prov = gov_utils.get_all_province_gov_id_info()

        value_df_.loc[df_county.index, "data"] = value_df_.loc[df_county.index, "value"].rank(method="max", pct=True, ascending=rank_ascending)
        value_df_.loc[df_county.index, "pctr_desc"] = value_df_.loc[df_county.index, "data"].apply(lambda x: "{}%的区县".format(round(x*100, 2)))
        value_df_.loc[df_county.index, "rank"] = value_df_.loc[df_county.index, "data"].rank(method="min", ascending=False)

        value_df_.loc[df_city.index, "data"] = value_df_.loc[df_city.index, "value"].rank(method="max", pct=True, ascending=rank_ascending)
        value_df_.loc[df_city.index, "pctr_desc"] = value_df_.loc[df_city.index, "data"].apply(lambda x: "{}%的市".format(round(x * 100, 2)))
        value_df_.loc[df_city.index, "rank"] = value_df_.loc[df_city.index, "data"].rank(method="min", ascending=False)

        value_df_.loc[df_prov.index, "data"] = value_df_.loc[df_prov.index, "value"].rank(method="max", pct=True, ascending=rank_ascending)
        value_df_.loc[df_prov.index, "pctr_desc"] = value_df_.loc[df_prov.index, "data"].apply(lambda x: "{}%的省".format(round(x * 100, 2)))
        value_df_.loc[df_prov.index, "rank"] = value_df_.loc[df_prov.index, "data"].rank(method="min", ascending=False)

        value_df_["data"] = value_df_.apply(lambda x: json.dumps({"score": round(x["data"]*100, 2), "value": round(x["value"],2), "rank": int(x["rank"]), "pctr": round(x["data"]*100, 2), "pctr_desc": x["pctr_desc"], "idx_version": str(idx_version)}, ensure_ascii=False), axis=1)

        # 放出gov_id
        value_df_ = value_df_.reset_index()

        insert_tree_data(value_df_, tree_data_obj)

        print("tree_data叶子节点数据插入完毕！index_code={}   idx_version={}   version={}".format(index_code, idx_version, version), flush=True)

    tree_data_obj.disconnect()


def get_leaf_index_condition(version, index_codes=[]):

    df_leaf_conf_ = df_leaf_conf[df_leaf_conf.index.isin(index_codes)] if index_codes else df_leaf_conf

    df_leaf_condition = deepcopy(df_leaf_conf_[["index_name", "source"]])

    for index_code, row in df_leaf_conf_.iterrows():
        # 政府指标
        if row["source"] == "gov":
            result = get_gov_idx_data(index_code, version, get_conn=True)
            data = result[0]
            idx_version = data["version"]

            value_dict = data["value"]
            value_series = pd.Series(value_dict)
            value_df = pd.DataFrame(value_series, columns=["value"])
            value_df.index.name = "gov_id"
            value_df.index = value_df.index.astype(int)
        # 知识库
        else:
            result = get_knowledge_base_data(index_code, version)
            idx_version = result["version"][index_code]

            value_df = pd.DataFrame.from_dict(result["datas"]).T
            value_df = value_df.rename(columns={index_code: "value"})
            value_df.index.name = "gov_id"
            value_df.index = value_df.index.astype(int)

        df_county = deepcopy(gov_utils.get_all_county_gov_id_info(with_hitec=False))
        df_county["value"] = value_df["value"]
        county_valid_r = round((df_county.shape[0] - df_county["value"].isna().sum())*100 / df_county.shape[0], 2)

        df_city = deepcopy(gov_utils.get_all_city_gov_id_info())
        df_city["value"] = value_df["value"]
        city_valid_r = round((df_city.shape[0] - df_city["value"].isna().sum()) * 100 / df_city.shape[0], 2)

        # if index_code == "8520100204":   # 多出来的市是：['广东省|东莞市', '广东省|中山市', '海南省|儋州市', '甘肃省|嘉峪关市']
        #     valid_city = df_city[df_city["value"] == df_city["value"]]
        #     print("index_code={}  index_name={}\nvalid_city_ids={}\nvalid_city_names={}".format(index_code, row["index_name"], valid_city.index.values.tolist(), valid_city["full_name"].tolist()))
        #     return False

        df_prov = deepcopy(gov_utils.get_all_province_gov_id_info())
        df_prov["value"] = value_df["value"]
        prov_valid_r = round((df_prov.shape[0] - df_prov["value"].isna().sum()) * 100 / df_prov.shape[0], 2)

        df_leaf_condition.loc[index_code, "county_ratio"] = county_valid_r
        df_leaf_condition.loc[index_code, "city_ratio"] = city_valid_r
        df_leaf_condition.loc[index_code, "prov_ratio"] = prov_valid_r

    df_leaf_condition.to_csv(sg_data_path + "leaf_index_condition.csv", encoding="GBK")


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

    df_county = gov_utils.get_all_county_gov_id_info(with_hitec=False)
    df_city = gov_utils.get_all_city_gov_id_info()  # 20191224 - 省级直辖县当做市处理
    df_prov = gov_utils.get_all_province_gov_id_info()

    county_valid_ids = list(value_df.index.intersection(df_county.index))
    city_valid_ids = list(value_df.index.intersection(df_city.index))
    prov_valid_ids = list(value_df.index.intersection(df_prov.index))

    valid_ids_dict = {"区县": county_valid_ids, "市": city_valid_ids, "省": prov_valid_ids}

    value_df_ = value_df.reindex(gov_utils.df_2861_gaode_geo_all.index)  # 补全gov_id

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

    value_df_["data"] = value_df_.apply(lambda x: {key: x[key] for key in data_columns}, axis=1)

    # 放出gov_id
    value_df_ = value_df_.reset_index()
    result_list = value_df_[["gov_id", "data", "actual_lack"]].to_dict(orient="records")

    return result_list


if __name__ == "__main__":
    version_ = TimeDispose.get_last_version_date(pm.STABLE_SCORE_STORAGE, "version.txt")

    # get_leaf_score(version_, test_mode=True, index_codes=[])

    # get_leaf_index_condition(version_, index_codes=[])

    value_ = get_gov_idx_data("X0063", "2019-12-01", get_conn=True)
    value_dict = value_[0]["value"]
    results = value2index(value_dict, True)
    print(results)
















