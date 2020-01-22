#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/11/29 16:21
@Author  : Liu Yusheng
@File    : test.py
@Description: 各种临时处理
"""
import pandas as pd

import utils.path_manager as pm


def readjust_gaode_geo():
    file_path = pm.RELATED_FILES
    old_file = "df_2861_gaode_geo_new.csv"
    new_file = "df_2861_gaode_geo_new_20191129.csv"

    df_gaode_old = pd.read_csv(file_path+old_file, encoding="GBK",index_col="gov_id")

    # df_gaode_old["gov_code"] = df_gaode_old["gov_code"].apply(lambda x: int(x))

    df_gaode_new = pd.read_csv(file_path+new_file, encoding="GBK", index_col="gov_id")

    df_gaode_new["gov_code"] = df_gaode_new["gov_code"].apply(lambda x: x*1000000)

    df_gaode_new["full_name"] = df_gaode_new.apply(lambda x: df_gaode_new.loc[x["in_city"], "gov_name"]+"|"+x["gov_name"] if x["in_city"] else x["gov_name"], axis=1)

    df_gaode_new["full_name"] = df_gaode_new.apply(lambda x: df_gaode_new.loc[x["in_province"], "gov_name"]+"|"+x["full_name"] if x["in_province"] else x["full_name"], axis=1)

    direct_index = df_gaode_new[df_gaode_new.gov_type.isin([4,7])].index
    df_gaode_new.loc[direct_index, "in_province"] = df_gaode_new.loc[direct_index, "in_city"]
    df_gaode_new.loc[direct_index, "in_city"] = 0

    df_gaode_new_concat_ = df_gaode_new.join(df_gaode_old, rsuffix="_past")

    for col_idx in ["gov_code", "gov_type", "full_name", "in_city", "in_province"]:
        df_gaode_new_concat_["%s_diff"%col_idx] = df_gaode_new_concat_.apply(lambda x: 1 if x[col_idx] == x[col_idx+"_past"] else 0, axis=1)

    df_gaode_old_concat_ = df_gaode_old.join(df_gaode_new, rsuffix="_new")

    for col_idx in ["gov_code", "gov_type", "full_name", "in_city", "in_province"]:
        df_gaode_old_concat_["%s_diff"%col_idx] = df_gaode_old_concat_.apply(lambda x: 1 if x[col_idx] == x[col_idx+"_new"] else 0, axis=1)

    df_gaode_new.to_csv(file_path + "df_2861_gaode_geo_new_20191129_.csv", encoding="GBK")
    df_gaode_new_concat_.to_csv(file_path + "gaode_geo_new_compare_20191129.csv", encoding="GBK")
    df_gaode_old_concat_.to_csv(file_path + "gaode_geo_old_compare_20191129.csv", encoding="GBK")


SQLSTR = """CREATE TABLE "public"."pw_tree_weight_usr" (
"usr_id" varchar(32) COLLATE "default" NOT NULL,
"product" varchar(32) COLLATE "default" NOT NULL,
"temp_name" varchar(32) COLLATE "default" NOT NULL,
"temp_value" json NOT NULL,
"update_time" timestamp(6) NOT NULL
)
WITH (OIDS=FALSE)
;



CREATE UNIQUE INDEX "pw_tree_weight_usr_idx" ON "public"."pw_tree_weight_usr" USING btree ("usr_id", "product", "temp_name");"""


def test_pandas_groupby():
    org_data = pd.DataFrame({"gov_id": [1, 1, 1, 2, 2, 2], "weight_id": ["a", "a", "a", "a", "a", "a"],
                             "type_code": ["s", "p", "e", "s", "p", "e"],
                             "data": [{"score": 50, "rank": 1}, {"score": 30, "rank": 2}, {"score": 20, "rank": 3},
                                      {"score": 10, "rank": 2}, {"score": 60, "rank": 1}, {"score": 120, "rank": 1}],
                             "version": ["2019-11-30", "2019-11-30", "2019-11-30", "2019-11-30", "2019-11-30",
                                         "2019-11-30"]})
    for data_col in ["score", "rank"]:
        org_data[data_col] = org_data["data"].apply(lambda x: x[data_col])
        org_data[data_col+"_"] = org_data.apply(lambda x: {x["type_code"]: x[data_col]}, axis=1)

    org_data_gb = org_data.groupby(by=["gov_id", "weight_id", "version"])

    print(org_data_gb)


def test_treedata_2_mapcolor():
    treedata = pd.DataFrame({"weight_id": ["a", "a", "a", "a"], "gov_id": [1, 2, 3, 4], "pctr": [{"sg": 40, "eco": 50, "cul": 60}, {"sg": 45, "eco": 55, "cul": 65}, {"sg": 50, "eco": 60, "cul": 70}, {"sg": 55, "eco": 65, "cul": 75}]})

    for index_code in ["sg", "eco"]:
        treedata[index_code] = treedata["pctr"].apply(lambda x: x[index_code])

    print(treedata)


if __name__ == "__main__":
    # readjust_gaode_geo()

    # DBOBJ = db_conf[False]
    # DBOBJ.get_conn()
    # DBOBJ.execute_any_sql(SQLSTR)
    # DBOBJ.disconnect()

    # test_pandas_groupby()
    test_treedata_2_mapcolor()