#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/9/25 15:19
@Author  : Liu Yusheng
@File    : env_stable_utils.py
@Description: 公用函数
"""
import time
import json
import random
import pandas as pd
from datetime import datetime
from urllib import request, parse

from utils.db_base import DBObj, DBShortName

CbdDataTestObj = DBObj(DBShortName.CbdDataTest).obj
CbdDataFormalObj = DBObj(DBShortName.CbdDataFormal).obj

db_conf = {True: CbdDataTestObj, False: CbdDataFormalObj}

POINT_TABLE = "point_info_new"
SCORE_TABLE = "cbd_gov_score"

# 知识库
DATA_URL = "http://192.168.0.47:9400/knowledgebase/?"


def get_html_result(url, result_json=True):
    count = 10
    while count > 0:
        try:
            html = request.Request(url)
            if html:
                data = request.urlopen(html).read()
                if result_json:
                    data = json.loads(data)
                return data
            else:
                continue
        except Exception as e:
            print("getHtml fail, info: %s" % e, flush=True)
            print("retry times %s" % count, flush=True)
            count -= 1
            time.sleep(random.uniform(0.5, 2))
            continue


def read_from_es_knowledge(data_dict, url=DATA_URL):
    class_url = url + "operation_type=read&"
    url_values = parse.urlencode(data_dict)
    url = class_url + url_values
    result = get_html_result(url)
    return result


def get_knowledge_base_data(type_codes, version):
    """
    @功能：获取知识库里的数据
    :param type_codes: 多个type_code的list
    :param version: 版本日期
    :return:
    """

    data_dict = dict()

    data_dict["type_code"] = ",".join(type_codes)
    data_dict["version"] = version

    base_datas = read_from_es_knowledge(data_dict)

    content_data = base_datas["content"]["datas"]

    df_base = pd.DataFrame.from_dict(data=content_data, orient="index")

    # 当前版本没有取到数据的情况，直接不给版本信息，要求返回当前已有的最新版的数据
    if (df_base.shape[0] == 0) or (len(df_base.columns) != len(type_codes)):

        data_dict = {"type_code": ",".join(type_codes)}
        base_datas = read_from_es_knowledge(data_dict)
        content_data = base_datas["content"]["datas"]

        df_base = pd.DataFrame.from_dict(data=content_data, orient="index")

    return df_base


def get_ss_info_of_each_gov_from_db(test_mode):

    DB_OBJ = db_conf[test_mode]

    DB_OBJ.get_conn()

    sqlstr1 = "SELECT MAX(pversion) AS version FROM {};".format(POINT_TABLE)

    max_version = DB_OBJ.read_from_table(sqlstr1)[0]["version"]

    sqlstr2 = "SELECT MIN(ss_id) AS ss_id, gov_id, MAX(city_id) AS city_id, MAX(pversion) AS version, MAX(ptype) AS ptype FROM {} WHERE pversion='{}' GROUP BY gov_id;".format(POINT_TABLE, max_version)

    ss_datas = DB_OBJ.read_from_table(sqlstr2)

    df_ss_info = pd.DataFrame.from_dict(ss_datas)

    print(df_ss_info)

    DB_OBJ.disconnect()

    return df_ss_info


def get_score_into_db(df_data, test_mode):

    DB_OBJ = db_conf[test_mode]

    DB_OBJ.get_conn()

    data_list = df_data.to_dict(orient="records")

    if SCORE_TABLE == "cbd_point_score":
        sqlstr_head = "INSERT INTO %s (ss_id, pversion, ptype, gov_id, city_id, env_score, env_affect, stable_score, stable_affect, stable_event_num) VALUES ({ss_id}, '{version}', ARRAY{ptype}, {gov_id}, {city_id}, {env_score}, {env_affect}, {stable_score}, {stable_affect}, {stable_event_num}) ON CONFLICT(ss_id, pversion) DO UPDATE SET env_score={env_score}, env_affect={env_affect}, stable_score={stable_score}, stable_affect={stable_affect}, stable_event_num={stable_event_num}, ctime='%s';"%(SCORE_TABLE, datetime.now())

    elif SCORE_TABLE == "cbd_gov_score":
        sqlstr_head = "INSERT INTO %s (gov_id, pversion, env_score, env_affect, stable_score, stable_affect, stable_event_num) VALUES ({gov_id}, '{version}', {env_score}, {env_affect}, {stable_score}, {stable_affect}, {stable_event_num}) ON CONFLICT(gov_id, pversion) DO UPDATE SET env_score={env_score}, env_affect={env_affect}, stable_score={stable_score}, stable_affect={stable_affect}, stable_event_num={stable_event_num}, ctime='%s';" % (
        SCORE_TABLE, datetime.now())

    else:
        return None

    sqlstr_list = [sqlstr_head.format(**data_row) for data_row in data_list]

    row_num = len(sqlstr_list)

    for i in range(0, row_num, 1000):
        DB_OBJ.execute_any_sql("".join(sqlstr_list[i:i+1000]))
        print("&&&&&&已插入{}条&&&&&&".format(len(sqlstr_list[i:i+1000])), flush=True)

    DB_OBJ.disconnect()


def main(version_date, test_mode):
    type_codes = ["4030110001", "4020010000", "4530200101", "4530100112", "4530100111"]
    type_codes_dict = {"4030110001": "env_score", "4020010000": "env_affect", "4530200101": "stable_score",
                       "4530100112": "stable_affect", "4530100111": "stable_event_num"}

    # version = df_ss_info["version"].tolist()[0]

    version = version_date
    df_base = get_knowledge_base_data(type_codes, version)

    df_base.index = df_base.index.astype(int)

    from utils.get_2861_gaode_geo_gov_id_info import get_all_gov_id_info

    df_2861_gaode_geo = get_all_gov_id_info(with_hitec=False)

    df_base = df_base.join(df_2861_gaode_geo["full_name"], how="outer")
    df_base.pop("full_name")
    df_base = df_base.reset_index()
    df_base = df_base.rename(columns={"index": "gov_id"})
    df_base = df_base.rename(columns=type_codes_dict)
    df_base = df_base.fillna(df_base.median())
    df_base["version"] = version
    df_base[["env_score", "stable_score"]] = df_base[["env_score", "stable_score"]].apply(
        lambda x: round(x, 2))
    df_base[["env_affect", "stable_affect"]] = df_base[["env_affect", "stable_affect"]].apply(
        lambda x: round(x / 10000, 4))

    get_score_into_db(df_base, test_mode)


if __name__ == "__main__":
    # versions = ["2019-08-10", "2019-09-11"]

    versions = ["2019-12-18"]

    for test_mode in [True, False]:
        for version_ in versions:
            main(version_, test_mode)



