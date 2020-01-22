#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/12/2 10:02
@Author  : Liu Yusheng
@File    : get_traffic_avg_speed.py
@Description: 交通月均早/晚高峰得分
"""
import sys
import pandas as pd
from copy import deepcopy

import utils.path_manager as pm
from utils.utilities import operate_es_knowledge
from utils.MyModule import TimeDispose
from utils.get_2861_gaode_geo_gov_id_info import complement_prov_city_data_with_counties


def calculate_traffic_monthly_avg_speed(version, morning_type=True):
    """
    @功能：计算交通的月度早/晚高峰平均速度，并写入知识库
    :param version: 当月最后一天 // 稳定相关一直都是取的最后一天
    :return:
    """

    if morning_type:
        org_code = '8510010205'
        aim_code = '8520100204'
    else:
        org_code = '8510010235'
        aim_code = '8520100205'

    version = str(version).split(' ')[0]
    start_date, end_date = TimeDispose(version).date_to_period(period_type="month")

    start_version = str(start_date).split(' ')[0]
    end_version = TimeDispose(end_date).get_ndays_ago(1)
    end_version = str(end_version).split(' ')[0]
    # date_to_period 中，返回的end_date为时段截止日期的后一天凌晨；此处知识库read_serial的end_version是被包括的，所以需要往前推一天。

    query_dict = {"type_code":org_code, "start_version": start_version, "end_version": end_version}
    query_result = operate_es_knowledge(query_dict, es_type="base", operation_type="read_serial")

    # query_result["content"] = {}   # 报错的情况
    if not query_result["content"]:
        print("取数据出错：\nquery_dict={};\nreturn_code={};\nreturn_msg={}".format(query_dict, query_result["return_code"], query_result["return_msg"] if "return_msg" in query_result.keys() else " "), flush=True)
        sys.exit()

    # 没有数据的情况
    if not query_result["content"]["datas"]:
        print("取出数据为空：query_dict={}".format(query_dict), flush=True)
        return

    org_datas = query_result["content"]["datas"]
    avg_dict = pd.DataFrame.from_dict(org_datas).mean().to_dict()

    # print(avg_dict, flush=True)

    insert_query = {"type_code": aim_code, "datas": avg_dict, "submitter": "yusheng.liu", "version": version}
    insert_result = operate_es_knowledge(insert_query, es_type="base", operation_type="write")

    print(insert_result, flush=True)


def update_more_traffic_meta():
    file_path = pm.LOCAL_STABLE_PROJECT_STORAGE + "state_govern/" + "files/"
    file_name = "traffic_knowledge_meta.csv"

    df_meta = pd.read_csv(file_path+file_name, encoding="utf8")

    df_meta_more = deepcopy(df_meta[(df_meta.submitter != 'leng')&(df_meta.type_code == df_meta.type_code)])

    df_meta_more["type_code"] = df_meta_more["type_code"].apply(lambda x: int(x))

    for index, row in df_meta_more.iterrows():
        update_dict = row.to_dict()
        update_result = operate_es_knowledge(update_dict, es_type="meta", operation_type="write")

        print("meta更新完毕！type_code = {};  type_name={}.\nupdate_result={}".format(row["type_code"], row["type_name"], update_result), flush=True)


def traffic_monthly_speed_main(version_):
    calculate_traffic_monthly_avg_speed(version_, morning_type=True)
    print("早高峰月度平均速度写入知识库！vesion={}".format(version_), flush=True)

    calculate_traffic_monthly_avg_speed(version_, morning_type=False)
    print("晚高峰月度平均速度写入知识库！vesion={}".format(version_), flush=True)


if __name__ == "__main__":
    version = '2019-01-01'

    if 0:
        versions = TimeDispose.get_all_version_dates(pm.STABLE_SCORE_STORAGE, "version.txt")
        for version_ in versions:
            calculate_traffic_monthly_avg_speed(version_, morning_type=True)
            calculate_traffic_monthly_avg_speed(version_, morning_type=False)









