#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/10/28 11:41
@Author  : Liu Yusheng
@File    : get_stats_n_score.py
@Description: 获取数据（法治化）
"""
import os
import time
import json
import pandas as pd
from datetime import datetime
from copy import deepcopy

import utils.path_manager as pm
from utils.MyModule import TimeDispose
from utils.utilities import get_valid_es_data_by_near, operate_es_knowledge
from utils.parameters import event_table, event_db_obj
import utils.get_2861_gaode_geo_gov_id_info as gov_info
from utils.db_base import DBShortName, DBObj


# 离群点检测
def check_outliers_by_tukeys_method(data_df, col="data", k=1.5, check_boundary="middle"):
    """
    @功能：找出异常点 - 箱线图原理 (Q1 - k*IQR, Q3 + k*IQR)，IQR=Q3-Q1; k=1.5，超出则温和异常；k=3，超出则极端异常。
    :param data_df:
    :param col:
    :param k:
    :param check_boundary: "upper": 只测满不满足上限Q3+k*IQR; "lower": 只测满不满足下限Q1-k*IQR; "middle"/其他：都默认测中间范围，正常情况
    :return:
    """

    Q1 = data_df[col].quantile(0.25)
    Q2 = data_df[col].quantile(0.5)
    Q3 = data_df[col].quantile(0.75)

    IQR = Q3 - Q1

    if check_boundary == "upper":   # 只测上限
        data_df["check"] = data_df[col].apply(lambda x: 1 if (x <= Q3 + k * IQR) else 0)
    elif check_boundary == "lower":    # 只测下限
        data_df["check"] = data_df[col].apply(lambda x: 1 if (x >= Q1 - k * IQR) else 0)
    else:   # 正常，测中间
        data_df["check"] = data_df[col].apply(lambda x: 1 if ((x >= Q1-k*IQR) and (x <= Q3+k*IQR)) else 0)

    return data_df


def get_law_data(interval=[], get_conn=False):
    """
    @功能：取打上verified_type标签的，【法治】相关的事件数据，
    :param interval: 为空则取全部，不空则按时段取
    :return:
    """
    if not interval:
        sqlstr_ = "SELECT gov_id, ROUND(CAST(actual_value/19 AS NUMERIC), 4) AS affect, more_info ->> 'verified_type' AS verified_type, more_info ->> 'sentiment' AS sentiment FROM {} WHERE (more_info ->> 'verified_type' IS NOT NULL) AND (more_info ->> 'sub_cates' LIKE '%法制%' OR more_info ->> 'sub_cates' LIKE '%刑法%') ORDER BY id ASC;".format(event_table)  # event_start_time,

    else:
        sqlstr_ = "SELECT gov_id, ROUND(CAST(actual_value/19 AS NUMERIC), 4) AS affect, more_info ->> 'verified_type' AS verified_type, more_info ->> 'sentiment' AS sentiment FROM {} WHERE (more_info ->> 'verified_type' IS NOT NULL AND event_start_time >= '{}' AND event_start_time < '{}') AND (more_info ->> 'sub_cates' LIKE '%法制%' OR more_info ->> 'sub_cates' LIKE '%刑法%') ORDER BY id ASC;".format(event_table, interval[0], interval[1])   # event_start_time,

    if get_conn:
        event_db_obj.get_conn()

    datas = event_db_obj.read_from_table(sqlstr_)

    if get_conn:
        event_db_obj.disconnect()

    return datas


def get_idx_data(idx_type, interval=[], get_conn=False):
    """
    @功能：取more_info里，mong字段中，含有idx_type标签的事件数据
    :param idx_type:
    :param interval: 为空则取全部，不空则按时段取
    :param get_conn:
    :return:
    """

    sqlstr_ = "SELECT gov_id, ROUND(CAST(actual_value/19 AS NUMERIC), 4) AS affect, more_info ->> 'verified_type' AS verified_type, more_info ->> 'sentiment' AS sentiment FROM %s WHERE more_info ->> 'verified_type' IS NOT NULL" % event_table

    if interval:
        sqlstr_ += " AND event_start_time >= '%s' AND event_start_time < '%s'"%(interval[0], interval[1])

    if idx_type != "mong":
        sqlstr_ += " AND more_info ->> 'mong' IS NOT NULL  AND ((more_info ->> 'mong')::jsonb ? '%s')"%idx_type

    sqlstr_ += " ORDER BY id ASC;"

    if get_conn:
        event_db_obj.get_conn()

    datas = event_db_obj.read_from_table(sqlstr_)

    if get_conn:
        event_db_obj.disconnect()

    return datas


def check_post_types(data_df):
    """
    @功能：把verified_type (0/1/-1/200……) ->> body_type (people/official/media/other)
    :param data_df:
    :return:
    """

    POST_CONF =pd.read_csv(pm.RELATED_FILES+"VERIFIED_TYPES.csv", encoding="utf8", index_col="verified_type")

    data_df["body_type"] = data_df["verified_type"].apply(lambda x: POST_CONF.loc[int(x), "body_type"] if int(x) in POST_CONF.index else "other")

    return data_df


def get_complement_prov_city_data_method(type_code):
    # 人均GDP - 求均值；其他（GDP / live_popu / web_popu等） - 求和
    if type_code == "9230200103":
        method_ = "average"
    else:
        method_ = "sum"
    return method_


def preprocess_data(data_df, version, official_typecodes={}, store=True, file_name=""):
    """
    @功能：从数据库拿回数据后，->> 各项指标
    :param data_df:
    :param version:
    :param official_typecodes:
    :param store:
    :return:
    """

    start_time_ = time.time()

    STATS_COLS = ['people_pos_affect', 'people_pos_num', 'people_neg_affect', 'people_neg_num', 'people_mid_affect', 'people_mid_num', 'people_total_affect', 'people_total_num', 'official_pos_affect', 'official_pos_num', 'official_neg_affect', 'official_neg_num', 'official_mid_affect', 'official_mid_num', 'official_total_affect', 'official_total_num', 'media_pos_affect', 'media_pos_num', 'media_neg_affect', 'media_neg_num', 'media_mid_affect', 'media_mid_num', 'media_total_affect', 'media_total_num', 'other_pos_affect', 'other_pos_num', 'other_neg_affect', 'other_neg_num', 'other_mid_affect', 'other_mid_num', 'other_total_affect', 'other_total_num', 'total_pos_affect', 'total_pos_num', 'total_neg_affect', 'total_neg_num', 'total_mid_affect', 'total_mid_num', 'total_total_affect', 'total_total_num']

    body_types = ["people", "official", "media", "other"]
    sentiments = ["pos", "neg", "mid"]
    data_types = ["num", "affect"]

    if data_df.shape[0]:
        # 按verified_type对应补上body_type（发博主体）
        data_df = check_post_types(data_df)

        # 将中文情绪类别 ->> 英文代称
        sentiments_dict = {"正": "pos", "负": "neg", "中": "mid"}
        data_df["sentiment"] = data_df["sentiment"].apply(lambda x: sentiments_dict[x])

        data_df["num"] = 1

        data_df_groupby = data_df.groupby(by=["gov_id", "body_type", "sentiment"]).agg({"affect": "sum", "num": "sum"})   # , as_index=False

        already_gov_ids = list(set(data_df_groupby.index.get_level_values('gov_id')))

        data_df_stats = pd.DataFrame(columns=["gov_id"]+STATS_COLS)
        data_df_stats["gov_id"] = already_gov_ids
        data_df_stats = data_df_stats.set_index(["gov_id"], drop=True)

        print("{} - 合并处理完毕，耗时：{}".format(datetime.now(), time.time() - start_time_), flush=True)

        data_pre_ = time.time()
        # 数据打平
        for gov_id in already_gov_ids:
            for body_type_ in body_types:
                for sentiment_ in sentiments:
                    for data_type_ in data_types:
                        if (gov_id, body_type_, sentiment_) in data_df_groupby.index:
                            data_df_stats.loc[gov_id, "{}_{}_{}".format(body_type_, sentiment_, data_type_)] = data_df_groupby.loc[(gov_id, body_type_, sentiment_), data_type_]
                        else:
                            data_df_stats.loc[gov_id, "{}_{}_{}".format(body_type_, sentiment_, data_type_)] = 0

        # total字段
        # people_total_num
        for body_type_ in body_types:
            for data_type_ in data_types:
                data_df_stats["{}_total_{}".format(body_type_, data_type_)] = sum([data_df_stats["{}_{}_{}".format(body_type_, x, data_type_)] for x in sentiments])

        # total_pos_num
        for sentiment_ in sentiments:
            for data_type_ in data_types:
                data_df_stats["total_{}_{}".format(sentiment_, data_type_)] = sum(
                    [data_df_stats["{}_{}_{}".format(x, sentiment_, data_type_)] for x in body_types])

        # total_total_num
        for data_type_ in data_types:
            data_df_stats["total_total_{}".format(data_type_)] = sum([data_df_stats["total_{}_{}".format(x, data_type_)] for x in sentiments])

        print("{} - 数据打平（每个gov_id一行）完毕，耗时：{}".format(datetime.now(), time.time() - data_pre_), flush=True)

    else:
        # 数据为空
        data_df_stats = pd.DataFrame(columns=STATS_COLS)
        print("警告！数据全为空，version={}，file_name={}".format(version, file_name), flush=True)

    completion_start_ = time.time()

    # 补齐各县、市、省、国
    data_df_stats = gov_info.complement_prov_city_data_with_counties(data_df_stats, STATS_COLS, "sum", with_country=True, verify_non_counties_with_zero=True, with_hitec=True)

    print("{} - 补齐县市省国数据完毕，耗时：{}".format(datetime.now(), time.time() - completion_start_), flush=True)

    official_start_ = time.time()

    # 添加官方数据
    if official_typecodes:
        versions = TimeDispose.get_all_version_dates(pm.STABLE_SCORE_STORAGE, "version.txt")

        for type_str, type_code in official_typecodes.items():

            if version != "score":
                content_data = get_valid_es_data_by_near(type_code, version, versions)
            else:
                content_data = get_valid_es_data_by_near(type_code, versions[-1], versions)

            df_es = pd.DataFrame.from_dict(data=content_data, orient="index")

            df_es.index = df_es.index.astype(int)

            method_ = get_complement_prov_city_data_method(type_code)

            df_es = gov_info.complement_prov_city_data_with_counties(df_es, [type_code], method_, with_country=True,
                                                                     verify_non_counties_with_zero=True,
                                                                     with_hitec=True)

            data_df_stats[type_str] = df_es[type_code]

    print("{} - 官方数据提取完毕，耗时：{}".format(datetime.now(), time.time() - official_start_), flush=True)

    if store:
        file_path = pm.STABLE_SCORE_STORAGE + version + "/" + "GovModern/"
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        data_df_stats.to_csv(file_path + file_name, encoding="utf8")

    else:
        return data_df_stats


def get_basic_stats(version, calculate_total=True, official_typecodes={"gdp": "9230200102", "per_gdp": "9230200103", "live_popu": "9230200183", "web_popu": "9230200197"}, idx_type="law"):
    """
    @功能：国家治理十几个指标的基础统计 —— 含version当月及截止数据
    :param version:
    :param calculate_total:
    :param official_typecodes:
    :param idx_type:
    :return:
    """
    start_time = time.time()

    # 累计
    if calculate_total:
        start_date = "2018-01-01"
        end_date = TimeDispose(version).get_next_month_first_day()
        interval = [start_date, end_date]
        file_name = "{}_ACCUMULATIVE_STATS.csv".format(idx_type.upper())
    # 当月
    else:
        interval = TimeDispose(version).date_to_period("month")
        file_name = "{}_MONTH_STATS.csv".format(idx_type.upper())

    # 统一入口获取
    datas = get_idx_data(idx_type, interval, get_conn=True)

    print("{} - 取数据完毕，耗时：{}".format(datetime.now(), time.time() - start_time), flush=True)

    pre_start = time.time()

    data_df = pd.DataFrame(datas)

    preprocess_data(data_df, version, official_typecodes, store=True, file_name=file_name)

    print("{} - 统计完毕，耗时：{}".format(datetime.now(), time.time() - pre_start), flush=True)


def calculate_score_accumulative(version, idx_type, dst_gov_ids):
    """
    @功能：根据基础统计，计算分数/指数
    :param version:
    :param idx_type:
    :param dst_gov_ids:
    :return:
    """

    file_path = pm.STABLE_SCORE_STORAGE + version + "/" + "GovModern/"
    src_file = "{}_ACCUMULATIVE_STATS.csv".format(idx_type.upper())

    df_src = pd.read_csv(file_path + src_file, encoding="utf8", index_col="gov_id")

    df_src = deepcopy(df_src[df_src.index.isin(dst_gov_ids)])

    df_dst = pd.DataFrame()

    COLS_CONF = {"people_data": "民众发布热点数",       # 平均每亿GDP  —— 20191113修改
                 "official_data": "官方发布热点数",     # 平均每亿GDP  —— 20191113修改
                 "people_rltpct": "民众高出平均水平的百分比",
                 "official_rltpct": "官方高出平均水平的百分比",
                 "people_score": "归一后的民众得分",
                 "official_score": "归一后的官方得分"}

    df_dst["people_data"] = df_src["people_total_num"]   #  * 10000 / df_src["gdp"]
    df_dst["official_data"] = df_src["official_total_num"]    # * 10000 / df_src["gdp"]

    # 可能存在gdp为0，可能为空的情况，用中位数补齐
    df_dst = df_dst.fillna(df_dst.median())

    df_dst["people_rltpct"] = (df_dst["people_data"] - df_dst["people_data"].mean()) * 100 / df_dst["people_data"].mean()
    df_dst["official_rltpct"] = (df_dst["official_data"] - df_dst["official_data"].mean()) * 100 / df_dst["official_data"].mean()

    # 只校验正，因为负的最多也就 -100 // 原始值 >= 0  - 不属于异常值校验的问题，是归一校正的问题，不能用异常值检验。。。自己设规则校正score
    people_rltpct_max = df_dst["people_rltpct"].max()

    # 如果最大大于100，才需要做校正
    if people_rltpct_max <= 100:
        df_dst["people_score"] = df_dst["people_rltpct"]
    else:
        # 将超出100的归一到 90~100之间；100以下的归一到 min~90之间  // 分段归一
        people_pos_out100 = deepcopy(df_dst[df_dst["people_rltpct"] >= 100])
        people_pos_out100_max = people_pos_out100["people_rltpct"].max()
        people_pos_out100_min = people_pos_out100["people_rltpct"].min()

        people_pos_under100 = deepcopy(df_dst[(df_dst["people_rltpct"] < 100) & (df_dst["people_rltpct"] >= 0)])
        people_pos_under100_max = people_pos_under100["people_rltpct"].max()
        people_pos_under100_min = people_pos_under100["people_rltpct"].min()

        for index, row in df_dst.iterrows():
            if row["people_rltpct"] < 0:
                df_dst.loc[index, "people_score"] = row["people_rltpct"]
            elif row["people_rltpct"] >= 100:
                if people_pos_out100_min == people_pos_out100_max:   # 超过(>=)100的值都相等的情况，都压到100
                    df_dst.loc[index, "people_score"] = 100
                else:       # 否则默认压到 90~100
                    df_dst.loc[index, "people_score"] = 90 + (row["people_rltpct"]-people_pos_out100_min)*10/(people_pos_out100_max - people_pos_out100_min)
            else:     # [0, 100) 之间的数
                if people_pos_under100_min == people_pos_under100_max:    # 低于100的值都相等的情况，保持原状 // 上限85，避免和 （90，100）打架
                    df_dst.loc[index, "people_score"] = min(people_pos_under100_max, 85)
                else:      # 否则默认压到 min ~ min(people_pos_under100_max, 85)
                    dst_max = min(people_pos_under100_max, 85)
                    df_dst.loc[index, "people_score"] = people_pos_under100_min + (row["people_rltpct"]-people_pos_under100_min)*(dst_max-people_pos_under100_min)/(people_pos_under100_max - people_pos_under100_min)

    # 官方值校验
    official_rltpct_max = df_dst["official_rltpct"].max()

    # 如果最大超过100，才需要做校正
    if official_rltpct_max <= 100:
        df_dst["official_score"] = df_dst["official_rltpct"]
    else:
        # 将超过100的归一到90~100之间；100以下的归一到 min~90 之间  //分段归一
        official_pos_out100 = deepcopy(df_dst[df_dst["official_rltpct"] >= 100])
        official_pos_out100_max = official_pos_out100["official_rltpct"].max()
        official_pos_out100_min = official_pos_out100["official_rltpct"].min()

        official_pos_under100 = deepcopy(df_dst[(df_dst["official_rltpct"] < 100) & (df_dst["official_rltpct"] >= 0)])
        official_pos_under100_max = official_pos_under100["official_rltpct"].max()
        official_pos_under100_min = official_pos_under100["official_rltpct"].min()

        for index, row in df_dst.iterrows():
            if row["official_rltpct"] < 0:
                df_dst.loc[index, "official_score"] = row["official_rltpct"]
            elif row["official_rltpct"] >= 100:
                if official_pos_out100_min == official_pos_out100_max:  # 超过(>=)100的值都相等的情况，都压到100
                    df_dst.loc[index, "official_score"] = 100
                else:  # 否则默认压到 90~100
                    df_dst.loc[index, "official_score"] = 90 + (row["official_rltpct"] - official_pos_out100_min) * 10 / (official_pos_out100_max - official_pos_out100_min)
            else:  # [0, 100) 之间的数
                if official_pos_under100_min == official_pos_under100_max:  # 低于100的值都相等的情况，保持原状 // 上限85，避免和 （90，100）打架
                    df_dst.loc[index, "official_score"] = min(official_pos_under100_max, 85)
                else:  # 否则默认压到 min ~ min(official_pos_under100_max, 85)
                    dst_max = min(official_pos_under100_max, 85)
                    df_dst.loc[index, "official_score"] = official_pos_under100_min + (row["official_rltpct"] - official_pos_under100_min) * (dst_max - official_pos_under100_min) / (official_pos_under100_max - official_pos_under100_min)

    return df_dst


def get_final_accumulative_score(version, idx_type):
    county_ids = gov_info.get_all_county_gov_id_info(with_hitec=False).index.values.tolist()
    city_ids = gov_info.get_all_city_gov_id_info().index.values.tolist()
    prov_ids = gov_info.get_all_province_gov_id_info().index.values.tolist()

    region_ids_dict = {"county": county_ids, "city": city_ids, "prov": prov_ids}

    df_score_list = []

    for region_type, region_ids in region_ids_dict.items():
        df_score = calculate_score_accumulative(version, idx_type, region_ids)
        df_score["region_type"] = region_type
        df_score_list.append(df_score)

    df_score_final = pd.concat(df_score_list)

    df_score_final["gov_name"] = gov_info.df_2861_gaode_geo_all["full_name"]
    file_path = pm.STABLE_SCORE_STORAGE + version + "/" + "GovModern/"
    score_file = "{}_ACCUMULATIVE_SCORE.csv".format(idx_type.upper())
    df_score_final.to_csv(file_path+score_file, encoding="utf8")


def get_score_from_local_to_db(version, idx_type, with_stats_cols=[], test_mode=True):
    """
    @功能：写入数据库，便于后续前端数据的提取
    :param version:
    :param idx_type:
    :param with_stats_cols:
    :param test_mode:
    :return:
    """
    file_path = pm.STABLE_SCORE_STORAGE + version + "/" + "GovModern/"
    score_file = "{}_ACCUMULATIVE_SCORE.csv".format(idx_type.upper())

    df_score = pd.read_csv(file_path + score_file, encoding="utf8", index_col="gov_id")

    dst_cols = ["people_data", "official_data", "people_rltpct", "official_rltpct", "people_score", "official_score"]

    if with_stats_cols:
        stats_file = "{}_ACCUMULATIVE_STATS.csv".format(idx_type.upper())
        df_stats = pd.read_csv(file_path + stats_file, encoding="utf8", index_col="gov_id")
        df_score[with_stats_cols] = df_stats[with_stats_cols]
        dst_cols = dst_cols + with_stats_cols

    df_score["score_info"] = df_score.apply(lambda x: {idx_col: x[idx_col] for idx_col in dst_cols}, axis=1)

    df_score["score_info"] = df_score["score_info"].apply(lambda x: json.dumps(x, ensure_ascii=False))

    df_score["version"] = version

    # df_score["index_type"] = idx_type

    df_score = df_score.reset_index()

    df_into_db = deepcopy(df_score[["gov_id", "score_info", "version"]])

    # 20191114改 不再分表，通过index_type字段区分不同指标
    df_into_db["index_type"] = idx_type

    if test_mode:
        db_obj = DBObj(DBShortName.ProductPWDataTest).obj
    else:
        db_obj = DBObj(DBShortName.ProductPWDataFormal).obj

    db_obj.get_conn()

    # table_name = "{}_score".format(idx_type)

    # 20191114改 不再分表，通过index_type字段区分不同指标
    table_name = "gov_modern_score"

    data_list = df_into_db.to_dict(orient="records")

    sqlstr_head = "INSERT INTO %s (gov_id, score_info, version, index_type) VALUES ({gov_id}, '{score_info}', '{version}', '{index_type}') ON CONFLICT (gov_id, version, index_type) DO UPDATE SET score_info='{score_info}', update_time='%s';"%(table_name, datetime.now())  # 20191114改，新增index_type字段

    sqlstr_list = [sqlstr_head.format(**data_row) for data_row in data_list]

    row_num = len(sqlstr_list)

    for i in range(0, row_num, 1000):
        db_obj.execute_any_sql("".join(sqlstr_list[i: i+1000]))
        print("&&&&&&&&&&已插入{}条&&&&&&&&&&&".format(len(sqlstr_list[i:i+1000])), flush=True)

    db_obj.disconnect()


def get_score_into_knowledge(version, idx_type, update_meta=False):
    """
    @功能：数据写入知识库
    :param version:
    :param idx_type:
    :return:
    """
    score_path = pm.STABLE_SCORE_STORAGE + version + "/" + "GovModern/"
    score_file = "{}_ACCUMULATIVE_SCORE.csv".format(idx_type.upper())

    df_score = pd.read_csv(score_path+score_file, index_col="gov_id", encoding="utf8")

    meta_path = pm.RELATED_FILES
    meta_file = "gov_modern_knowledgemeta.csv"

    df_meta = pd.read_csv(meta_path+meta_file, encoding="utf8")

    df_meta_idx = deepcopy(df_meta[df_meta["index_code"]==idx_type])

    for index, row in df_meta_idx.iterrows():
        data_dict = dict()
        data_dict["type_code"] = row["type_code"]
        data_dict["version"] = version
        data_dict["submitter"] = row["submitter"]
        data_dict["datas"] = df_score[row["column"]].to_dict()

        result = operate_es_knowledge(data_dict, "base", "write")
        print(result, flush=True)

        if update_meta:
            meta_result = operate_es_knowledge(row.to_dict(), "meta", "write")
            print(meta_result, flush=True)


def get_data_main(version, idx_type, calculate_total=True, official_typecodes={"gdp": "9230200102", "per_gdp": "9230200103", "live_popu": "9230200183", "web_popu": "9230200197"}, with_stats_cols=["gdp"], update_meta=False):   # test_mode=True,
    """
    @功能：生成数据总调度
    :param version:
    :param idx_type:
    :param calculate_total:
    :param official_typecodes:
    :param with_stats_cols:
    :param update_meta:
    :return:
    """
    # 基础统计
    get_basic_stats(version, calculate_total=calculate_total, official_typecodes=official_typecodes, idx_type=idx_type)
    print("{} - [基础统计完成] - version={}  idx_type={}".format(datetime.now(), version, idx_type), flush=True)
    # 最终评分
    get_final_accumulative_score(version, idx_type)
    print("{} - [最终评分生成] - version={}  idx_type={}".format(datetime.now(), version, idx_type), flush=True)
    # 写入数据库
    for test_mode in [True, False]:
        get_score_from_local_to_db(version, idx_type, with_stats_cols, test_mode)
        print("{} - [评分及数据已入库] - version={}  idx_type={}  test_mode={}".format(datetime.now(), version, idx_type, test_mode), flush=True)

    # 写入知识库
    get_score_into_knowledge(version, idx_type, update_meta=update_meta)
    print("{} - [评分及数据已写入知识库] - version={}  idx_type={}".format(datetime.now(), version, idx_type), flush=True)


if __name__ == "__main__":
    versions = TimeDispose.get_all_version_dates(pm.STABLE_SCORE_STORAGE, "version.txt")

    # 先跑最新的
    last_version = versions[-1]
    if 0:
        for version_ in versions:
            if version_ == last_version:
                continue
            for idx_type in UNIFORM_DISPOSAL_INDEXES:
                # # 【法治化】已经跑过了，跳过
                # if idx_type == "law":
                #     continue
                get_data_main(version_, idx_type, calculate_total=True, update_meta=False)

    # 数据上传到正式服务器+知识库
    if 0:
        for idx_type in UNIFORM_DISPOSAL_INDEXES:
            # 写入数据库
            get_score_from_local_to_db(last_version, idx_type, ["gdp"], False)
            print("{} - [评分及数据已入库] - version={}  idx_type={}".format(datetime.now(), last_version, idx_type), flush=True)

            # 写入知识库
            get_score_into_knowledge(last_version, idx_type, update_meta=True)
            print("{} - [评分及数据已写入知识库] - version={}  idx_type={}".format(datetime.now(), last_version, idx_type), flush=True)

    if 0:
        # 跑全量数据
        # get_data_main(last_version, "mong", calculate_total=True, test_mode=True, update_meta=True)

        get_score_into_knowledge(last_version, "mong", update_meta=True)

    if 0:
        # 写入正式数据库
        get_score_from_local_to_db(last_version, "mong", ["gdp"], False)

    # 写【政治民主化】和【国家治理现代化】 - 入知识库
    if 1:
        for version_ in versions:
            for idx_type in ["mong"]:  # , "politics"
                get_data_main(version_, idx_type, calculate_total=True, update_meta=False)










