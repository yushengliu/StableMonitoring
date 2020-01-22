#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/12/21 14:22
@Author  : Liu Yusheng
@File    : stable_calculate_score.py
@Description: 稳定监测 —— 口碑评分
"""
import os
import logging
import shutil

import pandas as pd
import numpy as np

from copy import deepcopy
from datetime import datetime, timedelta, date

from db_interface import database

from utils import utilities
from utils import parameters as para
from utils import path_manager as pm
from utils.MyModule import DataBasePython, DataBaseFirm, TimeDispose
# from stable_data_tables import update_accumulatively_score_data_into_es_knowledge, es_knowledge_base_refresh
from utils.get_2861_gaode_geo_gov_id_info import complement_prov_city_data_with_counties, add_gov_name_n_code_based_on_gov_id

df_2861_gaode_geo = para.df_2861_gaode_geo

# 事件信息表
event_server = database.create_user_defined_database_server(host="192.168.0.133",port="6500",user="etherpad", pwd="123456")
event_db = "text-mining"
event_table = "xmd_event_basic_info"

event_db_obj = DataBaseFirm(event_server, event_db, event_table)

weight_dict = {"K_weibo":6, "K_read":1, "K_comment":3, "K_share":2}

# 非正常占比的处理
corr_im_floor = 0.9
corr_im_ceiling = 1

STABLE_SCORE_STORAGE = pm.LOCAL_STABLE_DATA_STORAGE + "scored_origin_result/"

# 日志
logger = logging.getLogger("stable_monitor.score")


# 返回等级
def get_class_grade(value):
    for i in range(len(para.value_limits)):
        if value > para.value_limits[i]:
            return deepcopy(para.map_color_5classes[i])
        else:
            if i == len(para.value_limits) - 1:
                return deepcopy(para.map_color_5classes[i+1])
            else:
                continue


# 五星级评分归一化
def normailize_values(values, max, min):
    values = np.array(values)

    norm_values = (values - min) / (max - min)
    total = norm_values.size
    mean = np.mean(norm_values)
    var = np.var(norm_values)

    return total, mean, var


# wilson 信心评分，正态分布（五星级评分）
def wilson_score_norm(total, mean, var, p_z=2.):
    """
    威尔逊得分计算函数 正态分布版 支持如5星评价 或百分制评价
    :param total: 总数
    :param mean: 均值
    :param var: 方差
    :param p_z: 正态分布分位数 —— 可以取1.96，置信区间95%
    :return:
    """
    score = (mean + np.square(p_z)/(2.*total)-((p_z/(2.*total))*np.sqrt(4.*total*var + np.square(p_z)))) / (1+np.square(p_z)/total)

    return score


# wilson 信心评分，好评率/差评率 排序
def wilson_score(pos, total, p_z=2.):
    """
    威尔逊得分计算函数
    参考：https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval
    :param pos: 正例数
    :param total: 总数
    :param p_z: 正态分布的分位数
    :return: 威尔逊得分
    """

    pos_rate = pos * 1. / total * 1.  # 正例比率
    score = (pos_rate + (np.square(p_z) / (2. * total)) - ((p_z / (2. * total)) * np.sqrt(4. * total * (1. - pos_rate) * pos_rate + np.square(p_z)))) / (1. + np.square(p_z) / total)

    return score


# 计分
def score_rank_in_multi_ways(df_score_rank):
    # ① 稳定相关微博数 / 总微博数
    df_score_rank.loc[:, "score1_weibo_ratio"] = df_score_rank.loc[:, "event_count_weibo"] / df_score_rank.loc[:,"sum_count_weibo"]
    df_score_rank.loc[:, "score1_rank"] = df_score_rank.loc[:, "score1_weibo_ratio"].rank(method='max', ascending=False)
    df_score_rank.loc[:, "score1_pctr"] = df_score_rank.loc[:, "score1_weibo_ratio"].rank(method='max', ascending=True, pct=True) * 100  # 不稳定指数高于全国区县的占比（已经乘了100）

    improper_ratio_max = df_score_rank["score1_weibo_ratio"].max()
    improper_ratio_min = df_score_rank[df_score_rank["score1_weibo_ratio"] > 1]["score1_weibo_ratio"].min()
    if not np.isnan(improper_ratio_min):
        if improper_ratio_max != improper_ratio_min:
            df_score_rank["score1_weibo_ratio"] = df_score_rank["score1_weibo_ratio"].apply(lambda x: x if x < 1 else (corr_im_floor + (x - improper_ratio_min) * (corr_im_ceiling - corr_im_floor) / (improper_ratio_max - improper_ratio_min) - np.random.randint(5) / 100))
        else:
            df_score_rank["score1_weibo_ratio"] = df_score_rank["score1_weibo_ratio"].apply(lambda x: x if x < 1 else round(corr_im_floor + np.random.randint(9) / 100, 2))

        # 在这里修正一下微博总数 （ 因为占比不可能大于1 ，所以总数必须调大 ）
        df_score_rank.loc[df_score_rank["sum_count_weibo"] < df_score_rank["event_count_weibo"], "sum_count_weibo"] = df_score_rank.loc[df_score_rank["sum_count_weibo"] < df_score_rank["event_count_weibo"], "event_count_weibo"] / df_score_rank.loc[:, "score1_weibo_ratio"]

        df_score_rank["sum_count_weibo"] = df_score_rank["sum_count_weibo"].map(int)

    # wilson评分校正
    df_score_rank.loc[:, "score1_wilson"] = df_score_rank.apply(lambda x: wilson_score(x["event_count_weibo"], x["sum_count_weibo"]), axis=1)
    df_score_rank.loc[:, "score1_wilrank"] = df_score_rank.loc[:, "score1_wilson"].rank(method='max', ascending=False)
    df_score_rank.loc[:, "score1_wilpctr"] = df_score_rank.loc[:, "score1_wilson"].rank(method='max', ascending=True, pct=True) * 100

    # ② 稳定事件相关微博影响力值 / 所有微博影响力值  —— 启动追踪后的微博和影响力值可能有超过全值的情况，计算比例时处理一下
    df_score_rank["score2_inf_ratio"] = df_score_rank["stable_value"] / df_score_rank["whole_value"]

    # 占比random一下
    improper_ratio_max = df_score_rank["score2_inf_ratio"].max()
    improper_ratio_min = df_score_rank[df_score_rank["score2_inf_ratio"] > 1]["score2_inf_ratio"].min()
    if not np.isnan(improper_ratio_min):
        df_score_rank["score2_inf_ratio"] = df_score_rank["score2_inf_ratio"].apply(lambda x: x if x < 1 else (corr_im_floor + (x - improper_ratio_min) * (corr_im_ceiling - corr_im_floor) / (improper_ratio_max - improper_ratio_min) - np.random.randint(5) / 100))

        # 在这里修正一下总影响力值 （ 因为占比不可能大于1， 所以总影响力值必须调大 ）
        df_score_rank.loc[df_score_rank["whole_value"] < df_score_rank["stable_value"], "whole_value"] = df_score_rank.loc[df_score_rank["whole_value"] < df_score_rank["stable_value"], "stable_value"] / df_score_rank.loc[df_score_rank["whole_value"] < df_score_rank["stable_value"], "score2_inf_ratio"]

        df_score_rank["whole_value"] = df_score_rank["whole_value"].map(int)

    df_score_rank["score2_rank"] = df_score_rank["score2_inf_ratio"].rank(method='max', ascending=False)
    df_score_rank["score2_pctr"] = df_score_rank["score2_inf_ratio"].rank(method='max', ascending=True, pct=True) * 100

    # Wilson评分校正
    df_score_rank["score2_wilson"] = df_score_rank.apply(lambda x: wilson_score(x["stable_value"], x["whole_value"]),axis=1)
    df_score_rank.loc[:, "score2_wilrank"] = df_score_rank.loc[:, "score2_wilson"].rank(method='max', ascending=False)
    df_score_rank.loc[:, "score2_wilpctr"] = df_score_rank.loc[:, "score2_wilson"].rank(method='max', ascending=True, pct=True) * 100

    # ③ 稳定事件相关的平均每条微博的影响力值 / 所有微博的平均每条的影响力值   —— 首先pass掉，居然是只发生一件事儿的排最前面；明明更重大的事儿（万州公交车、泉港泄漏却被淹没掉了） 2018/12/25
    if 0:
        df_score_rank["score3_avg_inf_ratio"] = df_whole_gov["avg_stable_value"] / df_whole_gov["avg_whole_value"]
        df_score_rank["score3_rank"] = df_score_rank["score3_avg_inf_ratio"].rank(method='max', ascending=False)
        df_score_rank["score3_pctr"] = df_score_rank["score3_avg_inf_ratio"].rank(method='max', ascending=True,
                                                                                  pct=True) * 100

    # ④ 稳定事件相关的微博总影响力值 —— 绝对值，而非相对概念
    df_score_rank["score4_value_rank"] = df_score_rank["stable_value"].rank(method='max', ascending=False)
    df_score_rank["score4_value_pctr"] = df_score_rank["stable_value"].rank(method='max', ascending=True, pct=True) * 100

    # 根据法②的Wilson评分的pctrank评不稳定指数等级
    df_score_rank["stable_index"] = df_score_rank["score2_wilpctr"]
    df_score_rank["stable_extent"] = df_score_rank["stable_index"].apply(lambda x: get_class_grade(x)["name"])

    # 加一个稳定状态的评分 —— 最低限
    df_score_rank["stable_grade"] = 100 - (df_score_rank["stable_index"] - df_score_rank["stable_index"].min()) * 30 / (df_score_rank["stable_index"].max() - df_score_rank["stable_index"].min())

    df_score_rank["stable_pctr"] = df_score_rank["stable_index"].rank(ascending=False, pct=True)

    return df_score_rank


# 从zk_event里提取事件计分 —— 还是用 月度和年度 基础数据是 ①稳定事件相关微博数 / 所有微博数  ②稳定事件相关微博影响力值 / 所有微博影响力值  ③稳定事件相关微博的平均影响力值 / 所有微博的平均影响力值
def calculate_stable_monthly_score(start_date, end_date):
    sqlstr = "SELECT gov_id, gov_name, event_start_time, event_weibo_num, event_count_read, event_count_comment, event_count_share, first_content from %s where event_start_time >= '%s' and event_start_time < '%s' and event_type in ('std', 'stb');"%(event_table, start_date, end_date)

    event_db_obj.get_conn()

    rows = event_db_obj.read_from_table(sqlstr)

    event_db_obj.disconnect()

    df_event = pd.DataFrame(rows)
    df_groupby = df_event.groupby(["gov_id"])
    df_events_gov = df_groupby.agg({"gov_name":"max", "gov_id":"count", "event_weibo_num":"sum", "event_count_read":"sum", "event_count_comment":"sum", "event_count_share":"sum"})   #.reset_index()

    df_events_gov = df_events_gov.rename({"gov_id":"event_count"}, axis="columns")
    df_events_gov = df_events_gov.reset_index()

    # 没有发生事件的区县补齐数据
    gov_ids_left = list(set(df_2861_gaode_geo["gov_id"].tolist())-set(df_events_gov["gov_id"].tolist()))
    gov_names_left = [df_2861_gaode_geo[df_2861_gaode_geo["gov_id"]==k]["full_name"].values[0] for k in gov_ids_left]

    df_events_gov = df_events_gov.append(pd.DataFrame({"gov_id": gov_ids_left, "gov_name": gov_names_left, "event_weibo_num": [0]*len(gov_ids_left), "event_count_read":[0]*len(gov_ids_left), "event_count_comment": [0]*len(gov_ids_left), "event_count_share":[0]*len(gov_ids_left), "event_count":[0]*len(gov_ids_left)}))

    # , "stable_value":[0]*len(gov_ids_left), "avg_stable_value":[0]*len(gov_ids_left)

    df_events_gov = df_events_gov.sort_values(by="gov_id", ascending=True).reset_index(drop=True)

    # dataframe的栏目名命名规则保持一致
    df_events_gov = df_events_gov.rename({"event_weibo_num": "event_count_weibo"}, axis="columns")

    # 该时段各区县所有微博数等信息
    df_totals_gov = utilities.es_weibo_gov_agg_counts(start_date, end_date, logger=logger)

    # 拼接
    df_whole_gov = df_events_gov.join(df_totals_gov.set_index("gov_id"), on="gov_id")

    # 处理特殊区县
    value_cols = [col for col in list(df_whole_gov) if col not in ["gov_id", "gov_name"]]
    for sp_id in para.sp_gov_disposal.keys():
        df_whole_gov.loc[df_whole_gov[df_whole_gov.gov_id == sp_id].index.values[0], value_cols] = df_whole_gov[df_whole_gov["gov_id"].isin(para.sp_gov_disposal[sp_id])][value_cols].sum()

    # 各地区稳定事件的总影响力值
    df_whole_gov["stable_value"] = weight_dict["K_weibo"] * df_whole_gov["event_count_weibo"] + weight_dict["K_read"] * \
                                    df_whole_gov["event_count_read"] + weight_dict["K_comment"] * df_whole_gov[
                                        "event_count_comment"] + weight_dict["K_share"] * df_whole_gov[
                                        "event_count_share"]

    # 各地区稳定事件的平均每条微博的影响力值
    df_whole_gov["avg_stable_value"] = df_whole_gov["stable_value"] / (df_whole_gov["event_count_weibo"] + 1)

    # 去掉nan值 —— 用平均值补齐
    total_cols = list(df_totals_gov)
    total_cols.remove("gov_id")
    values = {key: int(df_whole_gov[key].mean()) for key in total_cols}

    df_whole_gov = df_whole_gov.fillna(value=values)

    # 各地区所有微博的总影响力值
    df_whole_gov["whole_value"] = weight_dict["K_weibo"] * df_whole_gov["sum_count_weibo"] + weight_dict["K_read"] * df_whole_gov["sum_count_read"] + weight_dict["K_comment"] * df_whole_gov["sum_count_comment"] + weight_dict["K_share"] * df_whole_gov["sum_count_share"]

    # 各地区平均每条微博的影响力值
    df_whole_gov["avg_whole_value"] = df_whole_gov["whole_value"] / df_whole_gov["sum_count_weibo"]

    # print(df_whole_gov)

    # 计算评分和排名
    df_score_rank = deepcopy(df_whole_gov[["gov_id", "gov_name", "event_count_weibo", "sum_count_weibo", "stable_value", "whole_value", "event_count"]])

    df_score_rank = score_rank_in_multi_ways(df_score_rank)

    # 没有微博的区县补齐数据 —— 方便后面补齐gov_code，保证数据完整性——为什么不在前面加？因为前面join后缺失的补的是均值，不是真实的0
    gov_ids_noweibo = list(set(df_2861_gaode_geo["gov_id"].tolist()) - set(df_totals_gov["gov_id"].tolist()))

    df_totals_gov = df_totals_gov.append(pd.DataFrame(
        {"gov_id": gov_ids_noweibo, "sum_count_weibo": [0] * len(gov_ids_noweibo), "sum_count_comment":[0]*len(gov_ids_noweibo),
         "sum_count_read": [0] * len(gov_ids_noweibo), "sum_count_share": [0] * len(gov_ids_noweibo)}))

    # 清洗奇怪的gov_id
    df_totals_gov = df_totals_gov.drop(index=df_totals_gov[df_totals_gov.apply(lambda x:x["gov_id"] in (set(df_totals_gov["gov_id"].tolist())-set(df_2861_gaode_geo["gov_id"].tolist())), axis=1)].index)

    df_totals_gov = df_totals_gov.sort_values(by="gov_id", ascending=True).reset_index(drop=True)

    # 所有文件：补一列gov_code，方便后续生成前端文件
    for df_data in [df_events_gov, df_totals_gov, df_whole_gov, df_score_rank]:
        df_data["gov_id"] = df_data["gov_id"].map(int)
        df_data["gov_code"] = df_data["gov_id"].apply(lambda x:df_2861_gaode_geo[df_2861_gaode_geo["gov_id"]==int(x)].index.values[0])

    # 稳定取的是每个月最后一号作为数据版本时间
    version_date = str(datetime.strptime(str(end_date).split(' ')[0], '%Y-%m-%d')-timedelta(days=1)).split(' ')[0]

    file_path = STABLE_SCORE_STORAGE + version_date + '/'

    if not os.path.exists(file_path):
        os.makedirs(file_path)

    # 测试时不必重新生成 —— 当前在测试2018-11数据
    events_file = "STABLE_EVENTS_STATS.csv"
    totals_file = "TOTAL_WEIBO_STATS.csv"
    joint_file = "STABLE_N_TOTAL_STATS.csv"
    score_file = "STABLE_SCORE_FINAL.csv"
    df_events_gov.to_csv(file_path+events_file, encoding="utf-8-sig")
    df_totals_gov.to_csv(file_path+totals_file, encoding="utf-8-sig")
    df_whole_gov.to_csv(file_path+joint_file, encoding="utf-8-sig")

    df_score_rank.to_csv(file_path+score_file, encoding="utf-8-sig")

    TimeDispose.update_version(STABLE_SCORE_STORAGE, "version.txt", version_date)
    return df_whole_gov


# 计算年度评分  _n_update_es
def calculate_stable_yearly_score(start_date, end_date):
    version_dates = TimeDispose.get_all_version_dates(STABLE_SCORE_STORAGE, "version.txt")
    dirs_chosen = []
    df_list = []

    df_yearly = pd.DataFrame()

    for version_date in version_dates:
        if start_date <= version_date <= end_date:
            dirs_chosen.append(version_date)
            file_name = "STABLE_SCORE_FINAL.csv"
            df_monthly = pd.read_csv(STABLE_SCORE_STORAGE+version_date+'/'+file_name, encoding="utf-8")
            df_list.append(df_monthly)

    for info_col in ["gov_id", "gov_name", "gov_code"]:
        df_yearly[info_col] = df_list[0][info_col]

    data_cols = ["event_count_weibo", "sum_count_weibo", "stable_value", "whole_value", "event_count"]
    for data_col in data_cols:
        df_yearly[data_col] = sum([df_m[data_col] for df_m in df_list])

    logger.info("STABLE_accumulatively - dirs_chosen: %s"%dirs_chosen)
    logger.info("STABLE_accumulatively - df num: %d"%len(df_list))
    # print("dirs_chosen:",dirs_chosen)
    # print("df num: %d"%len(df_list))

    df_yearly_score = score_rank_in_multi_ways(df_yearly)

    true_start = dirs_chosen[0][0:7]
    true_end = dirs_chosen[-1][0:7]
    interval = true_start+'_'+true_end
    file_score_name = interval + "_STABLE_SCORE_FINAL.csv"
    df_yearly_score.to_csv(STABLE_SCORE_STORAGE+"score/"+file_score_name, encoding="utf-8-sig")

    # update_accumulatively_score_data_into_es_knowledge(df_yearly_score.set_index("gov_code"), str(TimeDispose(end_date).get_ndays_ago(1)).split(' ')[0])
    # es_knowledge_base_refresh()
    return


def add_prov_city_monthly_data(version_date, file_name=pm.STABLE_MONTHLY_EVENTS_STATS_CSV, columns=pm.stats_cols, add_by="sum"):
    """
    @功能：对月度数据，添加省市数据 - （统计由下辖区县求和，得分由下辖区县求均值）
    :param version_date:
    :param file_name:
    :param columns:
    :param add_by:
    :return:
    """

    file_path = pm.STABLE_SCORE_STORAGE + version_date + "/"
    df_org = pd.read_csv(file_path+file_name, encoding="utf8", index_col="gov_id")

    df_final = deepcopy(df_org[columns])

    df_final = complement_prov_city_data_with_counties(df_final, columns, add_by, verify_non_counties_with_median=True)

    # df_final = add_gov_name_n_code_based_on_gov_id(df_final)

    return df_final


def add_prov_city_sumly_score(version_date, columns=["stable_grade"], add_by="average"):
    """
    @功能：对累计评分，添加省市评分
    :param version_date:
    :param columns:
    :param add_by:
    :return:
    """

    by_version = "-".join(version_date.split("-")[:-1])
    file_path = pm.STABLE_SCORE_STORAGE + "score/"
    file_name ="2018-01_%s_STABLE_SCORE_FINAL.csv"%by_version

    df_org = pd.read_csv(file_path+file_name, encoding="utf8", index_col="gov_id")

    df_final = deepcopy(df_org[columns])
    df_final = complement_prov_city_data_with_counties(df_final, columns, add_by, verify_non_counties_with_median=True)
    # df_final = add_gov_name_n_code_based_on_gov_id(df_final)

    return df_final


def prepare_data_for_es(version_date, dst_file="STABLE_INDEX_FOR_ES.csv"):
    """
    @功能：准备所有需要入es的数据指标，补上省市数据，统一存储
    :param version_date:
    :return:
    """

    df_prepare = add_prov_city_monthly_data(version_date, pm.STABLE_MONTHLY_EVENTS_STATS_CSV, pm.stats_cols, "sum")

    # 补上affect - 万人次
    df_prepare["stable_affect"] = (weight_dict["K_weibo"]*df_prepare["event_count_weibo"] + weight_dict["K_read"]*df_prepare["event_count_read"] + weight_dict["K_comment"]*df_prepare["event_count_comment"] + weight_dict["K_share"]*df_prepare["event_count_share"])/19   # 万人次
    df_prepare["stable_affect"] = df_prepare["stable_affect"].apply(lambda x: round(x, 4))

    df_score_m = add_prov_city_monthly_data(version_date, pm.STABLE_FINAL_MONTHLY_GRADE_CSV, ["stable_grade"], "average")

    if str(version_date).startswith("2018-01"):
        df_score_s = deepcopy(df_score_m)
    else:
        df_score_s = add_prov_city_sumly_score(version_date, ["stable_grade"], "average")

    df_prepare["stable_grade_monthly"] = df_score_m["stable_grade"]

    df_prepare["stable_grade_sumly"] = df_score_s["stable_grade"]

    df_prepare = add_gov_name_n_code_based_on_gov_id(df_prepare)

    dst_path = pm.STABLE_SCORE_STORAGE + version_date + "/"

    df_prepare.to_csv(dst_path+dst_file, encoding="utf8")


# 计算稳定监测指数的主函数
def stable_score_main(start_date, end_date):
    # 当月评分，并将评分的基础数据存成本地文件 csv
    calculate_stable_monthly_score(start_date, end_date)
    logger.info("[已完成当月稳定监测指数及评分计算] 当月：%s ~ %s" % (start_date, end_date))

    # 累计评分
    very_start_date = "2018-01-01"  # 默认都从2018年1月开始；因为稳定的版本记录的是每月的最后一天，所以不从version里面读初始时间了  _n_update_es
    calculate_stable_yearly_score(very_start_date, end_date)
    logger.info("[已完成累计稳定监测指数及评分计算] 累计：%s ~ %s" % (very_start_date, end_date))

    # 将更新后的文件移至客户端文件夹

    version_date = str(datetime.strptime(str(end_date).split(' ')[0], '%Y-%m-%d') - timedelta(days=1)).split(' ')[0]

    file_path = STABLE_SCORE_STORAGE + version_date + '/'

    msrc = file_path + "STABLE_SCORE_FINAL.csv"
    mdst = pm.STABLE_MONTHLY
    shutil.copyfile(msrc, mdst)

    wsrc = STABLE_SCORE_STORAGE + "score/" + very_start_date[0:7] + "_" + start_date[0:7] + "_STABLE_SCORE_FINAL.csv"
    wdst = pm.STABLE_YEARLY
    shutil.copyfile(wsrc, wdst)

    return


if __name__ == "__main__":
    print("stable_calculate_score.py")
    pass

    if 0:
        # start_dates = [str(date(2018, m, 1)) for m in range(1, 13)]    # + ["2019-01-01"]
        # end_dates = [str(date(2018, m, 1)) for m in range(2, 13)] + ["2019-01-01"]     # + ["2019-01-01", "2019-02-01"]
        #
        # # for i in range(0, len(start_dates)):
        # calculate_stable_monthly_score('2018-12-01', '2019-01-01')

        stable_score_main('2019-03-01', '2019-04-01')

    if 0:
        start_date = "2018-01-01"
        end_dates = ["2018-03-01", "2018-04-01", "2018-05-01", "2018-06-01", "2018-07-01", "2018-08-01", "2018-09-01", "2018-10-01", "2018-11-01", "2018-12-01", "2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01"]
        for end_date in end_dates:
            calculate_stable_yearly_score_n_update_es(start_date, end_date)
            print("累计评分文件生成完成，start_date：{}   end_date: {}".format(start_date, end_date), flush=True)

    # 测试
    if 1:
        version_date = "2018-02-28"
        file_name = "STABLE_EVENTS_STATS.csv"
        columns = ["event_count", "event_count_comment", "event_count_read", "event_count_share", "event_count_weibo"]

        prepare_data_for_es(version_date)













