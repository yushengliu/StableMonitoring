#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/9/26 16:09
@Author  : Liu Yusheng
@File    : readjust_stable_score_with_other_elements.py
@Description: 调整稳定评分，使发达地区不能垫底
"""
import pandas as pd
import numpy as np
from copy import deepcopy
from urllib import parse, request
import logging

from utils.MyModule import TimeDispose
import utils.path_manager as pm
import utils.parameters as para
from utils.utilities import get_html_result
from utils.get_2861_gaode_geo_gov_id_info import add_some_id_data, get_parent_prov_id_info, get_prov_sub_city_ids, add_gov_name_n_code_based_on_gov_id

logger = logging.getLogger("stable_monitor.readjust_score")

score_path = pm.STABLE_SCORE_STORAGE
adjust_path = pm.LOCAL_STABLE_DATA_STORAGE + "adjust_stable_score/"

municipal_with_no_counties = ["广东省|东莞市", "广东省|中山市", "海南省|三沙市", "海南省|儋州市", "甘肃省|嘉峪关市"]

# 非正常占比的处理
corr_im_floor = 0.9
corr_im_ceiling = 1


def update_stable_meta(line_num=None):
    df_meta = pd.read_csv(pm.RELATED_FILES + "stable_knowledgemeta.csv", encoding="utf8")

    newly_dict = df_meta.iloc[line_num,:].to_dict() if line_num else df_meta.to_dict()
    result = write_in_es_knowledge(newly_dict, para.META_URL)
    print(result, flush=True)


def write_in_es_knowledge(data_dict, url):
    class_url = url+"operation_type=write&"
    url_values = parse.urlencode(data_dict)
    # print(url_values)
    meta_url = class_url+url_values
    result = get_html_result(meta_url)
    return result


def read_from_es_knowledge(data_dict, url=para.DATA_URL):
    class_url = url+"operation_type=read&"
    url_values = parse.urlencode(data_dict)
    # print(url_values)
    meta_url = class_url+url_values
    result = get_html_result(meta_url)
    return result


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
    # df_score_rank["stable_extent"] = df_score_rank["stable_index"].apply(lambda x: get_class_grade(x)["name"])

    # 加一个稳定状态的评分 —— 最低限
    df_score_rank["stable_grade"] = 100 - (df_score_rank["stable_index"] - df_score_rank["stable_index"].min()) * 30 / (df_score_rank["stable_index"].max() - df_score_rank["stable_index"].min())

    df_score_rank["stable_pctr"] = df_score_rank["stable_index"].rank(ascending=False, pct=True)

    return df_score_rank


def get_provs_timeline_basic_data_from_counties(src_file="STABLE_N_TOTAL_STATS.csv", dst_region_type="prov", dst_file_name="PROV_STABLE_N_TOTAL_STATS.csv"):

    basic_cols = ["event_count_weibo", "sum_count_weibo", "stable_value", "whole_value", "event_count", "event_count_comment", "event_count_share", "event_count_read"]   # "gov_id", "gov_name",

    versions = TimeDispose.get_all_version_dates(score_path, "version.txt")

    for v_i in range(len(versions)):

        src_path = score_path + versions[v_i] + "/"

        df_v = pd.read_csv(src_path + src_file, encoding="utf-8", index_col="gov_code")

        # 省
        if dst_region_type == "prov":
            df_region = deepcopy(para.df_2861_gaode_geo_all[(para.df_2861_gaode_geo_all["gov_type"].isin([0, 2]))][["gov_id", "full_name"]])
            code_prefix_num = 2
        # 市
        elif dst_region_type == "muni":
            # 把gov_type=5的县级市作为区县（不作为城市）处理，行政区划上本来也与区县同级
            # &(~para.df_2861_gaode_geo_all["full_name"].isin(para.municipal_with_no_counties))
            df_region = deepcopy(para.df_2861_gaode_geo_all[para.df_2861_gaode_geo_all["gov_type"].isin([1])][["gov_id", "gov_type", "full_name"]])
            code_prefix_num = 4
        # 区县
        else:
            df_region = deepcopy(para.df_2861_gaode_geo[["gov_id", "gov_type", "full_name"]])
            code_prefix_num = 6

        df_region = df_region.rename(columns={"full_name": "gov_name"}) # 列名重命名

        for gov_code in df_region.index.values:
            df_counties = df_v.filter(regex="\A"+str(gov_code)[0: code_prefix_num], axis=0)
            for basic_col in basic_cols:
                if df_counties.shape[0]:
                    df_region.loc[gov_code, basic_col] = df_counties[basic_col].sum()
                else:
                    df_region.loc[gov_code, basic_col] = np.nan

        # # 把几个无下辖区县的市的数据，暂时用所有城市数据的中位数补齐，不对最后结果产生极端影响即可
        # df_region = df_region.fillna(df_region.median())

        # 单独处理无下辖区县的市的数据 - 用所在省下辖市数据的中位数
        if dst_region_type == "muni":
            for city_id_ in [2058, 2059, 2212, 2213, 2927]:
                prov_id_ = get_parent_prov_id_info(city_id_)["gov_id"]
                sub_city_ids_ = get_prov_sub_city_ids(prov_id_)

                df_region = add_some_id_data(df_region, city_id_, sub_city_ids_, by="median")

        # 先不着急归一评分，把其他几项数据拿到再说

        # 归一化
        stable_value_max = df_region["stable_value"].max()
        stable_value_min = df_region["stable_value"].min()
        df_region["stable_value_score"] = df_region["stable_value"].apply(lambda x: 100 - (x - stable_value_min) * 100 /(stable_value_max - stable_value_min))

        # 用排名来试试
        df_region["stable_value_pctr"] = df_region["stable_value"].rank(ascending=False, pct=True) * 100

        # stable_value / whole_value - 定了，稳定以影响力值为准；环境以数量和影响力值分别计算后加权
        df_region["value_ratio"] = df_region.apply(lambda x: x["stable_value"] / x["whole_value"] if x["whole_value"] else 0, axis=1)

        ratio_max = df_region["value_ratio"].max()
        ratio_min = df_region["value_ratio"].min()
        df_region["ratio_score"] = df_region["value_ratio"].apply(lambda x: 100 - (x - ratio_min) * 100 / (ratio_max - ratio_min))

        # 用排名来试试
        df_region["ratio_pctr"] = df_region["value_ratio"].rank(ascending=False, pct=True)*100

        # wilson评分
        df_region["value_ratio_wilson"] = df_region.apply(lambda x: wilson_score(x["stable_value"], x["whole_value"]), axis=1)

        wilson_max = df_region["value_ratio_wilson"].max()
        wilson_min = df_region["value_ratio_wilson"].min()
        # 越大抱怨越突出，越差
        df_region["wilson_score"] = df_region["value_ratio_wilson"].apply(lambda x: 100 - (x - wilson_min) / (wilson_max - wilson_min))
        df_region["wilson_pctr"] = df_region["value_ratio_wilson"].rank(ascending=False, pct=True)*100

        typecodes_dict = {"gdp": "9230200102", "per_gdp": "9230200103", "live_popu": "9230200183"}

        df_region = df_region.reset_index().set_index("gov_id")

        for type_str, type_code in typecodes_dict.items():

            # es_data = read_from_es_knowledge(data_dict={"type_code": type_code, "version": versions[v_i]})
            #
            # content_data = es_data["content"]["datas"]
            #
            # v_j = v_i
            #
            # times = len(versions)
            #
            # while (len(content_data) == 0) and (times != 0):
            #     times -= 1
            #
            #     if v_j < len(versions) - 1:
            #         v_j = v_j +1
            #     else:
            #         v_j = v_j -1
            #     es_data = read_from_es_knowledge(data_dict={"type_code": type_code, "version": versions[v_j]})
            #     content_data = es_data["content"]["datas"]

            content_data = get_valid_es_data_by_near(type_code, versions[v_i], versions)

            df_es = pd.DataFrame.from_dict(data=content_data, orient="index")

            df_es.index = df_es.index.astype(int)

            df_region[type_str] = df_es[type_code]

            df_region = df_region.fillna(df_region.median())

            # 归一化 —— 都是越大越好
            type_max = df_region[type_str].max()
            type_min = df_region[type_str].min()
            df_region["%s_score"%type_str] = df_region[type_str].apply(lambda x: (x - type_min) * 100 / (type_max - type_min))
            # 用排名来试试
            df_region["%s_pctr"%type_str] = df_region[type_str].rank(ascending=True, pct=True)*100

        # 突出指数和程度
        df_region["stable_grade"] = df_region["wilson_pctr"]
        df_region["stable_idx_"] = df_region["value_ratio_wilson"]
        df_region["stable_idx_"] = df_region["stable_idx_"].rank(method="max", ascending=True, pct=True) * 100
        df_region["stable_extent"] = df_region["stable_idx_"].apply(lambda x: get_class_grade(x)["name"])

        df_region.to_csv(score_path+versions[v_i]+"/"+dst_file_name, encoding="utf-8")


def get_valid_es_data_by_near(type_code, dst_version, optional_versions):
    """
    @功能：获取有效es数据，针对有时版本号不对，取数据为空的情况
    :param type_code:
    :param dst_version:
    :param optional_versions:
    :return:
    """

    v_i = optional_versions.index(dst_version)

    es_data = read_from_es_knowledge(data_dict={"type_code": type_code, "version": optional_versions[v_i]})

    content_data = es_data["content"]["datas"]

    v_j = v_i

    times = len(optional_versions)

    # and (times != 0)
    while (len(content_data) == 0):

        if times == 0:
            print("所有版本尝试完毕，均未取到数据！", flush=True)
            return False

        times -= 1

        if v_j < len(optional_versions) - 1:
            v_j = v_j + 1
        else:
            v_j = v_j - 1

        es_data = read_from_es_knowledge(data_dict={"type_code": type_code, "version": optional_versions[v_j]})

        content_data = es_data["content"]["datas"]

    return content_data


def adjust_provs_env_score_by_linear_weighting(src_file="PROV_STABLE_N_TOTAL_STATS.csv", weight_dict={"stable_value":0.3, "ratio": 0, "wilson": 0.3, "gdp":0.1, "per_gdp":0.3, "live_popu": 0}, col_suffix="pctr", dst_file="PROV_GRADES_TIMELINE_raw_pergdp_gdp_631_pctr.csv", score_type="grade", rewrite_src_file=False):
    """
    @功能：score = w1 * 抱怨值 + w2 * （稳定抱怨值占比/稳定抱怨值占比的Wilson评分） + w3 * GDP + w4 * 人均GDP + w5 * 常驻人口
    :param src_file:
    :param weight_dict:
    :param col_suffix:
    :param dst_file:
    :return:
    """

    versions = TimeDispose.get_all_version_dates(score_path, "version.txt")

    df_prov_adjust = pd.DataFrame()

    for version in versions:

        src_v_file = score_path + version + "/" + src_file

        # if df_prov_adjust.shape[0] == 0:
        #
        #     df_v = pd.read_csv(src_v_file, encoding="utf-8")
        #
        #     # 先从第一个csv拿出信息列，并作为index
        #     df_prov_adjust["gov_id"] = df_v["gov_id"]
        #     df_prov_adjust["gov_code"] = df_v["gov_code"]
        #     df_prov_adjust["full_name"] = df_v["full_name"]
        #
        #     df_prov_adjust[version] = sum([df_v["%s_%s"%(key, col_suffix)]*value for key, value in weight_dict.items()])

        df_v = pd.read_csv(src_v_file, encoding="utf-8")

        df_v = df_v.set_index(["gov_id", "gov_code", "gov_name"])

        df_prov_adjust[version] = sum([df_v["%s_%s" % (key, col_suffix)] * value for key, value in weight_dict.items()])

        if rewrite_src_file:
            df_v["stable_grade"] = deepcopy(df_prov_adjust[version])
            df_v["stable_idx_"] = df_v["stable_grade"].rank(method="max", ascending=False, pct=True)*100
            df_v["stable_extent"] = df_v["stable_idx_"].apply(lambda x: get_class_grade(x)["name"])

            # 再把stable_grade变成pctr
            df_v["stable_grade"] = df_v["stable_grade"].rank(ascending=True, pct=True) * 100
            df_v.to_csv(src_v_file, encoding="utf-8")

        if score_type == "rank":
            df_prov_adjust[version] = df_prov_adjust[version].rank(ascending=False)

        if score_type == "pct_rank":
            df_prov_adjust[version] = df_prov_adjust[version].rank(ascending=True, pct=True)*100

    df_prov_adjust.to_csv(adjust_path+dst_file, encoding="utf8")


def init_knowledge_data_n_write_into_es(type_code, version, submitter, datas, write_into_es=True):
    data_dict = dict()
    data_dict["type_code"] = type_code
    data_dict["version"] = version
    data_dict["submitter"] = submitter
    data_dict["datas"] = datas

    if write_into_es:
        result = write_in_es_knowledge(data_dict, para.DATA_URL)
        print(result, flush=True)

    return data_dict


def get_stable_score_adjusted_into_knowledge_es(version=None, update_newliest_only=False):

    adjust_final_path = adjust_path   # + "final/"

    file_name = "{}_PCTRANKS_TIMELINE_raw_pergdp_gdp_631_pctr.csv"

    df_prov = pd.read_csv(adjust_final_path + file_name.format("PROV"), encoding="utf8")
    df_muni = pd.read_csv(adjust_final_path + file_name.format("MUNI"), encoding="utf8")
    df_county = pd.read_csv(adjust_final_path + file_name.format("COUNTY"), encoding="utf8")

    df_total_data = pd.concat([df_prov, df_muni, df_county])

    columns = df_total_data.columns.tolist()

    version_total_cols = [col for col in columns if col not in ["gov_id", "gov_code", "gov_name"]]

    newliest_version = max(version_total_cols)

    if update_newliest_only:
        version_cols = [newliest_version]

    else:
        if version is not None:
            version_cols = [version]
        else:
            version_cols = version_total_cols

    df_total_data = df_total_data.set_index(["gov_id"])

    type_code = "4530200101"
    submitter = "yusheng.liu"

    for version_col in version_cols:
        version = version_col
        datas = dict()

        for gov_code in para.df_2861_gaode_geo_all.index:
            gov_id = int(para.df_2861_gaode_geo_all.loc[gov_code, "gov_id"])
            # 没有的数据用中位值替代
            if gov_id not in df_total_data.index.values:
                datas[gov_id] = round(df_total_data[version_col].median(), 2)
            else:
                datas[gov_id] = round(df_total_data.loc[gov_id, version_col], 2)
        init_knowledge_data_n_write_into_es(type_code, version, submitter, datas)
        print("type_code={}  version={}  data_num={}".format(type_code, version, len(datas)), flush=True)


# ===================================== 按月更新 ======================================
ADJUST_WEIGHT_DICT = {"stable_value":0.3, "ratio": 0, "wilson": 0.3, "gdp":0.1, "per_gdp":0.3, "live_popu": 0}
LINEAR_ELEMENT_TYPE = "pctr"


def get_region_basic_data_from_counties_by_version(version_date, region_type="prov"):
    basic_cols = ["event_count_weibo", "sum_count_weibo", "stable_value", "whole_value", "event_count"]  # "gov_id", "gov_name", "event_count_comment", "event_count_share", "event_count_read"

    versions = TimeDispose.get_all_version_dates(score_path, "version.txt")

    src_path = score_path + version_date + "/"

    src_file = "STABLE_N_TOTAL_STATS.csv"

    dst_file = "{}_STABLE_N_TOTAL_STATS.csv".format(region_type.upper())

    df_v = pd.read_csv(src_path + src_file, encoding="utf-8", index_col="gov_code")

    # 省
    if region_type == "prov":
        df_region = deepcopy(para.df_2861_gaode_geo_all[(para.df_2861_gaode_geo_all["gov_type"].isin([0, 2]))][["gov_id", "full_name"]])
        code_prefix_num = 2
    # 市
    elif region_type == "muni":
        # 把gov_type=5的县级市作为区县（不作为城市）处理，行政区划上本来也与区县同级
        # &(~para.df_2861_gaode_geo_all["full_name"].isin(para.municipal_with_no_counties))
        df_region = deepcopy(para.df_2861_gaode_geo_all[para.df_2861_gaode_geo_all["gov_type"].isin([1, 31])][["gov_id", "gov_type", "full_name"]])   # , 31 - 下辖无区县的市单独处理
        code_prefix_num = 4
    # 区县
    else:
        df_region = deepcopy(para.df_2861_gaode_geo[["gov_id", "gov_type", "full_name"]])
        code_prefix_num = 6

    df_region = df_region.rename(columns={"full_name": "gov_name"})  # 列名重命名

    for gov_code in df_region.index.values:
        df_counties = df_v.filter(regex="\A" + str(gov_code)[0: code_prefix_num], axis=0)
        for basic_col in basic_cols:
            if df_counties.shape[0]:
                df_region.loc[gov_code, basic_col] = df_counties[basic_col].sum()
            else:
                df_region.loc[gov_code, basic_col] = np.nan

    # 把几个无下辖区县的市的数据，暂时用所有城市数据的中位数补齐，不对最后结果产生极端影响即可
    # df_region = df_region.fillna(df_region.median())

    # 单独处理无下辖区县的市的数据 - 用所在省下辖市数据的中位数
    if region_type == "muni":
        for city_id_ in [2058, 2059, 2212, 2213, 2927]:
            prov_id_ = get_parent_prov_id_info(city_id_)["gov_id"]
            sub_city_ids_ = get_prov_sub_city_ids(prov_id_)

            # 要先把df_region从以gov_code为索引改为以gov_id为索引
            df_region = df_region.reset_index()
            df_region = df_region.set_index("gov_id")
            df_region[basic_cols] = add_some_id_data(df_region[basic_cols], city_id_, sub_city_ids_, by="median")
            # df_region = add_gov_name_n_code_based_on_gov_id(df_region)
            # df_region = df_region.fillna(31)
            df_region = df_region.reset_index()
            df_region = df_region.set_index("gov_code")

    # 先不着急归一评分，把其他几项数据拿到再说

    # 归一化
    stable_value_max = df_region["stable_value"].max()
    stable_value_min = df_region["stable_value"].min()
    df_region["stable_value_score"] = df_region["stable_value"].apply(lambda x: 100 - (x - stable_value_min) * 100 / (stable_value_max - stable_value_min))

    # 用排名来试试
    df_region["stable_value_pctr"] = df_region["stable_value"].rank(ascending=False, pct=True) * 100

    # stable_value / whole_value - 定了，稳定以影响力值为准；环境以数量和影响力值分别计算后加权
    df_region["value_ratio"] = df_region.apply(
        lambda x: x["stable_value"] / x["whole_value"] if x["whole_value"] else 0, axis=1)

    ratio_max = df_region["value_ratio"].max()
    ratio_min = df_region["value_ratio"].min()
    df_region["ratio_score"] = df_region["value_ratio"].apply(
        lambda x: 100 - (x - ratio_min) * 100 / (ratio_max - ratio_min))

    # 用排名来试试
    df_region["ratio_pctr"] = df_region["value_ratio"].rank(ascending=False, pct=True) * 100

    # wilson评分
    df_region["value_ratio_wilson"] = df_region.apply(lambda x: wilson_score(x["stable_value"], x["whole_value"]), axis=1)

    wilson_max = df_region["value_ratio_wilson"].max()
    wilson_min = df_region["value_ratio_wilson"].min()
    # 越大抱怨越突出，越差
    df_region["wilson_score"] = df_region["value_ratio_wilson"].apply(
        lambda x: 100 - (x - wilson_min) / (wilson_max - wilson_min))
    df_region["wilson_pctr"] = df_region["value_ratio_wilson"].rank(ascending=False, pct=True) * 100

    typecodes_dict = {"gdp": "9230200102", "per_gdp": "9230200103", "live_popu": "9230200183"}

    df_region = df_region.reset_index().set_index("gov_id")

    for type_str, type_code in typecodes_dict.items():

        content_data = get_valid_es_data_by_near(type_code, version_date, versions)

        df_es = pd.DataFrame.from_dict(data=content_data, orient="index")

        df_es.index = df_es.index.astype(int)

        df_region[type_str] = df_es[type_code]

        df_region = df_region.fillna(df_region.median())

        # 归一化 —— 都是越大越好
        type_max = df_region[type_str].max()
        type_min = df_region[type_str].min()
        df_region["%s_score" % type_str] = df_region[type_str].apply(
            lambda x: (x - type_min) * 100 / (type_max - type_min))
        # 用排名来试试
        df_region["%s_pctr" % type_str] = df_region[type_str].rank(ascending=True, pct=True) * 100

    # 突出指数和程度
    df_region["stable_grade"] = df_region["wilson_pctr"]
    df_region["stable_idx_"] = df_region["value_ratio_wilson"]
    df_region["stable_idx_"] = df_region["stable_idx_"].rank(method="max", ascending=True, pct=True) * 100
    df_region["stable_extent"] = df_region["stable_idx_"].apply(lambda x: get_class_grade(x)["name"])

    df_region.to_csv(score_path + version_date + "/" + dst_file, encoding="utf-8")


def adjust_score_by_linear_weighting_by_version(version_date, weight_dict=ADJUST_WEIGHT_DICT, col_suffix=LINEAR_ELEMENT_TYPE):
    """
    @功能：根据调权重确认的一版想法，更新某版本的最终调整后的分值、排名、百分比排名
    :param version_date:
    :param weight_dict:
    :param col_suffix:
    :return:
    """

    region_types = ["county", "muni", "prov"]

    src_path = score_path + version_date + "/"

    regions_df_list = []

    for region_type in region_types:

        src_file = "{}_STABLE_N_TOTAL_STATS.csv".format(region_type.upper())

        df_v = pd.read_csv(src_path+src_file, encoding="utf8")

        df_v = df_v.set_index(["gov_id", "gov_code", "gov_name"])

        df_ad = pd.DataFrame()

        df_ad["raw_grade"] = sum([df_v["%s_%s" % (key, col_suffix)] * value for key, value in weight_dict.items()])

        df_ad["rank"] = df_ad["raw_grade"].rank(ascending=False)

        df_ad["pct_rank"] = df_ad["raw_grade"].rank(ascending=True, pct=True)*100

        df_ad["region_type"] = region_type

        regions_df_list.append(df_ad)

    df_final = pd.concat(regions_df_list)

    dst_file = "ADJUSTED_STABLE_SCORE.csv"

    df_final.to_csv(src_path+dst_file, encoding="utf8")


def get_stable_adjusted_score_into_es_by_version(version_date):

    src_path = score_path + version_date + "/"
    src_file = "ADJUSTED_STABLE_SCORE.csv"

    df_adjusted = pd.read_csv(src_path+src_file, encoding="utf8", index_col="gov_id")

    type_code = "4530200101"
    submitter = "yusheng.liu"

    datas = df_adjusted["pct_rank"].to_dict()

    init_knowledge_data_n_write_into_es(type_code, version_date, submitter, datas)
    print("type_code={}   version={}   data_num={}".format(type_code, version_date, len(datas)))
    logger.info("[已完成校正后的指数存入知识库] %s   code: %s   data_num: %d"%(version_date, type_code, len(datas)))


def readjust_main(version_date):
    """
    @功能：数据调整总调度 - 月更新
    :param version_date:
    :return:
    """
    # Step1:提取省/市/县的基础数据，引入GDP，人均gdp，常驻人口等元素，进行评分（归一/pctr）
    for region_type in ["prov", "muni", "county"]:
        get_region_basic_data_from_counties_by_version(version_date, region_type)

    # Step2:按之前人工校验确定合理的权重ADJUST_WEIGHT_DICT 和 元素的分数类型LINEAR_ELEMENT_TYPE，线性调整得到修正后的分数、排名、百分比排名
    adjust_score_by_linear_weighting_by_version(version_date)

    # Step3:将校正后的指数（pctr）写入知识库
    get_stable_adjusted_score_into_es_by_version(version_date)


if __name__ == "__main__":

    # 历史数据
    # # Step1:提取省/市/县的基础数据和各子项的评分
    # if 1:
    #     get_provs_timeline_basic_data_from_counties(dst_region_type="county", dst_file_name="COUNTY_STABLE_N_TOTAL_STATS.csv")
    #
    # # Step2:调整权重，得到评分，人工查验合理性
    # if 1:
    #     region_type = "COUNTY"    # 省/市/县
    #
    #     adjust_provs_env_score_by_linear_weighting(src_file="%s_STABLE_N_TOTAL_STATS.csv"%region_type, weight_dict={"stable_value": 0.3, "ratio": 0, "wilson": 0.3, "gdp": 0.1, "per_gdp": 0.3, "live_popu": 0}, col_suffix="pctr", dst_file="%s_GRADES_TIMELINE_raw_pergdp_gdp_631_pctr.csv"%region_type, score_type="grade", rewrite_src_file=True)
    #     adjust_provs_env_score_by_linear_weighting(src_file="%s_STABLE_N_TOTAL_STATS.csv"%region_type, weight_dict={"stable_value":0.3, "ratio": 0, "wilson": 0.3, "gdp":0.1, "per_gdp":0.3, "live_popu": 0}, col_suffix="pctr", dst_file="%s_RANKS_TIMELINE_raw_pergdp_gdp_631_pctr.csv"%region_type, score_type="rank")
    #     adjust_provs_env_score_by_linear_weighting(src_file="%s_STABLE_N_TOTAL_STATS.csv"%region_type, weight_dict={"stable_value":0.3, "ratio": 0, "wilson": 0.3, "gdp":0.1, "per_gdp":0.3, "live_popu": 0}, col_suffix="pctr", dst_file="%s_PCTRANKS_TIMELINE_raw_pergdp_gdp_631_pctr.csv"%region_type, score_type="pct_rank")
    #
    # # Step3:将校正后的指数写入知识库
    # if 1:
    #     get_stable_score_adjusted_into_knowledge_es()

    # 月度更新
    if 1:
        versions_ = TimeDispose.get_all_version_dates(score_path, "version.txt")
        for version_ in versions_:
            readjust_main(version_)










