#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/12/21 10:22
@Author  : Liu Yusheng
@File    : stable_client_page.py
@Description: 稳定监测前端展示
"""
import os
import re
import shutil
import time
import logging
import pandas as pd
import numpy as np
from copy import deepcopy

# import logging.handlers

from db_interface import database
from utils import path_manager as pm
from utils import parameters as para
from utils import utilities
from utils.MyModule import TimeDispose, DataBaseFirm
from utils.web_charts import get_colored_mixed_line_dict, get_mixed_line_dict, get_stackbar_dict, get_textbox2_dict, get_wordcloud_dict
from utils.md_events import get_running_trace_seed_list_by_govs


logger = logging.getLogger('stable_main.client_web')

df_node = pd.read_csv(pm.CONFIG_NODE, dtype="str", encoding="utf-8")
df_2861_geo_all = para.df_2861_gaode_geo_all
df_2861_county = para.df_2861_gaode_geo

# 记得stable的数据暂时没有gov_code， 只有gov_id，之后考虑补上 —— 2018/12/29
df_stable_monthly = pd.DataFrame()
df_stable_yearly = pd.DataFrame()

df_stable_dict = {}

# 每个区县的事件详情
df_gov_events_details = pd.DataFrame()

UPDATE_MONTHS_LIST = []   # 2018-10
x_name_list = []   # 2018年10月
x_name_list_with_sizes = []  # 含span标签的横坐标
UPDATE_MONTHS_DATA_DF = {}
until_date = ""   # 统计截止 —— 当月最后一天

time_dict = {"monthly": "月度", "yearly": "年度"}

# events_inf_dict_in_city = {}   # 同一个市内，每个区县逐月的事件数+覆盖人次的dict。如：{"2":{

stable_corners = ["TOP_STABLE", "NODE_STABLE_STATUS", "NODE_STABLE_WARNING", "NODE_STABLE_TREND", "NODE_STABLE_SYSTEM", "NODE_STABLE_EXAMPLE"]

stable_codes = ["STABLE_STATUS", "STABLE_TREND"]

y_label_size = 16

# 事件信息表
event_server = database.create_user_defined_database_server(host="192.168.0.133",port="6500",user="etherpad", pwd="123456")
event_db = "text-mining"
event_table = "xmd_event_basic_info"

event_db_obj = DataBaseFirm(event_server, event_db, event_table)

max_title_words = 10

max_content_len = 100

monthly_top = 5
yearly_top = 10

# 去掉最高频的10% 和 最低频的10% 的关键词
thd_ratio = 0.1

stop_words = ["收费站", "好消息", "交通管理", "车流量", "候鸟", "耶稣", "数字身份", "暖到我了", "电子收费", "演唱会", "限行", "招租", "租房", "宠物领养", "团购", "看房"
              , "茶馆"]


# 全局变量赋值
def assign_global_variables():
    global df_stable_monthly
    global df_stable_yearly
    global df_stable_dict
    global UPDATE_MONTHS_LIST
    global UPDATE_MONTHS_DATA_DF
    global x_name_list
    global x_name_list_with_sizes
    global until_date

    df_stable_monthly = pd.read_csv(pm.STABLE_MONTHLY, encoding="utf-8", index_col="gov_code")
    df_stable_yearly = pd.read_csv(pm.STABLE_YEARLY, encoding="utf-8", index_col="gov_code")

    df_stable_dict = {"monthly":df_stable_monthly, "yearly":df_stable_yearly}

    # 趋势图数据
    trend_version_path = pm.STABLE_SCORE_STORAGE + "version.txt"
    file_name = para.stable_monthly_score_csv

    trend_versions = open(trend_version_path, encoding="utf-8").readlines()
    UPDATE_MONTHS_LIST = ['-'.join(m.split('-')[0:-1]) for m in trend_versions]

    for month in trend_versions:
        file_path = pm.STABLE_SCORE_STORAGE + month.strip() + '/'
        df = pd.read_csv(file_path+file_name, encoding="utf-8", index_col="gov_code")
        UPDATE_MONTHS_DATA_DF['-'.join(month.split('-')[0:-1])] = df

    x_name_list = [month.replace('-', '年')+'月' if len(month.split('-0')) == 1 else month.replace('-0', '年') + '月' for month in UPDATE_MONTHS_LIST]

    # 横坐标大小随柱状图增加 自动调整减小
    x_name_list_with_sizes = [
        "<span style='font-size:%dpx;color:%s'>%s</span>" % (16 * 11 / len(x_name_list), para.FT_PURE_WHITE, x_name) for
        x_name in x_name_list]

    until_date = trend_versions[-1].strip()

    return


# 取事件详情的数据 —— xmd_event_basic_info表
def get_events_details_from_db(gov_id, interval=[], limit=0):
    # gov_id = df_2861_county.loc[gov_code, "gov_id"]

    sqlstr = "SELECT gov_id, gov_name, event_title, event_start_time, event_weibo_num, event_count_read, event_count_comment,event_count_share, sensitive_word, department, gov_post, scope_grade, thd_grade, actual_value, first_content from %s where event_type in ('std', 'stb') and gov_id=%d" % (
    event_table, gov_id)

    if interval:
        sqlstr += " and event_start_time >= '%s' and event_start_time < '%s'" % (interval[0], interval[1])

    if limit:
        sqlstr += " LIMIT %d;" % limit

    event_db_obj.get_conn()

    rows = event_db_obj.read_from_table(sqlstr)

    event_db_obj.disconnect()
    
    return rows


# 生成【稳定监测】角的第一页 —— 事件数的历史走势和影响人次双坐标混合图
def get_status_local_mixed1_trend(gov_code):
    global df_gov_events_details

    gov_id = df_2861_county.loc[gov_code, "gov_id"]
    gov_name = df_2861_county.loc[gov_code, "full_name"]

    # 特殊处理的区县
    if int(gov_id) in para.sp_gov_disposal.keys():
        detail_rows = []
        for sec_id in para.sp_gov_disposal[int(gov_id)]:
            detail_rows.extend(get_events_details_from_db(sec_id))
    else:
        detail_rows = get_events_details_from_db(gov_id)

    title = "<span style='color:%s;font-weight:bold;font-size:24px;'>%s</span> %s 不稳定舆情事件及传播影响 - 逐月变化" % (
        para.UN_TITLE_YELLOW, gov_name.split('|')[-1], UPDATE_MONTHS_LIST[0] + "~" + UPDATE_MONTHS_LIST[-1])
    subtitle = "[每日采集，按月发布] - 统计截止：%s&nbsp&nbsp&nbsp测算时段：%s" % (
        until_date, UPDATE_MONTHS_LIST[0] + '~' + UPDATE_MONTHS_LIST[-1])

    if len(detail_rows):
        df_gov_events_details = pd.DataFrame(detail_rows)


        # 过滤一下停用词
        df_gov_events_details = df_gov_events_details[df_gov_events_details["first_content"].apply(lambda x: not re.search("|".join(stop_words), x))]

    else:
        df_gov_events_details = pd.DataFrame()

    # 有数据的情况
    # if len(detail_rows):
    if df_gov_events_details.shape[0] > 0:
        # df_gov_events_details = pd.DataFrame(detail_rows)
        #
        # # 过滤一下停用词
        # df_gov_events_details = df_gov_events_details[df_gov_events_details["first_content"].apply(lambda x: not re.search("|".join(stop_words), x))]

        df_gov_events_details["event_start_time"] = df_gov_events_details["event_start_time"].map(str).str.split('.', n=-1, expand=True).iloc[:, 0]

        # 按发生时间排序
        df_gov_events_details = df_gov_events_details.sort_values(by="event_start_time", ascending=False)

        month_detail_list = []

        for month in UPDATE_MONTHS_LIST:
            month_details = "<span style='color:%s'>■</span>&nbsp&nbsp<span style='color:%s;font-size:16px;font-weight:bold;'>%s  部分不稳定事件详情：</span><br/>"%(para.FT_ORANGE, para.FT_PURE_WHITE, month)
            month_start = month + '-01 00:00:00'
            month_end = TimeDispose(month_start).get_next_month_first_day() + " 00:00:00"
            df_month = df_gov_events_details[(df_gov_events_details["event_start_time"] >= month_start) & (df_gov_events_details["event_start_time"] < month_end)]

            # 按事件严重程度+时间近远排序
            # df_month = df_month.sort_values(by=["thd_grade", "actual_value", "event_start_time"], ascending=(["C级", "B级", "A级", "Z级"], False, False))

            df_month = df_month.sort_values(by=["actual_value", "event_start_time"], ascending=(False, False))

            event_no = 0
            for index, row in df_month.iterrows():
                event_no += 1
                month_details += "<%d> <span style='color:%s'>[%s]</span> 时间：%s  <br/>主题：%s<br/>"%(event_no, para.WCOLORS[row["thd_grade"][0]], row["scope_grade"], row["event_start_time"], " ".join(row["event_title"][0:max_title_words]) if isinstance(row["event_title"], list) else " ".join(eval(row["event_title"])[0:max_title_words]))
                month_details += "详情：%s<br/><br/>"%(row["first_content"] if (len(row["first_content"])>0 and len(row["first_content"])<=max_content_len) else "暂无" if len(row["first_content"]) == 0 else row["first_content"][0:max_content_len] + "...")

            month_detail_list.append(month_details)

        df_mixed1 = pd.DataFrame({"x_name": x_name_list_with_sizes, "value": [
            UPDATE_MONTHS_DATA_DF[m].loc[UPDATE_MONTHS_DATA_DF[m]["gov_id"] == gov_id, "event_count"].values[0] for m in
            UPDATE_MONTHS_LIST], "rank": [
            int(UPDATE_MONTHS_DATA_DF[m].loc[UPDATE_MONTHS_DATA_DF[m]["gov_id"] == gov_id, "stable_value"].values[0] / 19)
            for m in UPDATE_MONTHS_LIST], "color": [para.FT_ORANGE] * len(x_name_list), "text": month_detail_list})

    # 没有数据的情况
    else:
        df_mixed1 = pd.DataFrame({"x_name": x_name_list_with_sizes, "value": [
            UPDATE_MONTHS_DATA_DF[m].loc[UPDATE_MONTHS_DATA_DF[m]["gov_id"] == gov_id, "event_count"].values[0] for m in
            UPDATE_MONTHS_LIST], "rank": [
            int(UPDATE_MONTHS_DATA_DF[m].loc[UPDATE_MONTHS_DATA_DF[m]["gov_id"] == gov_id, "stable_value"].values[0] / 19)
            for m in UPDATE_MONTHS_LIST], "color": [para.FT_ORANGE] * len(x_name_list)})

    tips = {"left": "<span style='font-size:%dpx;color:%s'>不稳定事件数（件）</span>" % (y_label_size, para.FT_PURE_WHITE),
            "left_style": '{"color":"%s", "fontSize":"%dpx"}' % (para.FT_PURE_WHITE, y_label_size),
            "right": "<span style='font-size:%dpx;color:%s'>总传播覆盖人次（万）</span>" % (y_label_size, para.FT_PURE_WHITE),
            "right_style": '{"color":"%s", "fontSize":"%dpx"}' % (para.FT_PURE_WHITE, y_label_size), "rightColor": "red"}

    data_dict = get_mixed_line_dict(title, subtitle, df_mixed1, tips=tips)
    data_dict_name = "local_mixed1_trend"
    return data_dict, data_dict_name


# 生成【稳定监测】角的第二页 —— 同一市内各个县的事件数走势
def get_status_counties_multi_trends(gov_code):
    gov_name = df_2861_county.loc[gov_code, "full_name"]
    gov_id = df_2861_county.loc[gov_code, "gov_id"]
    gov_municipal = df_2861_county.filter(regex='\A'+str(gov_code)[0:4], axis=0)

    gov_ids = gov_municipal["gov_id"].values.tolist()
    gov_names = gov_municipal["full_name"].values.tolist()

    # 把当前区县调整到第一个
    gov_names.remove(gov_name)
    gov_ids.remove(gov_id)

    gov_names.insert(0, gov_name)
    gov_ids.insert(0, gov_id)

    # 折线还是太多了 —— 先选前五个， 等天博那边功能支持点选/搜索后再改回来 —— 2019/1/2
    gov_names = gov_names[0:5] if len(gov_names) >= 5 else gov_names
    gov_ids = gov_ids[0:5] if len(gov_ids) >= 5 else gov_ids

    df_data = pd.DataFrame()
    df_info = pd.DataFrame(index=gov_names, columns=["name", "type", "color"])

    # 暂时配置另外四个颜色写死
    colors = [para.EC_AIR_BLUE, para.EC_WATER_ORANGE, para.EC_NOI_PURPLE, para.FT_PURE_WHITE]

    for i in range(len(gov_names)):
        df_data[gov_names[i]] = [UPDATE_MONTHS_DATA_DF[m].loc[UPDATE_MONTHS_DATA_DF[m]["gov_id"] == gov_ids[i], "event_count"].values[0] for m in UPDATE_MONTHS_LIST]
        df_info.loc[gov_names[i], "name"] = gov_names[i].split('|')[-1]
        df_info.loc[gov_names[i], "type"] = "line"
        if gov_names[i] == gov_name:
            df_info.loc[gov_names[i], "color"] = "red"
            # df_info.loc[gov_names[i], "color"] = para.FT_ORANGE
            df_info.loc[gov_names[i], "name"] = "当前区县：" + gov_names[i].split('|')[-1]
        # else:
        #     df_info.loc[gov_names[i], "color"] = colors[i-1]

    # 纵坐标 —— 加颜色和字号
    yAxis = {"name": "不稳定事件数（件）", "color": para.FT_PURE_WHITE, "fontsize":18, "min":0, "max":max(df_data.max().values)}

    df_data["x_name"] = x_name_list_with_sizes

    title = "<span style='color:#ffcc00;font-weight:bold;font-size:24px;'>%s</span> %s %s各区县不稳定事件数 - 逐月变化" % (gov_name.split('|')[-1], UPDATE_MONTHS_LIST[0] + "~" + UPDATE_MONTHS_LIST[-1],
        gov_name.split('|')[-2])

    sub_title = "[每日采集，按月发布] - 统计截止：%s&nbsp&nbsp&nbsp   测算时段：%s" % (
    until_date, UPDATE_MONTHS_LIST[0] + '~' + UPDATE_MONTHS_LIST[-1])

    multi_trends = get_colored_mixed_line_dict(title, sub_title, df_data, df_info, yAxis)
    data_name = "counties_multi_trends"

    return multi_trends, data_name


# 生成【稳定监测】角的第三/四页 —— 同一市内各个区县的月度/年度 不稳定事件数+传播人次对标 —— 倒堆叠图
def get_status_counties_stackbar(gov_code, time_type="monthly", data_type="event_count"):
    gov_name = df_2861_county.loc[gov_code, "full_name"]
    df_stable = df_stable_dict[time_type]

    if time_type == "monthly":
        period_desc = UPDATE_MONTHS_LIST[-1]
    else:
        period_desc = UPDATE_MONTHS_LIST[0] + "~" + UPDATE_MONTHS_LIST[-1]

    if data_type == "event_count":
        data_desc = "不稳定事件数（评估事件多少）"
        yAxis_name = "单位：（件）"
        color = para.FT_ORANGE                      # para.FT_ORANGE
        # data_str = str(list(df_stable_municipal))
    elif data_type == "stable_value":
        data_desc = "传播覆盖总人次（评估事件大小）"   # （综合评论、转发、点赞等数目评估）
        yAxis_name = "单位：（万人次）"
        color = para.FT_LIGHT_RED
    else:
        data_desc = ""
        yAxis_name = ""
        color = ""

    subtitle = "统计截止：%s"%until_date

    title = "<span style='color:%s;font-size:24px;font-weight:bold;'>%s</span> %s <span style='color:%s'>%s</span>%s - %s各区县分布" % (
        para.UN_TITLE_YELLOW, gov_name.split('|')[-1], period_desc, para.UN_TITLE_YELLOW, time_dict[time_type], data_desc, gov_name.split('|')[-2])

    df_stable_municipal = df_stable.filter(regex='\A'+str(gov_code)[0:4], axis=0)

    # 按数据大小排一下
    df_stable_municipal = df_stable_municipal.sort_values(by=data_type, axis=0, ascending=False)

    df_stable_municipal["colored_extent"] = df_stable_municipal["stable_extent"].apply(lambda x:"<span style='color:%s'>"%para.grade2color[x]+x+"</span>")

    # xAxis_name = list(df_stable_municipal["gov_name"].str.split('|', n=-1, expand=True).iloc[:, -1] + '[' + df_stable_municipal["colored_extent"] + ']')

    xAxis_name = list(df_stable_municipal["gov_name"].str.split('|', n=-1, expand=True).iloc[:, -1])

    # 把本区县标黄/加粗/放大
    for i in range(len(xAxis_name)):
        if gov_name.split('|')[-1] in xAxis_name[i]:
            xAxis_name[i] = "<span style='color:%s;font-size:22px'><b>"%para.FT_ORANGE + xAxis_name[i] + "</b></span>"

    yAxis_dict = {"name":yAxis_name, "color":para.FT_PURE_WHITE, "fontsize":16}

    # 数据
    df_data = pd.DataFrame({"color":[color], "name":[data_desc], "data":[str(list(df_stable_municipal[data_type].map(int))) if data_type == "event_count" else str(list(round(df_stable_municipal[data_type]/19, 2))) if data_type == "stable_value" else " "]})

    stackbar_dict = get_stackbar_dict(title, subtitle, xAxis_name, yAxis_dict, df_data, percentage=False)
    dict_name = "counties_"+data_type.split('_')[-1]+"_"+time_type + "_stackbar"

    return stackbar_dict, dict_name


# 生成【稳定监测】角的第五页 —— 本区县月度TOP3事件 对标 年度TOP5事件
def get_status_top_events_textbox(gov_code, time_type="monthly"):
    # df_stable = df_stable_dict[time_type]
    gov_name = df_2861_county.loc[gov_code, "full_name"]

    event_toptitle = ""  # "事件详情"

    # 年度有事件的情况
    if df_gov_events_details.shape[0] > 0:
        if time_type == "monthly":
            period_desc = UPDATE_MONTHS_LIST[-1]
            month_start = period_desc + "-01 00:00:00"
            month_end = TimeDispose(period_desc).get_next_month_first_day() + " 00:00:00"
            df_events = df_gov_events_details[(df_gov_events_details["event_start_time"] >= month_start) & (df_gov_events_details["event_start_time"] < month_end)]
            line_thd = monthly_top
        else:
            period_desc = UPDATE_MONTHS_LIST[0] + "~" + UPDATE_MONTHS_LIST[-1]
            df_events = df_gov_events_details
            line_thd = yearly_top

        # 月度没有数据
        if df_events.shape[0] == 0:
            event_toplist = ["本月暂未监测到不稳定事件。"]

        else:
            # rows = get_events_details_from_db(gov_code)
            df_events = df_events.sort_values(by=["actual_value", "event_start_time"], ascending=(False, False))

            # "thd_grade", ["C级", "B级", "A级", "Z级"],

            df_show = df_events[0:line_thd]

            event_toplist = []

            event_no = 0
            for index, row in df_show.iterrows():
                event_no += 1
                event_details = "<%d> <span style='color:%s'>[%s]</span> 关键字：<span style='color:%s'>%s</span> <br/>时间：%s  传播覆盖人次：%s<br/>详情：%s<br>"%(event_no, para.WCOLORS[row["thd_grade"][0]], row["scope_grade"], para.UN_TITLE_YELLOW, " ".join(row["event_title"][0:max_title_words]) if isinstance(row["event_title"], list) else " ".join(eval(row["event_title"])[0:max_title_words]), row["event_start_time"], utilities.get_proper_unit_data(row["actual_value"]*1000/1.9), row["first_content"] if (len(row["first_content"])>0 and len(row["first_content"])<=max_content_len) else "暂无" if len(row["first_content"]) == 0 else row["first_content"][0:max_content_len] + "...")
                event_toplist.append(event_details)

    # 年度没有事件的情况
    else:
        if time_type == "monthly":
            period_desc = UPDATE_MONTHS_LIST[-1]
            line_thd = monthly_top
        else:
            period_desc = UPDATE_MONTHS_LIST[0] + "~" + UPDATE_MONTHS_LIST[-1]
            line_thd = yearly_top
        event_toplist = ["本区县暂未监测到不稳定事件。"]

    df_fieldset = pd.DataFrame({"text-align":["left"], "scroll":[0], "height":["90%"], "top_title":[event_toptitle], "content_list":[str(event_toplist)]})

    title = "<span style='color:%s;font-size:24px;font-weight:bold;'>%s</span> %s <span style='color:%s'>%s</span> 不稳定事件 TOP%d" % (
    para.UN_TITLE_YELLOW, gov_name.split('|')[-1], period_desc, para.UN_TITLE_YELLOW, time_dict[time_type], line_thd)

    subtitle = ""

    content_textbox = get_textbox2_dict(title, subtitle, df_fieldset, margin_top=False)
    dict_name = "%s_top_events_textbox"%time_type
    return content_textbox, dict_name


# 生成【稳定监测】角的第六页 —— 所有不稳定事件敏感词/涉及官员/部门 的词云图
def get_status_sensitive_wordcloud(gov_code, time_type="monthly"):
    gov_name = df_2861_county.loc[gov_code, "full_name"]

    if df_gov_events_details.shape[0] > 0:
        if time_type == "monthly":
            period_desc = UPDATE_MONTHS_LIST[-1]
            month_start = period_desc + "-01 00:00:00"
            month_end = TimeDispose(period_desc).get_next_month_first_day() + " 00:00:00"
            df_events = df_gov_events_details[(df_gov_events_details["event_start_time"] >= month_start) & (
                        df_gov_events_details["event_start_time"] < month_end)]
        else:
            period_desc = UPDATE_MONTHS_LIST[0] + "~" + UPDATE_MONTHS_LIST[-1]
            df_events = df_gov_events_details

        # 月度没有事件发生
        if df_events.shape[0] == 0:
            words_list = ["本月暂未监测到不稳定事件。"]

        else:
            df_events = df_events.sort_values(by=["actual_value", "event_start_time"], ascending=(False, False))

            words_list = []
            for index, row in df_events.iterrows():
                words_list += sum([row[word_type] if isinstance(row[word_type], list) else eval(row[word_type]) for word_type in ["sensitive_word", "department", "gov_post"]], [])   # , "department", "gov_post"

            remove_duplicate = list(set(words_list))
            remove_duplicate = sorted(remove_duplicate, key=lambda x:words_list.count(x), reverse=True)

            min_thd = int(len(remove_duplicate)*thd_ratio)
            max_thd = int(len(remove_duplicate)*(1-thd_ratio))+1

            remove_duplicate = remove_duplicate[min_thd: max_thd]

            words_list = [i for i in words_list if i in remove_duplicate]

    # 年度没有不稳定事件发生
    else:
        if time_type == "monthly":
            period_desc = UPDATE_MONTHS_LIST[-1]
        else:
            period_desc = UPDATE_MONTHS_LIST[0] + "~" + UPDATE_MONTHS_LIST[-1]

        words_list = ["本区县暂未监测到不稳定事件。"]

    title = "<span style='color:%s;font-size:24px;font-weight:bold;'>%s</span> %s <span style='color:%s'>%s</span> 民意高频词" % (
        para.UN_TITLE_YELLOW, gov_name.split('|')[-1], period_desc, para.UN_TITLE_YELLOW, time_dict[time_type])
    subtitle = ""

    wordcloud_dict = get_wordcloud_dict(title, subtitle, words_list)
    dict_name = "%s_wordcloud"%time_type

    return wordcloud_dict, dict_name


# 生成【稳定监测】角的基础数据
def generate_stable_status_basic_datas(gov_code):

    stable_basic_data_list = []
    stable_basic_data_name_list = []

    # page1 —— mixed1 柱子（事件数）+折线（传播覆盖人次）双坐标图
    # 先不用判断年度一件事都没有的情况 —— 少数 —— 要判断了哦，会报错 2019/1/7
    local_mixed1_trend, mixed1_dict_name = get_status_local_mixed1_trend(gov_code)

    stable_basic_data_list.append(local_mixed1_trend)
    stable_basic_data_name_list.append(mixed1_dict_name)

    # page2 —— 同一个市内各区县的不稳定事件数多折线图
    multi_trends, multi_trends_name = get_status_counties_multi_trends(gov_code)

    stable_basic_data_list.append(multi_trends)
    stable_basic_data_name_list.append(multi_trends_name)

    # page3（月度） / 4（年度） —— 同一个市内各区县的不稳定事件数VS影响人次对标 倒式堆叠条状图
    for time_type in time_dict.keys():
        for data_type in ["event_count", "stable_value"]:
            stackbar_dict, stackbar_name = get_status_counties_stackbar(gov_code, time_type, data_type)
            stable_basic_data_list.append(stackbar_dict)
            stable_basic_data_name_list.append(stackbar_name)

    # page5 —— 月度TOP事件 VS 年度TOP事件 对标
    for time_type in time_dict.keys():
        textbox_dict, textbox_name = get_status_top_events_textbox(gov_code, time_type)
        stable_basic_data_list.append(textbox_dict)
        stable_basic_data_name_list.append(textbox_name)

    # page6 —— 涉及官员/部门/敏感词 词云
    for time_type in time_dict.keys():
        wordcloud_dict, wordcloud_name = get_status_sensitive_wordcloud(gov_code, time_type)
        stable_basic_data_list.append(wordcloud_dict)
        stable_basic_data_name_list.append(wordcloud_name)

    return stable_basic_data_list, stable_basic_data_name_list


# 生成【稳定监测】角的配置文件
def generate_stable_status_settings(gov_code):
    gov_code_str = str(gov_code)[0:6]
    gov_name = df_2861_county.loc[gov_code, "full_name"]

    node_code = "STABLE_STATUS"

    setting_list = []
    setting_name_list = []

    # page1 —— 历史走势mixed1
    graph_name = "不稳定风险分析"
    setting = {}
    setting["title"] = graph_name
    data_dict = {}
    data_dict["id"] = "local_mixed1_trend"
    data_dict["node_code"] = node_code
    data_dict["name"] = graph_name
    data_dict["data"] = gov_code_str + '/' + data_dict["id"]

    setting["datas"] = [data_dict]

    setting_list.append(setting)
    setting_name_list.append("setting")

    # page2 —— 本市各区县的历史走势 - 多折线图
    setting_list.append({"title": graph_name, "datas":[{"id": "counties_multi_trends", "node_code":node_code, "name":graph_name, "data":gov_code_str + '/' + "counties_multi_trends"}]})
    setting_name_list.append("setting_multi")

    # page3 —— 本市各区县月度事件数VS影响人次对标
    setting_list.append({"title":graph_name, "datas": [{"id": "counties_count_monthly_stackbar", "node_code":node_code, "name":"事件数", "data":gov_code_str+'/'+"counties_count_monthly_stackbar"}, {"id":"counties_value_monthly_stackbar", "node_code":node_code, "name":"传播人次", "data":gov_code_str+'/'+"counties_value_monthly_stackbar", "multi_show":"on"}]})
    setting_name_list.append("setting_compare_monthly_stackbar")

    # page4 —— 本市各区县年度事件数VS影响人次对标
    setting_list.append({"title": graph_name, "datas": [
        {"id": "counties_count_yearly_stackbar", "node_code": node_code, "name": "事件数",
         "data": gov_code_str + '/' + "counties_count_yearly_stackbar"},
        {"id": "counties_value_yearly_stackbar", "node_code": node_code, "name": "传播人次",
         "data": gov_code_str + '/' + "counties_value_yearly_stackbar", "multi_show": "on"}]})
    setting_name_list.append("setting_compare_yearly_stackbar")

    # page5 —— 本县月度TOP 对标 年度TOP
    setting_list.append({"title":graph_name, "datas":[{"id":"monthly_top_events_textbox", "node_code":node_code, "name":"月度", "data": gov_code_str + '/' + "monthly_top_events_textbox"}, {"id":"yearly_top_events_textbox", "node_code":node_code, "name":"年度", "data": gov_code_str + '/' + "yearly_top_events_textbox", "multi_show":"on"}]})
    setting_name_list.append("setting_compare_top_textbox")

    # page6 —— 本县月度民意高频词 对标 年度民意高频词
    setting_list.append({"title": graph_name, "datas": [
        {"id": "monthly_wordcloud", "node_code": node_code, "name": "月度",
         "data": gov_code_str + '/' + "monthly_wordcloud"},
        {"id": "yearly_wordcloud", "node_code": node_code, "name": "年度",
         "data": gov_code_str + '/' + "yearly_wordcloud", "multi_show": "on"}]})
    setting_name_list.append("setting_compare_wordcloud")

    return setting_list, setting_name_list


# 生成【稳定监测】角的右侧描述
def generate_stable_status_list_desc(gov_code, page_settings):
    gov_code_str = str(gov_code)[0:6]
    gov_name = df_2861_county.loc[gov_code, "full_name"]

    node_code = "STABLE_STATUS"

    list_desc = {}
    list_desc["page_list"] = {}
    list_desc["page_list"]["setting_list"] = [node_code + '/' + gov_code_str + '/' + i for i in page_settings]
    list_desc["local_mixed1_trend"] = {"title":"", "sub_title":"", "width": "30%", "datas": [{"cols":[{"text":"<div style='padding-top:400px; padding-bottom:400px;'><span style='color:%s;font-weight:bold;font-size:18px'>【点击左侧柱子查看具体事件】<br/><br/>模型准确率：75%%</span></div>"%para.UN_TITLE_YELLOW}]}], "desc": "{}不稳定事件及传播影响逐月变化见左图，点击柱子可查看具体事件。".format(gov_name.split("|")[-1])}

    list_desc["counties_multi_trends"] = {"title":"", "sub_title":"", "width": "30%", "datas": [{"cols":[{"text":""}]}], "desc": "{}与同省市兄弟区县的不稳定事件数走势对比见左图".format(gov_name.split("|")[-1])}

    # counties_count_monthly_stackbar - 取右边的便于解读 20191114
    list_desc["counties_value_monthly_stackbar"] = {"title":"", "sub_title":"", "width": "30%", "datas": [{"cols":[{"text":""}]}], "desc": "本页从当月发生的不稳定事件的数量和影响力，来看{}及兄弟区县的排名变化".format(gov_name.split("|")[-1])}

    # counties_count_yearly_stackbar
    list_desc["counties_value_yearly_stackbar"] = {"title": "", "sub_title": "", "width": "30%",
                                                    "datas": [{"cols": [{"text": ""}]}], "desc": "本页从2018年1月至今，发生的不稳定事件的数量和影响力，来看{}及兄弟区县的排名变化".format(gov_name.split("|")[-1])}

    # monthly_top_events_textbox
    list_desc["yearly_top_events_textbox"] = {"title": "", "sub_title": "", "width": "30%",
                                                    "datas": [{"cols": [{"text": ""}]}], "desc": "本页展示了{}月度影响力最大的五件事，和年度影响力最大的十件事".format(gov_name.split("|")[-1])}

    # monthly_wordcloud
    list_desc["yearly_wordcloud"] = {"title": "", "sub_title": "", "width": "30%",
                                               "datas": [{"cols": [{"text": ""}]}], "desc": "本页展示了{}月度与年度民意高频词的对比".format(gov_name.split("|")[-1])}

    return list_desc


# 生成【稳定监测】角的HTML
def STABLE_STATUS_HTML_CONTENT(gov_code, target_path):
    # gov_id = df_2861_county.loc[gov_code, "gov_id"]
    basic_datas, basic_data_names = generate_stable_status_basic_datas(gov_code)
    settings, setting_names = generate_stable_status_settings(gov_code)
    right_desc = generate_stable_status_list_desc(gov_code, setting_names)

    if len(settings) > 0:
        for set in range(len(settings)):
            utilities.write_client_datafile_json(target_path, setting_names[set], '.json', settings[set])

        for pos in range(len(basic_datas)):
            data_file_name = basic_data_names[pos]
            data_dict = basic_datas[pos]
            utilities.write_client_datafile_json(target_path, data_file_name.split('.')[0], '.json', data_dict)

        utilities.write_client_datafile_json(target_path, 'list', '.json', right_desc)

    return


# 生成【历史趋势】角的HTML
def STABLE_TREND_HTML_CONTENT(gov_code, target_path):
    basic_datas, basic_data_names = generate_stable_status_basic_datas(gov_code)
    settings, setting_names = generate_stable_status_settings(gov_code)
    right_desc = generate_stable_status_list_desc(gov_code, setting_names)

    if len(settings) > 0:
        for set in range(len(settings)):
            utilities.write_client_datafile_json(target_path, setting_names[set], '.json', settings[set])

        for pos in range(len(basic_datas)):
            data_file_name = basic_data_names[pos]
            data_dict = basic_datas[pos]
            utilities.write_client_datafile_json(target_path, data_file_name.split('.')[0], '.json', data_dict)

        utilities.write_client_datafile_json(target_path, 'list', '.json', right_desc)

    pass


# 生成网页内容
def generate_html_content(gov_code, node_code, file_date):
    gov_code_str = str(gov_code)[0:6]
    # gov_id = df_2861_county.loc[gov_code, "gov_id"]

    target_dir_path = pm.LOCAL_STABLE_CLIENT_DATA_STORAGE + node_code + '/' + file_date + '/' + gov_code_str + '/'
    if not os.path.exists(target_dir_path):
        os.makedirs(target_dir_path)

    if node_code == "STABLE_STATUS":
        STABLE_STATUS_HTML_CONTENT(gov_code, target_dir_path)

    if node_code == "STABLE_TREND":
        STABLE_TREND_HTML_CONTENT(gov_code, target_dir_path)

    return


# 生成叶子节点的数据
def web_leaves_datafile(provinces, file_date):
    """
    :param provinces:
    :param file_date:
    :return:
    """

    # 遍历指定省

    time_loop_start = time.time()

    # for node_name in stable_corners:
    for node_code in stable_codes:
        count = 0
        # node_type = df_node[df_node["name"] == node_name]["is_bottom"].values[0]
        # display_type = df_node[df_node["name"] == node_name]["display_type"].values[0]
        # node_code = df_node[df_node["name"] == node_name]["code"].values[0]
        #
        # if node_type != '2':
        #     continue
        # if display_type != 'html':
        #     continue
        # if node_code == "STABLE_WARNING":
        #     continue

        # 测试
        if 0:
            if node_code == "STABLE_TREND":
                continue

        # 同期数据如果重跑，就删掉本来的文件夹
        if os.path.exists(pm.LOCAL_STABLE_DATA_STORAGE+node_code+'/'+file_date):
            shutil.rmtree(pm.LOCAL_STABLE_DATA_STORAGE+node_code+'/'+file_date)

        for prov in provinces:
            reg = '\A' + prov
            df_county_in_province = df_2861_county.filter(regex=reg, axis=0)
            gov_codes = df_county_in_province.index
            # 遍历一个省下的所有县
            for gov_code in gov_codes:
                count += 1
                if gov_code in df_2861_county.index.values:
                    generate_html_content(gov_code, node_code, file_date)
                time_loop1 = time.time()
                print("\r %s - 当前进度：%.2f%%，耗时：%.2f秒，还剩：%.2f秒"%(node_code, (count*100/2852), (time_loop1-time_loop_start), (time_loop1-time_loop_start)*(2852-count)/count), end="", flush=True)

    return


# 产生前端主界面数据文件 —— 2018/12/28
def generate_datafile(provinces, main_flag=True, syn_ftp=False, method='web'):
    app_date = TimeDispose.get_last_version_date(pm.STABLE_SCORE_STORAGE, "version.txt")
    if main_flag:
        if method == "web":
            assign_global_variables()
            web_leaves_datafile(provinces, app_date)
            logger.info("[已完成前端数据生成] 数据版本：%s" % app_date)

    if syn_ftp:
        upload_web_details()

    return app_date


# 上传前端web界面文件
def upload_web_details():
    app_date = TimeDispose.get_last_version_date(pm.STABLE_SCORE_STORAGE, "version.txt")

    # 打包上传前端细节数据
    for corner_code in stable_codes:
        utilities.zip_detail_web_datafile(corner_code, app_date)
        utilities.upload_detail_web_datafile_zip(corner_code, app_date)

    # 更新前端数据版本信息到数据库
    conn = utilities.init_XMD_DB_CONN(True)
    for corner_code in stable_codes:
        utilities.insert_web_updates_to_db(conn, corner_code, app_date, flag=0)

    conn.disconnect()
    logger.info("[已完成前端数据上传FTP] 数据版本：%s"%app_date)

    return


# 在数据库中插入边角注释文字信息
def corner_description_db(provinces):
    # 先命名全局变量
    assign_global_variables()

    for prov in provinces:
        reg = '\A'+prov
        df_county_in_province = df_2861_county.filter(regex=reg, axis=0)
        gov_codes = df_county_in_province.index
        for gov_code in gov_codes:
            if gov_code in df_county_in_province.index.values:
                print("generate %s corner desc."%gov_code)
                corner_interpret_dict(gov_code)


# 返回边角注解文字，字典格式
def corner_interpret_dict(gov_code):
    gov_id = df_2861_county.loc[gov_code, "gov_id"]
    current_time = TimeDispose.get_current_time()
    corner_dict = {}

    # TOP_STABLE的注脚  更新时间：%s\n  current_time,
    corner_dict[stable_corners[0]] = "本区县当月不稳定事件共监测到：<b><color=red><size=100%%>%d</size></color></b>件；\n不稳定事件传播覆盖人次：<b><color=red><size=100%%>%s</size></color></b>；\n地区当月不稳定风险指数高于全国<b><color=red><size=100%%>%.2f%%</size></color></b>的区县。"%(df_stable_monthly.loc[gov_code, "event_count"], utilities.get_proper_unit_data(df_stable_monthly.loc[gov_code, "stable_value"]*1000/1.9), df_stable_monthly.loc[gov_code, "stable_index"])

    # STABLE_STATUS的注脚 —— 之后数据端补一下省内排名 2019/1/7 （待定）

    df_prov_m = deepcopy(df_stable_monthly.filter(regex="\A"+str(gov_code)[:2], axis=0))
    df_prov_y = deepcopy(df_stable_yearly.filter(regex="\A"+str(gov_code)[:2], axis=0))
    df_city_m = deepcopy(df_stable_monthly.filter(regex="\A"+str(gov_code)[:4], axis=0))
    df_city_y = deepcopy(df_stable_yearly.filter(regex="\A" + str(gov_code)[:4], axis=0))
    # print(df_prov_m, flush=True)
    df_prov_m["stable_index"] = df_prov_m["stable_index"].rank(pct=True,ascending=True)
    df_prov_y["stable_index"] = df_prov_y["stable_index"].rank(pct=True, ascending=True)
    df_city_m["stable_index"] = df_city_m["stable_index"].rank(pct=True, ascending=True)
    df_city_y["stable_index"] = df_city_y["stable_index"].rank(pct=True, ascending=True)
    corner_dict[stable_corners[1]] = "<b><color=#ffcc00><size=100%%>[月度更新] %s：统计截止：%s</size></color></b>；\n系统监测不稳定事件：\n当月：<b><color=red><size=100%%>%d</size></color></b>件；本年度：<b><color=red><size=100%%>%d</size></color></b>件；\n综合不稳定风险指数：\n月度：高于全市<b><color=red><size=100%%>%.2f%%</size></color></b>的区县，高于全省<b><color=red><size=100%%>%.2f%%</size></color></b>的区县，高于全国<b><color=red><size=100%%>%.2f%%</size></color></b>的区县；年度：高于全市<b><color=red><size=100%%>%.2f%%</size></color></b>的区县，高于全省<b><color=red><size=100%%>%.2f%%</size></color></b>的区县，高于全国<b><color=red><size=100%%>%.2f%%</size></color></b>的区县。"%(UPDATE_MONTHS_LIST[-1].replace('-0', '年').replace('-', '年')+'月', until_date, df_stable_monthly.loc[gov_code, "event_count"], df_stable_yearly.loc[gov_code, "event_count"], df_city_m.loc[gov_code, "stable_index"], df_prov_m.loc[gov_code, "stable_index"], df_stable_monthly.loc[gov_code, "stable_index"], df_city_y.loc[gov_code, "stable_index"], df_prov_y.loc[gov_code, "stable_index"], df_stable_yearly.loc[gov_code, "stable_index"])

    # STABLE_WARNING的注脚
    corner_dict[stable_corners[2]] = "<b><color=#ffcc00><size=100%%>[实时更新]</size></color></b>追踪全国不稳定事件，最快20分钟采样一次；数据推送每小时更新。"

    running_seeds = get_running_trace_seed_list_by_govs([gov_id], 100, "stb", 1, False)

    if len(running_seeds):
        df_basic_events = pd.DataFrame(running_seeds,
                                       columns=["events_head_id", "gov_id", "gov_name", "key_word_str", "start_time",
                                                "sync_time", "search_cnt"])
        corner_dict[stable_corners[2]] += "\n本区县上一次稳定事件预警发生在：%s"%str(df_basic_events["sync_time"].values[0]).split('T')[0]

    else:
        corner_dict[stable_corners[2]] += "\n到{}，本区县未发现触及预警标准的事件".format(UPDATE_MONTHS_LIST[-1].replace('-0', '年').replace('-', '年')+'月')


    # STABLE_TREND的注脚
    corner_dict[stable_corners[3]] = "一直记录和监测本区县不稳定事件数月度走势"

    corner_dict[stable_corners[4]] = ' '

    corner_dict[stable_corners[5]] = ' '

    conn = utilities.init_XMD_DB_CONN(True, '0.133')

    for corner_name in stable_corners:
        utilities.insert_node_description(conn, gov_id, str(gov_code)[0:6], corner_name, corner_dict[corner_name], value_type=1)

    conn.disconnect()

    return corner_dict


if __name__ == "__main__":
    provinces = [
        '11', '12', '13', '14', '15',
        '21', '22', '23',
        '31', '32', '33', '34', '35', '36', '37',
        '41', '42', '43', '44', '45', '46',
        '50', '51', '52', '53', '54',
        '61', '62', '63', '64', '65'
    ]

    if 1:
        # debug
        # provinces = ["110101", "520102"]
        # provinces = ["320172"]
        generate_datafile(provinces, syn_ftp=True)

    # test
    if 0:
        gov_code = 110101000000
        detail_rows = get_events_details_from_db(gov_code)
        df_gov_events_details = pd.DataFrame(detail_rows)
        df_gov_events_details["event_start_time"] = df_gov_events_details["event_start_time"].map(str).str.split('.', n=-1, expand=True).iloc[:, 0]

        month_start = '2018-11-01 00:00:00'
        month_end = '2018-12-01 00:00:00'

        # print(df_gov_events_details)
        # print(df_gov_events_details["department"][1][1])
        print(df_gov_events_details[(df_gov_events_details["event_start_time"] >= month_start) & (df_gov_events_details["event_start_time"] < month_end)])

        df_gov_month = df_gov_events_details[(df_gov_events_details["event_start_time"] >= month_start) & (df_gov_events_details["event_start_time"] < month_end)]

        df_gov_month = df_gov_month.sort_values(by=["thd_grade", "event_start_time"], ascending=(["C级", "B级", "A级", "Z级"], False))

        print(df_gov_month)

    if 0:
        upload_web_details()
