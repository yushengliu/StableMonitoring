#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/9/27 17:44
@Author  : Liu Yusheng
@File    : fetch_trifle_data.py
@Description: 合并小事件，计算政府执行力指数（综合管控力度）；event_time_start的问题已经解决 —— 2018/9/30
"""

import sys, io
# sys.path.append('D:\\liuyusheng\\gov_maintain_stability')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import re
import logging

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from copy import deepcopy

from db_interface import database
from utils import es_utils
from utils.path_manager import LOCAL_LOG_FILE_DIR, RELATED_FILES, UTILS_PATH


logger = logging.getLogger('mylogger')
formatter = logging.Formatter('%(asctime)s %(levelname)-8s:%(message)s')
file_handler = logging.FileHandler(LOCAL_LOG_FILE_DIR + 'es_compare_with_hst.log')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

from os.path import abspath, dirname

gaode_geo_path = RELATED_FILES + "df_2861_gaode_geo_new_20191129.csv"
env_sense_path = RELATED_FILES + 'env_sensitive_word_userdict.txt'
stb_sense_path = RELATED_FILES + 'stb_sensitive_word_userdict.txt'

df_2861_county = pd.read_csv(gaode_geo_path, index_col='gov_code', encoding='utf-8')   # 取所有区域

# 小事件信息表
trifle_server = database.create_user_defined_database_server(host="192.168.0.133",port="6500",user="etherpad", pwd="123456")
trifle_db = "text-mining"
trifle_table = "xmd_trifles_under_gov_control"

# 追踪库
trace_server = database.create_user_defined_database_server(host="39.108.127.36", port="5432", user="postgres", pwd="jiatao")
trace_db = "public_sentiment"
trace_seed_table = "public_sentiment_trace_seed_table"
trace_info_table = "public_sentiment_trace_info_table"
trace_detail_table = "public_sentiment_trace_detail_table"


# 关键词系列

#区县官职名
gov_guanzhi_keyword_new = \
    u"县委副书记|区委副书记|市委副书记|党委副书记|纪委副书记|政法委副书记|政协副主席|人大副主任|副县长|副区长|副市长|副旗长|" \
    u"县委书记|区委书记|市委书记|党委书记|纪委书记|政法委书记|政协主席|人大主任|县长|区长|市长|旗长|" \
    u"副书记|副局长|副处长|副科长|副部长|副主任|大队长|中队长|副镇长|副乡长|副村长|副秘书长|" \
    u"书记|局长|处长|科长|部长|主任|队长|镇长|乡长|村长|秘书长|领导|干部|当官的|官员"

#区县部门名
gov_department_keyword_new = \
    u"县委|区委|市委|旗委|党委|纪委|政法委|县政协|区政协|市政协|旗政协|县人大|区人大|市人大|旗人大|县政府|区政府|市政府|旗政府" \
    u"人武部|法院|检察院|管理处|信息办|保密局|防邪办|组织部|老干局|党建办|编办|宣传部|国土资源局|" \
    u"文明办|社科联|新闻宣传中心|文联|政研室|统战部|民宗局|机关工委|工信局|群工局|国资办|法制办|应急指挥中心|" \
    u"应急办|公共资源交易中心|安监局|人社局|审计局|公安局|公安分局|派出所|司法局|民政局|档案局|督查办|督查室|扶贫开发局|" \
    u"扶贫办|扶贫局|发改局|统计局|交通运输局|交通局|商旅局|工商局|旅游局|商业局|商务局|招商局|供销联社|财政局|" \
    u"住房公积金中心|住房公积金管理处|农办|农业局|林业局|水务局|管理局|管理处|住建局|国土局|国土分局|环保局|防洪办|" \
    u"教体局|教育局|体育局|卫计局|卫生局|防震减灾局|文广新局|文化局|新闻局|文广局|投促局|投资促进局|国保大队|邮政局|" \
    u"气象局|食药监局|质监局|烟草专卖局|烟草局|供电局|国税局|地税局|税务局|惠民帮扶中心|帮扶中心|金融办|城管局|规划局|" \
    u"规划分局|消防大队|消防局|消防队|党校|总工会|妇联|台办|工商联|残联|红十字会|机关事务局|机关事务管理局|网信办|土地局|" \
    u"信访办|信访局|建设局|工商税务|园林局|管理委员会|接待办|人防|人民防空|农牧局|渔业局|保障局|药监局|交警队|刑警队|" \
    u"特警队|防暴警察|维稳办|安全局|档案局|物价局|空管局|执法局|管委会|测绘局|勘测局|勘探局|" \
    u"有关部门|相关部门|政府部门|医院|学校|城管|督察组|中学|小学|幼儿园"


# 污染敏感词
def get_env_sensitive_words():
    with open(env_sense_path, 'r', encoding='utf-8') as fp_out:
        origin_words = fp_out.readlines()
    env_sense_words = []
    for i in origin_words:
        env_sense_words.append(i.strip().split(' ')[0])
    # print(env_sense_words)
    return env_sense_words


# 稳定敏感词
def get_stb_sensitive_words():
    with open(stb_sense_path, 'r', encoding='utf-8') as fp_out:
        origin_words = fp_out.readlines()
    stb_sense_words = []
    for i in origin_words:
        stb_sense_words.append(i.strip().split(' ')[0])
    # print(env_sense_words)
    return stb_sense_words


env_sens_list = get_env_sensitive_words()
env_sens_list.append('环保')

stb_sens_list = get_stb_sensitive_words()

# sfd = stb
events_type_dict = {"edu":"教育局|学校|幼儿园|小学|中学|留守儿童|感恩费|霸凌", "med":"医院|救治", "env":"|".join(env_sens_list), "std":"政府|公安局|有关部门|纪委|法院|检察院|政协|"+"|".join(stb_sens_list)}


# 停用词 —— 暂未使用，持续积累 —— 2018/10/8
stopWords = ["交通管制", "气象台", "车流量", "收费站", "从我做起", "生日快乐"]


# 得到一个区县，指定时间段发生的events信息
def get_local_events_info_from_ES(gov_code,time_start,time_end):

    time_start = re.split(' ', str(time_start))[0]
    time_end = re.split(' ', str(time_end))[0]
    gov_code_str = str(gov_code)[0:6]

    query_count = {
        "query":
            {
                "bool":
                    {"filter":
                        [
                            {
                                "range":{"sensitive_word":{"gt":"''"}}
                            }
                        ]
                    }
            },
    }
    #从ES获取区县微博信息
    details_info, _ = es_utils.es_get_events_info(es_utils.es,query_count,gov_code=gov_code_str, max_size=10000000, interval=[time_start,time_end])
    event_info_list = []
    for details in details_info:
        es_type = details['_type']
        es_index = details['_index']
        es_score = details['_score']
        es_source = details['_source']

        event_info = {}
        event_info['event_id'] = details['_id']
        event_info['gov_id'] = es_source['gov_id']
        event_info['gov_code'] = es_source['gov_code']
        event_info['gov_name'] = es_source['location']
        event_info['do_time'] = datetime.strptime(es_source['event_time_start'], '%Y-%m-%d %H:%M:%S')
        # 加上 event_time_start
        event_info['event_time_start'] = datetime.strptime(es_source['event_time_start'], '%Y-%m-%d %H:%M:%S')
        event_title = es_source['event_title']
        event_title_list = re.split(' ', event_title)
        # if len(event_title_list) > 10:
        #     event_title_list = event_title_list[0:10]
        event_info['event_title'] = event_title_list
        event_info['url_list'] = es_source['data_id_list']
        event_info['first_content'] = es_source['first_content']
        event_info['event_count_read'] = int(es_source['event_count_read'])
        event_info['event_count_comment'] = int(es_source['event_count_comment'])
        event_info['event_count_share'] = int(es_source['event_count_share'])
        event_info['event_weibo_num'] = int(es_source['event_weibo_num'])
        event_info['event_value'] = int(es_source['event_value'])
        # ES中提供的敏感信息
        if es_source['gov_post'] == "":
            event_info['gov_post'] = []
        else:
            event_info['gov_post'] = re.split(' ', es_source['gov_post'])
        if es_source['department'] == "":
            event_info['department'] = []
        else:
            event_info['department'] = re.split(' ', es_source['department'])
        if es_source['sensitive_word'] == "":
            event_info['sensitive_word'] = []
        else:
            event_info['sensitive_word'] = re.split(' ', es_source['sensitive_word'])
        negative_flag = 0
        if len(event_info['gov_post']) > 0:
            negative_flag = negative_flag + 10
        if len(event_info['department']) > 0:
            negative_flag = negative_flag + 100
        if len(event_info['sensitive_word']) > 0:
            negative_flag = negative_flag + 1000
        event_info['content_match_flag'] = negative_flag
        event_info_list.append(event_info)

    return event_info_list


# 合并事件  —— 将ES的事件取出来就合并一遍
def combin_local_events_info(gov_code,time_start,time_end,debug=False):
    new_event_info_list = []
    event_info_list = get_local_events_info_from_ES(gov_code, time_start, time_end)
    for event_info in event_info_list:
        new_event_info_list_len = len(new_event_info_list)
        if new_event_info_list_len == 0:
            new_event_info_list.append(event_info)
        else:
            event_time_start = event_info['do_time']
            event_title_list = event_info['event_title']
            new_event_flag = 1
            for i in range(0,new_event_info_list_len):
                new_event_time_start = new_event_info_list[i]['do_time']
                new_event_title_list = new_event_info_list[i]['event_title']
                new_event_data_id_list = new_event_info_list[i]['url_list']
                # delta_days = (datetime(new_event_time_start.year, new_event_time_start.month, new_event_time_start.day)-
                #               datetime(event_time_start.year, event_time_start.month, event_time_start.day)).days
                # 从es输出时按时间先后，所以计算时间差时，顺序换一下
                delta_days = (datetime(event_time_start.year, event_time_start.month, event_time_start.day) - datetime(new_event_time_start.year, new_event_time_start.month, new_event_time_start.day)).days
                if delta_days < 3:
                    #交集
                    intersection = list(set(new_event_title_list).intersection(set(event_title_list)))
                    combin_cnt = 0
                    if len(intersection) >= 3:
                        new_event_flag = 0
                        # new_event_info_list[i]['event_time_start'] = event_time_start
                        # new_event_info_list[i]['']
                        # new_event_info_list[i]['event_id'] = event_info['event_id']
                        # # 把event_time_start标为和同一件事儿一样
                        # new_event_info_list[i]['event_time_start'] = event_info["event_time_start"]
                        new_event_info_list[i]['event_title'] += event_info['event_title']
                        new_event_info_list[i]['event_title'] = list(set(new_event_info_list[i]['event_title']))
                        new_event_info_list[i]['url_list'] += event_info['url_list']
                        new_event_info_list[i]['url_list'] = list(set(new_event_info_list[i]['url_list']))
                        new_event_info_list[i]['event_count_read'] += event_info['event_count_read']
                        new_event_info_list[i]['event_count_comment'] += event_info['event_count_comment']
                        new_event_info_list[i]['event_count_share'] += event_info['event_count_share']
                        new_event_info_list[i]['event_weibo_num'] += event_info['event_weibo_num']
                        new_event_info_list[i]['event_value'] += event_info['event_value']
                        new_event_info_list[i]['gov_post'] += event_info['gov_post']
                        new_event_info_list[i]['gov_post'] = list(set(new_event_info_list[i]['gov_post']))
                        new_event_info_list[i]['department'] += event_info['department']
                        new_event_info_list[i]['department'] = list(set(new_event_info_list[i]['department']))
                        new_event_info_list[i]['sensitive_word'] += event_info['sensitive_word']
                        new_event_info_list[i]['sensitive_word'] = list(set(new_event_info_list[i]['sensitive_word']))
                        negative_flag = 0
                        if len(new_event_info_list[i]['gov_post']) > 0:
                            negative_flag = negative_flag + 10
                        if len(new_event_info_list[i]['department']) > 0:
                            negative_flag = negative_flag + 100
                        if len(new_event_info_list[i]['sensitive_word']) > 0:
                            negative_flag = negative_flag + 1000
                            new_event_info_list[i]['content_match_flag'] = negative_flag
                        combin_cnt += 1
                        if combin_cnt >= 2:
                            if debug == True:
                                print("new_event_combin Err!combin_cnt=%d"%combin_cnt)
                        else:
                            if debug == True:
                                print("new_event_combin sucess!")
                                print("new_event_title_list:%s,time_start:%s,value:%d" % (new_event_title_list,new_event_time_start,new_event_info_list[i]['event_value']))
                                print("url:%s"%new_event_data_id_list)
                                print("event_title_list:%s,time_start:%s,value:%d" % (event_title_list, event_time_start,event_info['event_value']))
                                print("url:%s" % event_info['url_list'])

            if new_event_flag == 1:
                new_event_info_list.append(event_info)

    return new_event_info_list


# 读数据库的表——过往七天的事件信息(2018-11-06 不限定es, trace_db也可以，保证过往七天内的事件即可)  # and source = 'es'
def get_es_past_trifles_from_db(gov_id, before_start, before_end):
    trifle_conn = database.ConnDB(trifle_server, trifle_db, trifle_table)
    trifle_conn.switch_to_arithmetic_write_mode()
    read_sql = "SELECT max(event_id) as event_id, max(event_type) as event_type, max(event_title) as event_title, max(first_content) as first_content, min(do_time) as do_time, min(event_value) as event_value, min(event_time_start) as event_time_start from %s where gov_id = %d and do_time >= '%s' and do_time < '%s' group by event_id"%(trifle_table, gov_id, before_start, before_end)
    res = trifle_conn.read(read_sql)
    if not res.code:
        print("Failed to get past trifles info: ", res.result)
    rows = res.data
    trifle_conn.disconnect()
    return rows


# 根据部门、敏感词、官职粗略判断事件类型，入参——事件信息dict /event_info —— 从es拿出来的
def judge_event_type(event_info):
    # content_match_flag = event_info["content_match_flag"]
    # department有值 —— 暂不判断呢
    # if content_match_flag >= 1100:
    all_words = event_info["sensitive_word"] + event_info["department"] + event_info["gov_post"]
    all_words_str = " ".join(all_words)
    match_types = {}
    for key in events_type_dict.keys():
        if len(re.findall(events_type_dict[key], all_words_str)):
            match_types[key] = len(re.findall(events_type_dict[key], all_words_str))
    if match_types and sum(match_types.values()) != 0:
        event_info["event_type"] = sorted(match_types, key=lambda x: match_types[x])[-1]
    return event_info


# 比较当天和前七天的事件信息 —— es
def compare_current_with_before_es(current_time, gov_name, current_event_list, past_event_list, show_details=True, current_es=True):
    final_aug_event_list = []
    past_num = len(past_event_list)
    same_event_num = 0
    new_event_num = 0
    for current_event in current_event_list:
        current_title = current_event["event_title"]
        # 当前的每件事儿和过去的每件事儿（已去重）相比
        # if past_num > 0:  —— 已经在外面判断了
        for i in range(len(past_event_list)):
            past_title = eval(past_event_list[i]["event_title"])
            intersection = list(set(current_title).intersection(set(past_title)))
            # 如果判定为和过往事件是一件事儿，就将event_id和event_type与其置为一样，event_title和first_content先不作合并，保持各天自己的状态
            if len(intersection) >= 3:
                new_event_flag = 0
                current_event["origin_id"] = current_event["event_id"]
                current_event["event_type"] = past_event_list[i]["event_type"]
                current_event["event_id"] = past_event_list[i]["event_id"]
                # 把发生时间取两者最小 —— 可能会有和es不一样的情况，但没关系，有es和trace_db重复时，只取trace_db的数据
                current_event["event_time_start"] = min([current_event["event_time_start"], past_event_list[i]["event_time_start"]])
                # current_event["event_title"] = past_event["event_title"]
                if current_es:
                    current_event["source"] = "es"
                else:
                    current_event["source"] = "trace_db"
                    current_event["intersection"] = "T"
                final_aug_event_list.append(current_event)
                same_event_num += 1
                if show_details:
                    print("Have found same event in history !")
                    print("history event: event_title: %s ; do_time: %s ; event_value: %d "%(past_event_list[i]["event_title"], past_event_list[i]["do_time"], past_event_list[i]["event_value"]))
                    print("current_day event: event_title: %s ; do_time: %s ; event_value: %d ; source: %s" % (
                    current_event["event_title"], current_event["do_time"], current_event["event_value"], current_event["source"]))
                    print("history content: %s" % past_event_list[i]["first_content"])
                    print("current content: %s" % current_event["first_content"])
                break
            else:
                if i < past_num-1:
                    continue
                else:
                    # 与过往的每一件事儿都不同，所以是新事件，需要加上event_type 和 source
                    new_event_flag = 1
                    # content_match_flag =
                    if current_es:
                        current_event = judge_event_type(current_event)
                        current_event["source"] = "es"
                    else:
                        current_event["source"] = "trace_db"
                    final_aug_event_list.append(current_event)
                    new_event_num += 1
    if current_es:
        logger.info("event_occur_time: %s    gov_name: %s    events_occur_num：%d    same_events: %d    newly_events: %d    es_source: %s"%(current_time, gov_name, len(current_event_list), same_event_num, new_event_num, current_es))
    return final_aug_event_list


# 将整合、提取出的小事件数据写入数据表中
def write_trifle_info_into_db(event_info_list, debug=False):
    trifle_conn = database.ConnDB(trifle_server, trifle_db, trifle_table)
    trifle_conn.switch_to_arithmetic_write_mode()

    table_fields = ["source", "intersection", "event_type", "event_id", "event_title", "gov_id", "gov_name",
                    "first_content", "do_time", "event_value", "event_weibo_num", "event_count_read",
                    "event_count_comment", "event_count_share", "url_list", "search_cnt", "origin_id", "sensitive_word",
                    "department", "gov_post", "event_time_start"]

    for event_info in event_info_list:
        data_dict = {}
        for field in table_fields:
            if field not in event_info.keys():
                continue
            if isinstance(event_info[field], (int, float)):
                data_dict[field] = event_info[field]
            else:
                data_dict[field] = str(event_info[field]).replace("'", '"')
        insert_sql = database.create_insert_sql(trifle_table, data_dict)
        if debug:
            print(insert_sql)
        ex_res = trifle_conn.execute(insert_sql)
        if not ex_res.code:
            print("Failed to insert into trifle_db: ", ex_res.result)
        # else:
        #     print("Succeeded to insert into trifle_db.")
    trifle_conn.disconnect()
    return


# time_start 和 time_end 以 datetime 格式传入 —— 按天跑
def get_es_trifles_info_into_db(gov_code, current_day):
    gov_id = df_2861_county.loc[gov_code, 'gov_id']
    gov_name = df_2861_county.loc[gov_code, 'full_name']

    time_start = datetime.strptime(str(current_day).split(' ')[0], '%Y-%m-%d')
    time_end = datetime.strptime(str(current_day+timedelta(days=1)).split(' ')[0], '%Y-%m-%d')

    # 新增当天的 —— 当天的自己合并之后的
    event_info_list = combin_local_events_info(gov_code, time_start, time_end, True)

    # 取过往7天的
    before_start = time_start - timedelta(days=7)
    before_end = time_start
    if len(event_info_list) > 0:
        past_events_info_list = get_es_past_trifles_from_db(gov_id, before_start, before_end)
        if len(past_events_info_list) > 0:
            final_events_info_list = compare_current_with_before_es(str(time_start).split(' ')[0], gov_name, event_info_list, past_events_info_list)
        else:
            # 把source 和 events_type 加上
            final_events_info_list = []
            for current_event in event_info_list:
                current_event = judge_event_type(current_event)
                current_event["source"] = "es"
                final_events_info_list.append(current_event)
        write_trifle_info_into_db(final_events_info_list)

        print("\n=========Succeeded in writing es trifles info, gov_name=%s, current_day=%s==========\n"%(gov_name, current_day))

    return


# ------------------------------------------------------------------------------------------------------------------------

# 先读seed表，得到停止追踪的事件最后一次search_cnt信息 —— 判断在最后一次追踪在，从当天往前推第4天
def get_trace_seed_list(time_start, time_end, debug=False):
    trace_conn = database.ConnDB(trace_server, trace_db, trace_seed_table)
    trace_conn.switch_to_arithmetic_write_mode()

    sqlstr = "SELECT events_head_id, gov_id, gov_name, key_word_str, start_date, search_cnt, sync_time, events_type from %s where trace_state=4 and spider_sync_time >=  '%s' and spider_sync_time < '%s' order by start_date desc"%(trace_seed_table, time_start, time_end)

    if debug:
        sqlstr += " limit 10;"
    ret = trace_conn.read(sqlstr)
    if not ret.code:
        print("Failed to get trace seed info: ", ret.result)
    rows = ret.data
    trace_conn.disconnect()
    return rows


# 得到某事件的各周期的每条微博detail情况
def get_event_details_info(events_head_id=0):
    trace_conn = database.ConnDB(trace_server, trace_db, trace_detail_table)
    trace_conn.switch_to_arithmetic_write_mode()

    sqlstr = "SELECT gov_id, pub_time, do_time, url, content, count_read, count_comment, count_share, events_head_id, search_cnt, more_info from %s where more_info != '{\"access\": false}'"%(trace_detail_table)

    if events_head_id != 0:
        sqlstr += " and events_head_id = '%s'"%events_head_id

    ret = trace_conn.read(sqlstr)
    if not ret.code:
        print('Failed to get trace detail: %s, event_id: %s'%(ret.result, events_head_id))
    rows = ret.data
    trace_conn.disconnect()
    return rows


# 获取敏感词库信息
def get_sensitive_word_list():
    t_file_in = RELATED_FILES + 'sensitive_word_userdict.txt'
    with open(t_file_in,'rt', encoding='utf-8') as f_in:
        pattern1 = re.compile(r' ')
        sensitive_word_list = []
        while True:
            s = f_in.readline()
            t_strlen = len(s)
            if t_strlen > 0:
                match1 = pattern1.search(s)
                if match1:
                    t_str_list = pattern1.split(s)
                    if t_str_list[0] != '':
                        sensitive_word_list.append(t_str_list[0])
            else:
                break
    return sensitive_word_list


def match_warning_keywords_frontend(content, type="sensitive", freq=True):
    match_results = []
    if type == "sensitive":
        sensitive_words = get_sensitive_word_list()
        key_words_str = '|'.join(sensitive_words)
    elif type == "department":
        key_words_str = gov_department_keyword_new
    elif type == "guanzhi":
        key_words_str = gov_guanzhi_keyword_new
    else:
        print("没有当前类别对应的关键词，请重新输入类别:sensitive/department/guanzhi")
        return False
    # print(key_words_str)
    words_res = re.findall(key_words_str, content)
    if len(words_res) > 0:
        words_remove = set(words_res)
        total = len(words_res)
        for word in words_remove:
            match_dict = {}
            match_dict["word"] = word
            if freq:
                count = words_res.count(word)
                match_dict["count"] = count
                match_dict["freq"] = round(count/total, 4)
                match_dict["type"] = type
            match_results.append(match_dict)
    return match_results


# trace库 —— 未达到预警门限的事件// 取的全部
def get_trace_trifles_info_into_db(time_start, time_end, debug=True):
    # 从seed表拿数据 —— 以事件为单位的各指数表
    past_seed_list = get_trace_seed_list(time_start, time_end, debug)
    df_seeds = pd.DataFrame(past_seed_list)

    # 清洗、去重
    df_seeds.drop_duplicates(subset=["events_head_id"], keep="first", inplace=True)
    df_seeds.reset_index(drop=True, inplace=True)

    if not df_seeds.shape[0]:
        return

    df_seeds["key_word_str"] = df_seeds["key_word_str"].apply(lambda x:x.split("and")[1:])
    df_seeds["events_type"] = df_seeds["events_type"].apply(lambda x:"env" if x == 2000 else "stb")

    # 取trace_info —— 先不取trace_info， 之后件事件为单位的指数表时需要取trace_info

    # 取detail表
    # df_seeds.rename()
    events_head_ids = list(df_seeds["events_head_id"])
    # 直接取全量detail太慢了 —— 按事件取
    url_concat = lambda x:",".join(x)
    content_fx = lambda x:list(x)[[len(i) for i in x].index(max([len(i) for i in x]))]

    for events_head_id in events_head_ids:

        details_list = get_event_details_info(events_head_id)
        df_event_detail = pd.DataFrame(details_list)
        df_event_detail["weibo_num"] = 1
        if df_event_detail.shape[0] == 0:
            continue
        # df_event_detail["weibo_num"]
        df_event_detail.sort_values(by=['search_cnt'], inplace=True)
        df_event_detail.reset_index(drop=True, inplace=True)
        df_itg_weibo = df_event_detail.groupby(by='search_cnt', as_index=False).agg({'gov_id':max, 'pub_time':min, 'do_time':max, "url":url_concat, "content":content_fx, "count_read":sum, "count_comment":sum, "count_share":sum, "weibo_num":sum, "events_head_id":max})
        df_itg_weibo["event_value"] = 6*df_itg_weibo["weibo_num"] + df_itg_weibo["count_read"] + 3*df_itg_weibo["count_comment"] + 2*df_itg_weibo["count_share"]
        df_itg_weibo["url_list"] = df_itg_weibo["url"].apply(lambda x:x.split(","))

        # 补足其他项
        df_itg_weibo["source"] = "trace_db"
        # event_type为stb的， 之后再过一遍 judge_event_type
        df_itg_weibo["event_type"] = df_seeds[df_seeds.events_head_id == events_head_id]["events_type"].values[0]
        df_itg_weibo["event_title"] = [df_seeds[df_seeds.events_head_id == events_head_id]["key_word_str"].values[0]]*df_itg_weibo.shape[0]
        df_itg_weibo["gov_name"] = df_2861_county[df_2861_county.gov_id == df_itg_weibo[df_itg_weibo.events_head_id == events_head_id]["gov_id"].values[0]]["full_name"].values[0]
        df_itg_weibo.rename(columns={"events_head_id":"event_id", "content":"first_content", "weibo_num":"event_weibo_num", "count_read":"event_count_read", "count_comment":"event_count_comment","count_share":"event_count_share"}, inplace=True)

        # 过一批关键词
        key_words_type = ["sensitive", "guanzhi", "department"]
        df_itg_weibo["sensitive_word"] = df_itg_weibo["first_content"].apply(lambda x:[i["word"] for i in match_warning_keywords_frontend(x, type="sensitive", freq=False)])
        df_itg_weibo["department"] = df_itg_weibo["first_content"].apply(lambda x: [i["word"] for i in match_warning_keywords_frontend(x, type="department", freq=False)])
        df_itg_weibo["gov_post"] = df_itg_weibo["first_content"].apply(lambda x: [i["word"] for i in match_warning_keywords_frontend(x, type="guanzhi", freq=False)])

        # 加上event_time_start
        df_itg_weibo["event_time_start"] = df_itg_weibo["do_time"].min()
        df_itg_weibo.sort_values(by=['do_time'], ascending=True, inplace=True)

        # 转为dict的list
        trace_events_list = []
        for index, row in df_itg_weibo.iterrows():
            data_dict = {}
            for col in list(df_itg_weibo):
                data_dict[col] = row[col]
            if data_dict["event_type"] == "stb":
                data_dict = judge_event_type(data_dict)
            trace_events_list.append(data_dict)

        # 与过去七天es里的事件合并
        gov_id = df_itg_weibo["gov_id"][0]
        # 追踪中的事件可能持续的最长时间 —— 取【可能】 最初的 和 最后的
        start_date = df_itg_weibo["pub_time"].min()
        end_date = df_itg_weibo["do_time"].max()

        # 取es里的持续时间往前推七天
        # 毫秒的情况，'2018-08-15 17:52:50.542847' —— datetime.strptime(sync_time, '%Y-%m-%d %H:%M:%S.%f')
        before_start = datetime.strptime(str(start_date).split('.')[0], '%Y-%m-%d %H:%M:%S') - timedelta(days=7)
        before_end = datetime.strptime(str(end_date).split('.')[0], '%Y-%m-%d %H:%M:%S')
        past_es_events_info_list = get_es_past_trifles_from_db(gov_id, before_start, before_end)

        gov_name = df_itg_weibo["gov_name"][0]
        trace_event_sample_list = [trace_events_list[0]]
        final_trace_list = []
        if len(past_es_events_info_list) > 0:
            final_trace_event_sample = compare_current_with_before_es(start_date, gov_name, trace_event_sample_list, past_es_events_info_list, current_es=False)[0]

            # 所有的event_trace_info保持和final_sample中改动的字段一致

            for trace_info in trace_events_list:
                for col in ["origin_id", "event_type", "event_id", "intersection", "event_time_start"]:
                    if col in final_trace_event_sample.keys():
                        trace_info[col] = final_trace_event_sample[col]
                final_trace_list.append(trace_info)
        else:
            final_trace_list = trace_events_list

        if debug:
            print(final_trace_list)

        # 写入数据库
        write_trifle_info_into_db(final_trace_list, debug=debug)
        print("\n=================================================")
        print("Write %s into db done: %s\n"%(events_head_id,final_trace_list[0]["event_title"]))
    return


# 添加一列 —— event_time_start —— 2018/9/29 ，之后不用再调用，已经全部加到之前的代码中，写入时就有event_time_start字段。
def add_table_start_time(last_update):
    trifle_conn = database.ConnDB(trifle_server, trifle_db, trifle_table)
    trifle_conn.switch_to_arithmetic_write_mode()

    sqlstr = "SELECT distinct(event_id) from %s where last_update > '%s';"%(trifle_table, last_update)

    ret = trifle_conn.read(sqlstr)
    if not ret.code:
        print("Failed to fetch trifle all data: %s"%ret.result)

    rows = ret.data

    for row in rows:
        select_sql = "SELECT min(do_time) as event_time_start from %s where event_id = '%s';"%(trifle_table, row["event_id"])
        search_ret = trifle_conn.read(select_sql)
        if not search_ret.code:
            print("Failed to fetch event_time_start: %s"%search_ret.result)
        time_start = search_ret.data[0]["event_time_start"]

        update_sql = "UPDATE %s SET event_time_start = '%s' where event_id = '%s'"%(trifle_table, time_start, row['event_id'])
        ex_ret = trifle_conn.execute(update_sql)
        if not ex_ret.code:
            print("Failed to update %s start_time: %s" % (row['event_id'], ex_ret.result))

        print("================\nSucceed in marking event_time_start from history. event_id = %s , event_time_start = %s\n======================="%(row['event_id'], time_start))
    trifle_conn.disconnect()
    return


# 同一事件 保留追踪search_cnt最大的一组
def delete_trace_differ_search_cnt():
    trifle_conn = database.ConnDB(trifle_server, trifle_db, trifle_table)
    trifle_conn.switch_to_arithmetic_write_mode()

    sql_path = UTILS_PATH + "deal_trifle_table.sql"
    with open(sql_path, "r", encoding="utf-8") as f:
        delete_sql = f.read()
        delete_sql = delete_sql.replace('\n', ' ')
    # print(delete_sql)
    # trifle_conn.execute(delete_sql)
    ex_ret = trifle_conn.execute(delete_sql)
    if not ex_ret.code:
        print("Failed to delete_trace_differ_search_cnt: %s" % (ex_ret.result))
    print("\n============Succeeded in delete_trace_differ_search_cnt, change lines: %d===================\n"%ex_ret.data)
    trifle_conn.disconnect()
    return


def fetch_trifle_main(start_days=[]):
    """
    @功能：总调度入口
    :param start_days:  ['2019-12-31', '2020-01-01', …… ]
    :return:
    """
    # 没有入参日期，就更新六天前的数据
    start_days = start_days if start_days else [str(datetime.now()+timedelta(days=-6)).split(' ')[0]]

    start_days = [datetime.strptime(start_day, '%Y-%m-%d') for start_day in start_days]

    for start_day in start_days:
        end_day = start_day + timedelta(days=1)

        # es中的事件
        for gov_code in df_2861_county.index:
            get_es_trifles_info_into_db(gov_code, start_day)

        # 追踪库的事件
        get_trace_trifles_info_into_db(start_day, end_day, debug=False)

    # 清洗trace事件合并后多组search_cnt的情况 —— 只保留search_cnt最大的一组
    delete_trace_differ_search_cnt()


if __name__ == "__main__":

    # 每日运行
    fetch_trifle_main()

    # 查漏补缺示例
    # fetch_trifle_main(['2019-12-31', '2020-01-01'])







