#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/12/19 14:18
@Author  : Liu Yusheng
@File    : mark_words_for_synchro.py
@Description: 给政府信息打分类标签，和各类关键词
"""

import re

RELATED_FILES = './related_files/'

# 环境大类关键词
env_r = '霾|空气不好|空气污染|浓烟|废气|焚烧秸秆|排污|尾气|刺激性气体|扬尘|粉尘|降尘|脱硫|空气糟糕|污染空气|污染大气|大气污染|水质' \
        '|排放|排污|臭水|水污染|水体|干旱|水化物|污水|采滤|供水|酸性|赤潮|持水|废水|地下水|富集|富营养化|河道污染|净水|枯水期|水处理|' \
        '水蚀|水土流失|水土保持|河长|噪音|扰民|噪音污染|噪声|养殖粪污|粪尿|养殖异味|畜禽尸体|养鸡场臭气|养鸡场异味|养殖场臭气|' \
        '养殖异味|养鸡场粪便|养殖场污染|猪舍的废水|畜禽养殖|养猪场|畜禽粪便|农村养殖|生猪养殖|鸡鸭养殖|粪尿直排|猪场污染|' \
        '养殖场整治|非法养殖|养殖业|养殖污染|养殖粪便|猪粪|猪屎|养猪场|垃圾坑|化工厂|偷排|刺鼻|造纸厂|垃圾成堆|工业污染|建筑垃圾|工业垃圾|' \
        '倾倒垃圾|臭气熏天|横流|镉|重金属|铅中毒|毒大米'


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


#区县官职名
gov_guanzhi_keyword_new = \
    u"县委副书记|区委副书记|市委副书记|党委副书记|纪委副书记|政法委副书记|政协副主席|人大副主任|副县长|副区长|副市长|副旗长|" \
    u"县委书记|区委书记|市委书记|党委书记|纪委书记|政法委书记|政协主席|人大主任|县长|区长|市长|旗长|" \
    u"副书记|副局长|副处长|副科长|副部长|副主任|大队长|中队长|副镇长|副乡长|副村长|副秘书长|" \
    u"书记|局长|处长|科长|部长|主任|队长|镇长|乡长|村长|秘书长|领导|干部|当官的|官员"


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


# 污染敏感词
def get_env_sensitive_words():
    with open(RELATED_FILES+'env_sensitive_word_userdict.txt', 'r', encoding='utf-8') as fp_out:
        origin_words = fp_out.readlines()
    env_sense_words = []
    for i in origin_words:
        env_sense_words.append(i.strip().split(' ')[0])
    # print(env_sense_words)
    return env_sense_words


# 稳定敏感词
def get_stb_sensitive_words():
    with open(RELATED_FILES+'stb_sensitive_word_userdict.txt', 'r', encoding='utf-8') as fp_out:
        origin_words = fp_out.readlines()
    stb_sense_words = []
    for i in origin_words:
        stb_sense_words.append(i.strip().split(' ')[0])
    # print(env_sense_words)
    return stb_sense_words


# 根据部门、敏感词、官职粗略判断事件类型，入参——事件信息dict /event_info —— 从es拿出来的
def judge_event_type(event_info, content=True):
    # content_match_flag = event_info["content_match_flag"]
    # department有值 —— 暂不判断呢
    # if content_match_flag >= 1100:
    # events_type_dict = {"edu": "教育局|学校|幼儿园|小学|中学|留守儿童|感恩费|霸凌", "med": "医院|救治",
    #                     "env": "|".join(get_env_sensitive_words()),
    #                     "std": "政府|公安局|有关部门|纪委|法院|检察院|政协|" + "|".join(get_stb_sensitive_words())}

    # 学校|   医院|
    events_type_dict = {"edu": "教育局|幼儿园|小学|中学|留守儿童|感恩费|霸凌|上学难|教委|家长", "med": "救治|庸医|看病难|医疗|医保|医患|医药费|就医|患者|医院",
                        "env": "|".join(get_env_sensitive_words())+"|噪音|扰民|污染|脏乱差|垃圾|施工|臭味熏天|污水|排污|"+env_r,
                        "std": "政府|公安局|有关部门|纪委|法院|检察院|政协|偷税|漏税|" + "|".join(get_stb_sensitive_words())}

    if content:
        check_str = event_info["event_content"]
    else:
        all_words = event_info["sensitive_word"] + event_info["department"] + event_info["gov_post"]
        all_words_str = " ".join(all_words)
        check_str = all_words_str
    match_types = {}
    for key in events_type_dict.keys():
        if len(re.findall(events_type_dict[key], check_str)):
            match_types[key] = len(re.findall(events_type_dict[key], check_str))
    if match_types and sum(match_types.values()) != 0:
        event_info["event_type"] = sorted(match_types, key=lambda x: match_types[x])[-1]
    else:
        event_info["event_type"] = "stb"
    return event_info


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

    # match_str = str([i["word"] for i in match_results]).replace("'",'"')
    match_str = " ".join([i["word"] for i in match_results])
    # return match_results
    return match_str