#!/usr/bin/env
# -*- coding:utf-8 -*-
#
import re
import os
import json
import copy
import traceback
import random
import time
from urllib import request, parse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date

#
from elasticsearch import Elasticsearch

from db_interface import database
from utils import path_manager as pm
from utils import ftpManager
from utils import parameters as para


# 从全文检索查询相关数据
query_complex = {
        "query": {
            "bool": {"filter": []}
        }
    }

es_host_cluster = [{"host": "192.168.0.135", "port": 9200},
                   {"host": "192.168.0.133", "port": 9200},
                   {"host": "192.168.0.118", "port": 9200},
                   {"host": "192.168.0.38", "port": 9200},
                   {"host": "192.168.0.88", "port": 9200}]

# es = Elasticsearch(["192.168.0.135", "192.168.0.118", "192.168.0.38", "192.168.0.133", "192.168.0.88"], timeout=120, read_timeout=60, max_retries=10, retry_on_timeout=True)
es = Elasticsearch(es_host_cluster, maxsize=25, timeout=120, read_timeout=60, max_retries=10, retry_on_timeout=True)


# es 按区县和时间提取微博数据
def es_get_weibo_info_by_gov_id(query_complex=query_complex, es=es, gov_id=0, max_size=10000,interval=[], index="zk_social"):
    # print(query_complex)
    query = copy.deepcopy(query_complex)
    if (gov_id != 0):
        filter_gov = {"terms": {"gov_id": [gov_id]}}
        query["query"]["bool"]["filter"].append(filter_gov)

    if interval:
        if index == "zk_social" or index == "zk_leader_mailbox_msg":
            pub_col = "pub_time"
        elif index == "zk_event":
            pub_col = "event_time_start"
        range_interval = {
            "range": {
                pub_col: {"lt": datetime.strptime(interval[1],'%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                             "gte": datetime.strptime(interval[0],'%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')}}}
        query["query"]["bool"]["filter"].append(range_interval)
    # print(query)
    res = es.search(index=index, body=query, size=max_size)
    print("gov_id %s, Took %s milliseconds. Got %d Hits." % (str(gov_id), res['took'], res['hits']['total']))
    total_hits = res['hits']['total']
    # print(total_hits)
    if total_hits == 0:
        return [], 0
    else:
        return res['hits']['hits'], res['hits']['total']


# es 按区县和时间，一天一天提取数据
def es_get_data_day_by_day(query_complex=query_complex, es=es, gov_id=0, max_size=10000, interval=[], index="zk_social"):
    query = copy.deepcopy(query_complex)
    if gov_id != 0:
        filter_gov = {"terms":{"gov_id":[gov_id]}}
        query["query"]["bool"]["filter"].append(filter_gov)

    if interval:
        res_datas = []
        total_hits = 0

        if index == "zk_social" or index == "zk_leader_mailbox_msg":
            pub_col = "pub_time"
        elif index == "zk_event":
            pub_col = "event_time_start"

        query["query"]["bool"]["filter"].append({"range":{}})
        start_datetime = datetime.strptime(interval[0], '%Y-%m-%d')
        end_datetime = datetime.strptime(interval[1], '%Y-%m-%d')

        days = (end_datetime-start_datetime).days

        # 一天一天取
        for d in range(0, days):
            every_start_date = str((start_datetime + timedelta(days=d)).date())
            every_end_date = str((start_datetime+timedelta(days=d+1)).date())

            range_interval = {
                "range": {
                    pub_col: {"lt": datetime.strptime(every_end_date, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                              "gte": datetime.strptime(every_start_date, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')}}}
            # query["query"]["bool"]["filter"].append(range_interval)

            query["query"]["bool"]["filter"][-1] = range_interval
            # print(query)
            day_res = es.search(index=index, body=query, size=max_size)

            print("gov_id %s, %s: Took %s milliseconds. Got %d Hits." % (str(gov_id), every_start_date, day_res['took'], day_res['hits']['total']))

            total_hits += day_res['hits']['total']
            res_datas.extend(day_res["hits"]["hits"])

    else:
        res = es.search(index=index, body=query, size=max_size)
        print("gov_id %s, Took %s milliseconds. Got %d Hits." % (str(gov_id), res['took'], res['hits']['total']))
        total_hits = res['hits']['total']
        res_datas = res["hits"]["hits"] if total_hits != 0 else []

    return res_datas, total_hits


# 从es计算某时段所有区县微博的统计信息 —— 按照gov_id聚合
def es_weibo_gov_agg_counts(start_date, end_date, logger=None):

    # 抹掉时分秒信息
    start_time = str(datetime(int(str(start_date).split(' ')[0].split('-')[0]), int(str(start_date).split(' ')[0].split('-')[1]),int(str(start_date).split(' ')[0].split('-')[2])))
    end_time = str(datetime(int(str(end_date).split(' ')[0].split('-')[0]), int(str(end_date).split(' ')[0].split('-')[1]),int(str(end_date).split(' ')[0].split('-')[2])))

    es_query = {
        "size": 0,
        "aggregations": {
            "gov_id": {
                "terms": {"field": "gov_id", "size": 9999},
                "aggregations": {
                    "sum_count_read": {"sum": {"field": "count_read"}},
                    "sum_count_share": {"sum": {"field": "count_share"}},
                    "sum_count_comment": {"sum": {"field": "count_comment"}},
                    # "sum_count_weibo": {"value_count":{"field":"gov_id"}}  —— 直接用输出的doc_count就可以
                }
            }
        },
        "query": {
            "bool": {
                "filter": [
                    {"range":{"pub_time": {"lt": end_time, "gte": start_time}}}
                ]
            }
        }
    }

    res = es.search(index="zk_social", body=es_query, size=100000)
    if logger:
        logger.info("[%s ~ %s][zk_social] Took %s milliseconds. Got %d docs. Cover counties num: %d."%(start_date, end_date, res['took'], res["hits"]["total"], len(res["aggregations"]["gov_id"]["buckets"])))
    else:
        print("[%s ~ %s] Took %s milliseconds. Got %d docs. Final agg num: %d."%(start_date, end_date, res['took'], res["hits"]["total"], len(res["aggregations"]["gov_id"]["buckets"])), flush=True)

    res_datas = res["aggregations"]["gov_id"]["buckets"]

    df_datas_org = pd.DataFrame(res_datas)
    df_datas_ext = df_datas_org.rename({"doc_count":"sum_count_weibo", "key":"gov_id"}, axis=1)

    cols = ["comment", "read", "share"]
    for col in cols:
        df_datas_ext["sum_count_"+col] = df_datas_ext.apply(lambda x:x["sum_count_"+col]["value"], axis=1)

    df_datas_ext = df_datas_ext.sort_values(by="gov_id", ascending=True).reset_index(drop=True)
    # print(df_datas_ext)

    return df_datas_ext


# =========================================================================
# 获取敏感词库信息
def get_sensitive_word_list():
    t_file_in = pm.RELATED_FILES + 'sensitive_word_userdict.txt'
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
    with open(pm.RELATED_FILES+'env_sensitive_word_userdict.txt', 'r', encoding='utf-8') as fp_out:
        origin_words = fp_out.readlines()
    env_sense_words = []
    for i in origin_words:
        env_sense_words.append(i.strip().split(' ')[0])
    # print(env_sense_words)
    return env_sense_words


# 稳定敏感词
def get_stb_sensitive_words():
    with open(pm.RELATED_FILES+'stb_sensitive_word_userdict.txt', 'r', encoding='utf-8') as fp_out:
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
                        "env": "|".join(get_env_sensitive_words())+"|噪音|扰民|污染|脏乱差|垃圾|施工|臭味熏天|污水|排污|"+para.env_r,
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
        key_words_str = para.gov_department_keyword_new
    elif type == "guanzhi":
        key_words_str = para.gov_guanzhi_keyword_new
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


# 处理json格式转换时遇到datetime的情况 / np.int64等情况
class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


# 存成json格式
def write_client_datafile_json(target_dir_path, file_name, postfix, ret_content):
    if not os.path.exists(target_dir_path):
        os.makedirs(target_dir_path)
    with open(target_dir_path + file_name + postfix, 'w') as outfile:
        json.dump(ret_content, outfile, cls=CJsonEncoder)
    return


# 将本地产生的前端网页解读数据文件上传到ftp，服务器的存储路径模板： 根路径/web/node_code/版本时间/各个县的gov_code文件夹
def upload_detail_web_datafile(node_code, version):
    serverDir = pm.FTP_CLIENT_DATA_STORAGE + 'web/' + node_code + '/'
    localDir = pm.LOCAL_STABLE_CLIENT_DATA_STORAGE + node_code + '/'
    print(serverDir, "<========", localDir)
    flag = ftpManager.upload_datafiles(serverDir, localDir, version)
    return flag

# -----------------------------2018/11/20 打包后上传 ---------------------------------


import zipfile


def zip_dir(file_path,zfile_path):
    '''
    function:压缩
    params:
        file_path:要压缩的件路径,可以是文件夹
        zfile_path:解压缩路径
    description:可以在python2执行
    '''
    filelist = []
    if os.path.isfile(file_path):
        filelist.append(file_path)
    else :
        for root, dirs, files in os.walk(file_path):
            for name in files:
                filelist.append(os.path.join(root, name))
                print('joined:',os.path.join(root, name),dirs)

    zf = zipfile.ZipFile(zfile_path, "w", zipfile.zlib.DEFLATED)
    for tar in filelist:
        arcname = tar[len(file_path):]
        print(arcname,tar)
        zf.write(tar,arcname)
    zf.close()


# 将本地产生的前端网页数据文件打包
def zip_detail_web_datafile(node_code, version):
    app_date = version
    data_files_dir = pm.LOCAL_STABLE_CLIENT_DATA_STORAGE + node_code + '/' + app_date + '/'
    zip_file_dir = pm.LOCAL_STABLE_CLIENT_DATA_STORAGE + node_code + '/zip/' + app_date + '/'
    if not os.path.exists(zip_file_dir):
        os.makedirs(zip_file_dir)
    zip_file_dir = zip_file_dir + node_code + '_apps.zip'
    zip_dir(data_files_dir, zip_file_dir)
    return 1


# 将本地产生的前端网页解读数据文件的zip压缩包上传到ftp，服务器的存储路径模板： 根路径/web/node_code/zip/版本时间/node_code_apps.zip
def upload_detail_web_datafile_zip(node_code, version):
    serverDir = pm.FTP_CLIENT_DATA_STORAGE + 'web/' + node_code + '/zip/'
    localDir = pm.LOCAL_STABLE_CLIENT_DATA_STORAGE + node_code + '/zip/'
    print(serverDir, "<========", localDir)
    flag = ftpManager.upload_datafiles(serverDir, localDir, version)
    return flag


# 色号转半透明色
def color2rgba(color):
    color = color[1:] if re.search(r'^#', color) else color
    rgb_list = []
    for i in range(0, len(color), 2):
        rgb_list.append(str(int(color[i:i+2], 16)))
    rgb_list.append('0.3')
    rgba = 'rgba('+','.join(rgb_list)+')'
    print(rgba)
    return rgba


# 随机分配颜色
def get_color(first_color, second_color, split_num):

    color_list = list()

    red_1 = int('0x%s' % first_color[:2], 16)
    green_1 = int('0x%s' % first_color[2:4], 16)
    blue_1 = int('0x%s' % first_color[4:6], 16)

    red_2 = int('0x%s' % second_color[:2], 16)
    green_2 = int('0x%s' % second_color[2:4], 16)
    blue_2 = int('0x%s' % second_color[4:6], 16)

    red_per = int((red_2 - red_1) / split_num)
    green_per = int((green_2 - green_1) / split_num)
    blue_per = int((blue_2 - blue_1) / split_num)

    for idx in range(0, split_num):
        if idx == 0:
            start_color = first_color
        else:
            start_color = str(hex((red_1 + red_per * idx) * 256 * 256 + (green_1+green_per*idx) * 256 + (blue_1+blue_per*idx)))[-6:]

        if idx == split_num - 1:
            end_color = second_color
        else:
            end_color = str(hex((red_1 + red_per * (idx+1)) * 256 * 256 + (green_1 + green_per * (idx+1)) * 256 + (blue_1 + blue_per * (idx+1))))[-6:]

        color_list.append({'from': start_color, 'to': end_color})

    return color_list


# 适合的单位
# 调整数字单位， 默认最小单位是1
def get_proper_unit_data(num):

    if num >= 100000000:
        return "%.2f亿"%(num/100000000)
    if num >= 10000000:
        return "%.2f千万"%(num/10000000)
    if num >= 10000:
        # print(type(num))
        # print(num)
        return "%.2f万"%(num/10000)
    return "%d"%num


db_server_dict = {
    'aliyun':'222.247',  # 向阿里云写数据
    'localtest': '0.133',  # 在测试服务器记录每个角的提示注释文字
    'localxmd': '0.138'  # 在本地记录每个角的得分
}


# 初始化一个数据库连接
def init_XMD_DB_CONN(write_mode=False, db_address=db_server_dict['localxmd']):
    if db_address == '222.247':
        my_server = database.get_database_server_by_nick(database.SERVER_PRODUCT)  # aliyun
        conn = database.ConnDB(my_server, 'product', 'xmd_nodes_values')
    elif db_address == '0.133':
        my_server = database.create_user_defined_database_server('192.168.0.133', '5434', 'postgres', '123456')
        conn = database.ConnDB(my_server, 'product', 'xmd_nodes_description')
    else:
        my_server = database.get_database_server_by_nick(database.SERVER_SPIDER_BASE_DATA_MANAGE)
        conn = database.ConnDB(my_server, 'xmd', 'xmd_nodes_values')
    if write_mode:
        conn.switch_to_arithmetic_write_mode()
    return conn


# 初始化产品服务器连接
def init_PRODUCT_DB_CONN(table_name, write_mode=False, formal_product=True):
    if formal_product:
        my_server = database.get_database_server_by_nick(database.SERVER_PRODUCT) # aliyun 产品正式服务器
    else:
        my_server = database.create_user_defined_database_server('192.168.0.133', '5434', 'postgres', '123456')   # 产品测试服务器

    conn = database.ConnDB(my_server, 'product', table_name)

    if write_mode:
        conn.switch_to_arithmetic_write_mode()

    return conn


# # 执行一条插入操作（传入的是conn——一个连接）
# def execute_sql(conn, sqlstr):


# 插入一条node分析的数据
def insert_node_values(conn, node_name, node_value, commit_time, value_type='base', version='', submitter=''):
    today = datetime.now()
    sqlstr = 'insert into xmd_nodes_values(node_name, values, time, type, version, submitter, submit_time) ' \
             'values (\'%s\', \'%s\', \'%s\', %s, %s, \'%s\', \'%s\')' % \
             (node_name, node_value, commit_time, value_type, version, submitter, today)
    # print(sqlstr)
    try:
        retrieve = conn.execute(sqlstr)
        print(retrieve.data)
        if not retrieve.code:
            print(retrieve.result)
    except:
        traceback.print_exc()
        print("sqlexecute_2861basedb Error")
    return retrieve


# 插入一条node解读的数据
def insert_node_description(conn, gov_id, gov_code_str, node_name, node_desciption, value_type=1):
    today = datetime.now()
    sqlstr = 'insert into xmd_nodes_description(gov_id, gov_code, node_name, content, type, submit_time) ' \
             'values (%s, \'%s\', \'%s\', \'%s\', %s, \'%s\')' % (gov_id, gov_code_str, node_name, node_desciption, value_type, today)
    try:
        retrieve = conn.execute(sqlstr)
        #print(retrieve.data)
        if not retrieve.code:
            print(retrieve.result)
    except:
        traceback.print_exc()
        print("sqlexecute_2861basedb Error")
    return retrieve


# 向数据库插入一条产生web页面数据的节点日志记录
def insert_web_updates_to_db(conn, node_code, time, flag=0):
    today = datetime.now()
    sqlstr = 'insert into xmd_nodes_web_refresh(node_code, time, submit_time, flag) ' \
             'values (\'%s\', \'%s\', \'%s\', %s)' % (node_code, time, today, flag)
    try:
        retrieve = conn.execute(sqlstr)
        # print(retrieve.data)
        if not retrieve.code:
            print(retrieve.result)
    except:
        traceback.print_exc()
        print("sqlexecute_2861basedb Error")
    return retrieve


# -----------------------------------------请求url-------------------------------------------
# 请求网页
def get_html_result(url):
    count = 10
    while count > 0:
        header_ = random.choice(para.my_headers)
        header = {'User-Agent':header_}
        try:
            html = request.Request(url, headers=header)
            if html:
                data = request.urlopen(html).read()
                html = json.loads(data)
                return html
            else:
                continue
        except Exception as e:
            print("getHtml fail, info: %s"%e)
            print("retry times %s"%count)
            # print()
            count -= 1
            time.sleep(random.uniform(0.5, 2))
            continue
    return


def read_from_es_knowledge(data_dict, url=para.DATA_URL):
    class_url = url+"operation_type=read&"
    url_values = parse.urlencode(data_dict)
    # print(url_values)
    meta_url = class_url+url_values
    result = get_html_result(meta_url)
    return result


def write_in_es_knowledge(data_dict, url=para.DATA_URL):
    class_url = url+"operation_type=write&"
    url_values = parse.urlencode(data_dict)
    # print(url_values)
    meta_url = class_url+url_values
    result = get_html_result(meta_url)
    return result


def operate_es_knowledge(data_dict, es_type="base", operation_type="write"):
    """
    @功能：操作es知识库的接口，读/写(/删除)都统一了，用es_type区分操作对象是meta还是data
    :param data_dict:
    :param es_type:
    :param operation_type:
    :return:
    """
    if operation_type == "write":
        if es_type == "meta":
            class_url = para.META_URL + "operation_type=write&"
            must_paras = ["type_code", "module", "type_name","category_big", "category_mid", "category_sub", "submitter"]
            optional_paras = ["data_unit",  "description"]

        else:
            class_url = para.DATA_URL + "operation_type=write&"
            must_paras = ["type_code", "datas", "submitter"]
            optional_paras = ["version"]
    elif operation_type == "read":
        if es_type == "meta":
            class_url = para.META_URL + "operation_type=read&"
            must_paras = []
            optional_paras = ["type_code", "module"]

        else:
            class_url = para.DATA_URL + "operation_type=read&"
            must_paras = []
            optional_paras = ["type_code", "gov_id", "version"]
    elif operation_type == "read_serial":
        class_url = para.DATA_URL + "operation_type=read_serial&"
        must_paras = ["type_code"]
        optional_paras = ["gov_id", "start_version", "end_version"]
    elif operation_type == "delete":
        class_url = para.META_URL + "operation_type=delete"
        must_paras = ["type_code"]
        optional_paras = []
    else:
        print("不支持当前入参operation_type={}，请重新输入，在一下列表中选择：\n['read', 'write', 'read_serial', 'delete']".format(operation_type), flush=True)
        return False

    data_dict_ = dict()

    if len(must_paras):
        data_dict_ = {k: data_dict[k] for k in must_paras}

    if len(optional_paras):
        for data_key in optional_paras:
            if data_key in data_dict.keys():
                data_dict_[data_key] = data_dict[data_key]

    url_values = parse.urlencode(data_dict_)
    final_url = class_url + url_values
    result = get_html_result(final_url)
    return result


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


# ============================ 民心民情国家治理现代化相关 =============================


if __name__ == "__main__":
    print('utilities.py')
    start_date = '2018-11-01'
    end_date = '2018-12-01'
    es_weibo_gov_agg_counts(start_date, end_date)
