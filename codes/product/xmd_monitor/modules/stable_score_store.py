#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/12/13 11:50
@Author  : Liu Yusheng
@File    : stable_score_store.py
@Description: 存储稳定数据的各种表
"""
import json
import time
import random
import logging

import numpy as np
import pandas as pd
from datetime import datetime
from urllib import parse, request

from db_interface import database

from utils import utilities
from utils import path_manager as pm
from utils import parameters as para
import utils.get_2861_gaode_geo_gov_id_info as gov_info
from product.xmd_monitor.modules.stable_calculate_score import prepare_data_for_es
# from readjust_stable_score_with_other_elements import readjust_main

logger = logging.getLogger("stable_monitor.store")

PRODUCT_TEST_SERVER = database.create_user_defined_database_server('192.168.0.133', '5434', 'postgres', '123456')

PRODUCT_OFFICIAL_SERVER = database.get_database_server_by_nick(database.SERVER_PRODUCT)

DB_NAME = 'product'

TABLE_NAME = 'interface_stable'

score_data_path = pm.STABLE_SCORE_STORAGE

CLASS_URL = "http://192.168.0.88:8882/env/?"

df_meta = pd.read_csv(pm.META_PATH, encoding="utf-8-sig")

# 政府留言信息表
msg_server = database.create_user_defined_database_server(host="192.168.0.133",port="6500",user="etherpad", pwd="123456")
msg_db = "text-mining"
msg_table = "zk_leader_mailbox_msg"


# 初始化政府留言数据库链接
def init_db_conn_msg(write_mode=True):
    """
    @功能：初始化政府留言数据库链接
    :param write_mode:
    :return:
    """
    conn = database.ConnDB(msg_server, msg_db, msg_table)
    if write_mode:
        conn.switch_to_arithmetic_write_mode()

    return conn


def create_data_table(test=False):
    if test:
        db_server = PRODUCT_TEST_SERVER
    else:
        db_server = PRODUCT_OFFICIAL_SERVER

    create_sql = open('./db.sql', encoding='utf-8').read().strip()

    conn = database.ConnDB(db_server, DB_NAME, None)

    conn.switch_to_arithmetic_write_mode()

    ret = conn.execute(create_sql)

    if not ret.code:
        print(ret.result)

    return


# 写不稳定指数 —— 综合历史VS当月 指数
def update_stable_score_into_product(version, accumulatively=True):
    # score_file = score_data_path+version+'/'    # +'STABLE_STATUS_grade.csv'

    if 1:
        if accumulatively:
            file_name = "2018-01_%s_STABLE_SCORE_FINAL.csv" % version[0:7]
            df_score = pd.read_csv(score_data_path + "score/" + file_name, index_col='gov_code', encoding='utf-8')
            data_name = "不稳定指数（综合历史）"
            data_sign = "stable_index_comprehensively"
        else:
            file_name = "STABLE_SCORE_FINAL.csv"
            df_score = pd.read_csv(score_data_path + version + '/' + file_name, index_col="gov_code", encoding="utf-8")
            data_name = "不稳定指数（当月）"
            data_sign = "stable_index_monthly"

    # 对2018-01的数据，累计和当月一样
    else:
        file_name = "STABLE_SCORE_FINAL.csv"
        df_score = pd.read_csv(score_data_path + version + '/' + file_name, index_col="gov_code", encoding="utf-8")
        if accumulatively:
            data_name = "不稳定指数（综合历史）"
            data_sign = "stable_index_comprehensively"
        else:
            data_name = "不稳定指数（当月）"
            data_sign = "stable_index_monthly"

    content_dict = {}
    for index, row in df_score.iterrows():
        content_dict[str(int(row["gov_id"]))] = row["stable_index"]

    data_dict = {}
    data_dict["node_name"] = "TOP_STABLE"
    data_dict["data_name"] = data_name   # 是结合历史上的数据评估的
    data_dict["data_type"] = "index"
    data_dict["version"] = version
    data_dict["content"] = json.dumps(content_dict)
    data_dict["data_sign"] = data_sign

    for server in [PRODUCT_OFFICIAL_SERVER, PRODUCT_TEST_SERVER]:
        conn = database.ConnDB(server, DB_NAME, None)
        conn.switch_to_arithmetic_write_mode()
        insert_sql = database.create_insert_sql(TABLE_NAME, data_dict)
        insert_sql += " on conflict on CONSTRAINT stable_unique do UPDATE SET content='%s', update_time='%s'"%(data_dict["content"], datetime.now())
        print(insert_sql)
        ex_res = conn.execute(insert_sql)
        if not ex_res.code:
            print(ex_res.result)
        conn.disconnect()

    return


# 写入留言板数据库
def write_msg_into_db(data_list, debug=False):
    conn = init_db_conn_msg()

    for data in data_list:
        insert_sql = database.create_insert_sql(msg_table, data)
        insert_sql += " on conflict on CONSTRAINT mailbox_unique do update set %s, last_update='%s';"%(", ".join(["%s=%d"%(key, data[key]) if isinstance(data[key], int) else "%s='%s'"%(key, data[key]) for key in data.keys()]), datetime.now())

        if debug:
            print(insert_sql)

        ex_res = conn.execute(insert_sql)
        if not ex_res.code:
            print("Failed to insert into msg_db: ", ex_res.result)

    conn.disconnect()
    return


# 留言板信息入库
def fetch_msg_data_into_db(gov_id, start_date, end_date):
    result, total_num = utilities.es_get_data_day_by_day(gov_id=int(gov_id), interval=[start_date, end_date], index="zk_leader_mailbox_msg")

    if total_num != 0:
        table_list = []
        for res_info in result:
            data_dict = {}
            data_dict["gov_id"] = int(gov_id)
            data_dict["gov_code"] = str(int(res_info["_source"]["gov_code"]))
            data_dict["gov_name"] = res_info["_source"]["gov_name"]
            data_dict["pub_time"] = res_info["_source"]["pub_time"]
            data_dict["do_time"] = res_info["_source"]["do_time"]
            data_dict["source_type"] = res_info["_type"]
            data_dict["site_code"] = res_info["_source"]["site_code"]
            data_dict["msg_type"] = res_info["_source"]["msg_type"]
            data_dict["event_id"] = res_info["_source"]["data_id"]
            data_dict["event_title"] = res_info["_source"]["title"]
            data_dict["event_content"] = res_info["_source"]["content"]
            data_dict["event_url"] = res_info["_source"]["url"]
            # print(type(res_info["_source"]["reply_pub_time"]))
            # print(res_info["_source"]["reply_pub_time"] == None)
            data_dict["reply_flag"] = res_info["_source"]["reply_flag"]
            data_dict["reply_time"] = "" if res_info["_source"]["reply_pub_time"] is None else res_info["_source"]["reply_pub_time"]
            data_dict["reply_content"] = "" if res_info["_source"]["reply_content"] is None else res_info["_source"]["reply_content"]
            data_dict["reply_more"] = "" if res_info["_source"]["replay_more"] is None else res_info["_source"]["replay_more"]
            data_dict["is_deleted"] = 1 if (('keep' in res_info["_source"]) and res_info["_source"]["keep"] == 1) else 0

            # 判断事件类型/敏感词/官职/部门
            data_dict["sensitive_word"] = str([i["word"] for i in utilities.match_warning_keywords_frontend(res_info["_source"]["content"], type="sensitive", freq=False)]).replace("'",'"')
            data_dict["department"] = str([i["word"] for i in utilities.match_warning_keywords_frontend(res_info["_source"]["content"], type="department",freq=False)]).replace("'", '"')
            data_dict["gov_post"] = str([i["word"] for i in utilities.match_warning_keywords_frontend(res_info["_source"]["content"], type="guanzhi",freq=False)]).replace("'", '"')

            data_dict = utilities.judge_event_type(data_dict)

            table_list.append(data_dict)

        write_msg_into_db(table_list, debug=True)

    return


# 预测 —— 通过网页端访问模型预测，并返回结果
def get_env_classify_info(url, origin_data):
    count = 10
    df_results = pd.DataFrame()
    while count > 0:
        header_ = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
        header = {'User-Agent': header_}

        try:
            html = request.Request(url, headers=header)
            if html:
                data = request.urlopen(html).read()
                html = eval(data.decode('utf-8'))
                df_results = pd.DataFrame(html)
                return df_results
            else:
                continue
        except Exception as e:
            print("getHtml fail, info:%s"%e)
            print("retry times %s"%count)
            print(origin_data)   # 最好是打在log里面
            count -= 1
            time.sleep(random.uniform(0.5, 2))
    return df_results


# 清洗打好标记的帖子信息
def clean_env_info(df_temp):
    # 行数
    # temp_rows = df_temp.shape[0]

    # 加一列cnn判断的最终标签
    for index, row in df_temp.iterrows():
        if row["subsubclass_label"] != "":
            env_label_cnn = row["subsubclass_label"]
        elif row["subclass_label"] != "":
            env_label_cnn = row["subclass_label"]
        else:
            env_label_cnn = row["class_label"]

        df_temp.loc[index, "env_label"] = env_label_cnn

    # 数据清洗
    ratios = ["sentiment_ratio", "class_ratio", "subclass_ratio", "subsubclass_ratio"]
    for ratio_col in ratios:
        df_temp[ratio_col].replace([np.nan, '', None], 0, inplace=True)

    # df_temp["content"] = df_temp["content"].apply(lambda x:re.compile(r"['’‘]").sub("''", x))

    more_cols = ["env_label", "class_label", "class_ratio", "subclass_label", "subclass_ratio", "subsubclass_label", "subsubclass_ratio", "sentiment_label", "sentiment_ratio"]  # other的先不清洗，保留下来，作为关键词后的二次校正

    df_temp["more_info"] = df_temp.apply(lambda x: str({key:x[key] for key in more_cols}).replace("'", '"'), axis=1)

    return df_temp


# 环保模型分子类 —— 20条一批
def env_classify_by_cnn(df_event, batch_num=20):
    rows_count = df_event.shape[0]

    # if rows_count > 0:  —— 这个在调用函数前判断
    for batch in range(0, rows_count, batch_num):

        if (batch+batch_num) <= rows_count:
            last = batch + batch_num
        else:
            last = rows_count

        df_temp = df_event[batch:last]
        df_temp = df_temp.reset_index()  # drop=True —— 会丢掉event_id
        df_temp.rename(columns={"index":"event_id"}, inplace=True)
        df_temp["content_pre"] = df_temp["content"]

        url_texts = list(map(lambda x:"text="+parse.quote(x.encode('gbk', 'ignore').decode('gbk')), df_temp["content_pre"]))
        origin_texts = list(map(lambda x:"text=" + x.encode('gbk', 'ignore').decode('gbk'), df_temp["content_pre"]))

        if len(origin_texts) == 0:
            continue

        origin_data = '&'.join(origin_texts)
        url_data_new = '&'.join(url_texts)

        # 预测 —— 打分类+情绪标签
        url = CLASS_URL + url_data_new
        df_results = get_env_classify_info(url, origin_data)

        if df_results.shape[0] == 0:
            continue

        # # 拼接 —— 全是非环境的【其他】类，则走下一批 —— 暂时保留，在更新数据库时，修改event_type —— 暂时不改 打印出来看看 2018/12/17
        # if False not in (df_results['class_label'] == "others").values:
        #     continue

        cols = list(df_results)
        for col in cols:
            df_temp[col] = df_results[col]

        # 清洗数据
        df_temp = clean_env_info(df_temp)

        df_temp.set_index("event_id", inplace=True)

        df_event.update(df_temp)

    return df_event


# 用环保模型分一下污染子类
def judge_sub_env_msg_with_cnn(gov_id, interval=[]):
    conn = init_db_conn_msg()
    sqlenv = "SELECT event_id, event_content from %s where event_type = 'env' and gov_id=%d"%(msg_table, gov_id)
    if interval:
        sqlenv += " and pub_time >= '%s' and pub_time < '%s'"%(interval[0], interval[1])

    ret = conn.read(sqlenv)

    conn.disconnect()

    if not ret.code:
        print("Get env msg failed : %s"%ret.result)
        return False

    rows = ret.data

    # print(rows)

    if len(rows) == 0:
        return False

    df_env_msg = pd.DataFrame(rows)   # , index="event_id" —— 不能这样写, 要给list
    df_env_msg.set_index("event_id", inplace=True)
    df_env_msg.rename(columns={"event_content":"content"}, inplace=True)

    # 加两列空的，方便之后update
    df_env_msg = pd.concat([df_env_msg, pd.DataFrame(columns=["class_label", "more_info"])])

    df_env_msg = env_classify_by_cnn(df_env_msg)

    # 写数据库 —— 模型other的情况，修改event_type
    data_list = []
    for index, row in df_env_msg.iterrows():
        data_dict = dict()
        data_dict["event_id"] = index
        if row["class_label"] == "others":
            data_dict["event_type"] = "env_blur"
        data_dict["more_info"] = row["more_info"]
        data_list.append(data_dict)

    write_msg_into_db(data_list, debug=True)
    return


# 请求网页
def get_knowledge_html_result(url):
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


# data_dict.keys() = ["type_code", "module", "type_name", "data_unit", "category_big", "category_mid", "category_sub", "description", "submitter"]
def write_in_es_knowledge(data_dict, url):
    class_url = url+"operation_type=write&"
    url_values = parse.urlencode(data_dict)
    # print(url_values)
    meta_url = class_url+url_values
    result = get_knowledge_html_result(meta_url)
    return result


# 更新知识库meta
def update_stable_meta():
    for i in range(df_meta.shape[0]):
        result = write_in_es_knowledge(df_meta.iloc[i, :].to_dict(), para.META_URL)
        print(result, flush=True)

    return


# 写知识库 _得分数据
def update_monthly_score_data_into_es_knowledge(version_date):
    df_grade = pd.read_csv(pm.STABLE_SCORE_STORAGE+version_date+"/"+pm.STABLE_FINAL_MONTHLY_GRADE_CSV, index_col="gov_code", encoding="utf=8")

    # 写得分数据
    for col in pm.grade_cols:
        data_dict = {}
        type_code = df_meta[df_meta["column"] == col]["type_code"].values[0]  # 历史累计得分是values[1]
        submitter = df_meta[df_meta["column"] == col]["submitter"].values[0]

        version = version_date

        datas = {}

        for gov_code in para.df_2861_gaode_geo.index:
            gov_id = para.df_2861_gaode_geo.loc[gov_code, "gov_id"]
            datas[gov_id] = df_grade.loc[gov_code, col] if col != "stable_value" else int(df_grade.loc[gov_code, col]*1000/1.9)  # 影响力值转覆盖人次

        data_dict["type_code"] = type_code
        data_dict["version"] = version
        data_dict["submitter"] = submitter
        data_dict["datas"] = datas

        result = write_in_es_knowledge(data_dict, para.DATA_URL)
        print(result, flush=True)
        # print("[已完成稳定月度评分数据存入知识库] %s   code：%s" % (version, type_code), flush=True)
        logger.info("[已完成稳定月度评分数据存入知识库] %s   code：%s" % (version, type_code))

    return


# 写知识库 _累计得分数据 _生成时直接调用写入es的函数，和其他指标从文件读取区分开
def update_accumulatively_score_data_into_es_knowledge(data_df, end_date):
    # 写得分数据
    data_dict = dict()
    type_code = df_meta[df_meta["column"] == "stable_grade"]["type_code"].values[1]
    submitter = df_meta[df_meta["column"] == "stable_grade"]["submitter"].values[1]
    version = end_date

    datas = dict()

    for gov_code in para.df_2861_gaode_geo.index:
        gov_id = para.df_2861_gaode_geo.loc[gov_code, "gov_id"]
        datas[gov_id] = data_df.loc[gov_code, "stable_grade"]

    data_dict["type_code"] = type_code
    data_dict["version"] = version
    data_dict["submitter"] = submitter
    data_dict["datas"] = datas

    result = write_in_es_knowledge(data_dict, para.DATA_URL)
    print(result, flush=True)
    print("[已完成稳定综合（历史）评分数据存入知识库] %s   code：%s" % (version, type_code), flush=True)
    logger.info("[已完成稳定综合（历史）评分数据存入知识库] %s   code：%s" % (version, type_code))

    return


# 写知识库 ——统计数据
def update_monthly_stats_data_into_es_knowledge(version_date):
    df_stats = pd.read_csv(pm.STABLE_SCORE_STORAGE + version_date + "/" + pm.STABLE_MONTHLY_EVENTS_STATS_CSV, index_col="gov_code", encoding="utf-8")

    # 写统计数据
    for col in pm.stats_cols:
        data_dict = {}
        type_code = df_meta[df_meta["column"] == col]["type_code"].values[0]
        submitter = df_meta[df_meta["column"] == col]["submitter"].values[0]
        version = version_date
        datas = {}
        for gov_code in para.df_2861_gaode_geo.index:
            gov_id = para.df_2861_gaode_geo.loc[gov_code, "gov_id"]
            datas[gov_id] = df_stats.loc[gov_code, col]

        data_dict["type_code"] = type_code
        data_dict["version"] = version
        data_dict["submitter"] = submitter
        data_dict["datas"] = datas

        # print(len(datas))

        result = write_in_es_knowledge(data_dict, para.DATA_URL)
        print(result, flush=True)
        # print("[已完成稳定月度统计数据存入知识库] %s   code：%s" % (version, type_code), flush=True)
        logger.info("[已完成稳定月度统计数据存入知识库] %s   code：%s" % (version, type_code))

    return


def update_stable_index_into_es_knowledge(version_date):
    file_path = pm.STABLE_SCORE_STORAGE + version_date + "/"
    file_name = "STABLE_INDEX_FOR_ES.csv"
    df_index_for_es = pd.read_csv(file_path+file_name, index_col="gov_id")

    # {"event_count":{1:100,2:22,……},……}
    # whole_data_dict = df_index_for_es.to_dict()

    # 这里只写column有值的行
    df_meta_ = df_meta[df_meta["column"]==df_meta["column"]]

    for index, row in df_meta_.iterrows():
        data_dict = dict()
        data_dict["type_code"] = row["type_code"]
        data_dict["submitter"] = row["submitter"]
        data_dict["version"] = version_date
        col = row["column"]
        data_dict["datas"] = df_index_for_es[col].to_dict()

        result = write_in_es_knowledge(data_dict, para.DATA_URL)
        print(result, flush=True)
        logger.info("[已完成稳定指标存入知识库] %s   code：%s" % (version_date, row["type_code"]))


# 知识库refresh
def es_knowledge_base_refresh():
    import requests
    params = {'operation_type': 'refresh'}
    rsp = requests.post("http://192.168.0.88:9400/knowledgebase/?", params, timeout=600)
    print(rsp.json())
    return


# 监测数据存储main函数
def stable_score_store_main(version_date):

    # 产品测试/正式服务器
    for accumulatively in [True, False]:
        update_stable_score_into_product(version_date, accumulatively)

    logger.info("[已完成不稳定指数存入产品及测试服务器] %s   表名：%s"%(version_date, TABLE_NAME))

    # 写入知识库
    prepare_data_for_es(version_date)
    update_stable_index_into_es_knowledge(version_date)

    return


# 补齐省/市数据 - 分数求平均，统计求和
def update_monthly_score_data_into_es_knowledge_with_munis_provs(version_date):
    df_county_grade = pd.read_csv(pm.STABLE_SCORE_STORAGE+version_date+"/"+pm.STABLE_FINAL_MONTHLY_GRADE_CSV, index_col="gov_code", encoding="utf-8")

    for col in pm.grade_cols:
        data_dict = dict()
        type_code = df_meta[df_meta["column"] == col]["type_code"].values[0]
        submitter = df_meta[df_meta["column"] == col]["submitter"].values[0]

        version = version_date

        datas = dict()

        for gov_code in para.df_2861_gaode_geo.index:
            gov_id = para.df_2861_gaode_geo.loc[gov_code, "gov_id"]
            datas[gov_id] = df_county_grade.loc[gov_code, col] if col != "stable_value" else int(df_county_grade.loc[gov_code, col]*1000/1.9)   # 影响力转覆盖人次

        # 补上市的数据
        city_ids = gov_info.get_all_city_gov_id_info().index.values.tolist()

        for city_id in city_ids:
            sub_ids = gov_info.get_city_sub_county_ids(city_id, with_hitec=False)
            datas[city_id] = df_county_grade[df_county_grade["gov_id"].isin(sub_ids)][col].sum() * 1000 / 1.9 if col == "stable_value" else df_county_grade[df_county_grade["gov_id"].isin(sub_ids)][col].mean()

        # 补上省的数据
        prov_ids = gov_info.get_all_province_gov_id_info().index.values.tolist()

        for prov_id in prov_ids:
            sub_ids = gov_info.get_prov_sub_county_ids(prov_id, with_hitec=False)
            datas[prov_id] = df_county_grade[df_county_grade["gov_id"].isin(sub_ids)][col].sum() * 1000 / 1.9 if col == "stable_value" else df_county_grade[df_county_grade["gov_id"].isin(sub_ids)][col].mean()

        # 补上省辖地级市（市辖无区县）的数据 - 该省所在市数据的中位数
        for muni_id in para.municipal_with_no_county_ids:
            prov_id = gov_info.get_parent_prov_id_info(muni_id)["gov_id"]
            sub_muni_ids = gov_info.get_prov_sub_city_ids(prov_id)

            sub_muni_grades = [datas[sub_muni_id] for sub_muni_id in sub_muni_ids]

            if col == "stable_value":
                muni_grade = np.median(sub_muni_grades) * 1000 / 1.9
            else:
                muni_grade = np.median(sub_muni_grades)

            # datas[muni_id] = df_county_grade[df_county_grade["gov_id"].isin(sub_muni_ids)][col].median() * 1000 / 1.9 if col == "stable_value" else df_county_grade[df_county_grade["gov_id"].isin(sub_muni_ids)][col].median()

            datas[muni_id] = muni_grade

        data_dict["type_code"] = type_code
        data_dict["version"] = version
        data_dict["submitter"] = submitter
        data_dict["datas"] = datas

        print(len(datas), flush=True)
        result = write_in_es_knowledge(data_dict, para.DATA_URL)
        print(result, flush=True)

    return


if __name__ == "__main__":
    if 0:
        # for line in open(score_data_path+'version.txt',encoding='utf-8').readlines():
        version = open(score_data_path+'version.txt',encoding='utf-8').readlines()[-1].strip()
        update_stable_score_into_product(version)

    if 0:
        for gov_code in para.df_2861_gaode_geo.index:
            gov_id = para.df_2861_gaode_geo.loc[gov_code, "gov_id"]
            # if gov_id != 4:
            #     continue
            fetch_msg_data_into_db(gov_id, '2018-01-01', '2018-12-15')

    if 0:
        for gov_code in para.df_2861_gaode_geo.index:
            gov_id = para.df_2861_gaode_geo.loc[gov_code, "gov_id"]
            judge_sub_env_msg_with_cnn(gov_id)

    if 0:
        # 写知识库
        versions = TimeDispose.get_all_version_dates(pm.STABLE_SCORE_STORAGE, "version.txt")

        for date in versions:
            update_monthly_score_data_into_es_knowledge(date)
            update_monthly_stats_data_into_es_knowledge(date)

        es_knowledge_base_refresh()

    # 补一下产品服务器的分数
    if 0:
        versions = TimeDispose.get_all_version_dates(pm.STABLE_SCORE_STORAGE, "version.txt")

        for date in versions[:1]:
            stable_score_store_main(date)

    if 0:
        versions = TimeDispose.get_all_version_dates(pm.STABLE_SCORE_STORAGE, "version.txt")

        for version_date in ["2019-06-30"]:
            update_monthly_score_data_into_es_knowledge_with_munis_provs(version_date)

    if 0:
        version_dates = TimeDispose.get_all_version_dates(pm.STABLE_SCORE_STORAGE, "version.txt")

        for version_date_ in version_dates:
            stable_score_store_main(version_date_)
            print("version_date=%s  稳定指标写入完毕"%version_date_)




