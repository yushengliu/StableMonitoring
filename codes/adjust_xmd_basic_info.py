#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/10/17 20:48
@Author  : Liu Yusheng
@File    : adjust_xmd_basic_info.py
@Description: 给xmd_event_basic_info表，新增more_info列，记录百度接口返回的情绪概率、分类类别、摘要等
"""
import logging
import time
from datetime import datetime, timedelta
import json
import pandas as pd
from copy import deepcopy
from elasticsearch import Elasticsearch

from utils.parameters import event_db_obj, event_table
from utils.NlpApi import BaiduNlpApi
from utils.MyModule import CJsonEncoder
from utils.es_utils import get_es_nodes

MAX_SUMMARY_LENGTH = 80

THD_IDS = [13000, 57000, 107000, 155000, 200000, 250000, 300000]

# es_host_cluster = ["192.168.0.135", "192.168.0.133", "192.168.0.38", "192.168.0.118", "192.168.0.46", "192.168.0.47", "192.168.0.56", "192.168.0.57"]
es_host_cluster = get_es_nodes()

g_es = Elasticsearch(es_host_cluster, maxsize=25, timeout=600, read_timeout=600, max_retries=10, retry_on_timeout=True)

logger = logging.getLogger("stable_monitor.adjust_xmd_basic_info")


def get_none_more_info_data(limit=None, thd_idx=6):
    """
    @功能：获取没写入more_info的数据
    :param limit:
    :return:
    """
    # sqlstr = "SELECT event_id, event_title, first_content FROM {} WHERE more_info is NULL ORDER BY id ASC".format(event_table)
    _s_date = (datetime.now() - timedelta(days=100)).strftime("%Y-%m-%d")
    sqlstr = "SELECT event_id, event_title, first_content FROM {} WHERE event_start_time >= '{}' and more_info is NULL ORDER BY id ASC".format(event_table, _s_date)

    if limit is not None:
        sqlstr += " LIMIT %d"%limit

    datas = event_db_obj.read_from_table(sqlstr)

    return datas


def get_event_title_str(org_event_title):
    """
    @功能：把数据库原始存的event_title(eg:[aa, bb, cc])，转换为拼接字符串的形式（aabbcc)
    :param org_event_title:
    :return:
    """

    org_event_title = eval(org_event_title) if isinstance(org_event_title, str) else org_event_title
    title_str = " ".join(org_event_title)

    return title_str


def nlp_process_multi(datas_dict, sentiment=True, category=True, category_with_more_info=True, summary=True):

    results_dict = dict()

    baidu_obj = BaiduNlpApi(datas_dict["content"])

    if sentiment:
        sentiments = baidu_obj.senti_auto_split()
        results_dict["sentiment"] = sentiments

    if category or summary:
        texts_data = [[data_dict["title"], data_dict["content"]] for data_dict in datas_dict]

        if category:
            if category_with_more_info:
                category_infos = baidu_obj.category_with_more_info(texts_data)
                results_dict.update()
            categorys = baidu_obj.category_by_sdk(texts_data)
            results_dict["category"] = categorys

        if summary:
            summarys = baidu_obj.newsSummary(MAX_SUMMARY_LENGTH)
            results_dict["summary"] = summarys

    return results_dict


def nlp_process_single(data_dict, sentiment=True, senti_desc=True, category=True, category_with_more_info=True, summary=True, default_result={}, nlp_idx=1):

    result_dict = dict()

    # 如果标题/内容有值
    if len(data_dict["content"])+len(data_dict["title"]):

        baidu_obj = BaiduNlpApi([data_dict["content"]], id_num=nlp_idx) if data_dict["content"] else BaiduNlpApi([data_dict["title"]], id_num=nlp_idx)

        if sentiment:
            sentiments = baidu_obj.senti_auto_split()
            result_dict["positive"] = sentiments[0][0]
            if senti_desc:
                result_dict["sentiment"] = get_sentiment_desc(result_dict["positive"])
            # print("sentiment finished", flush=True)

        if category or summary:
            if len(data_dict["title"]) and len(data_dict["content"]):
                texts_data = [[data_dict["title"], data_dict["content"]]]
            elif len(data_dict["title"]):
                texts_data = [[data_dict["title"], data_dict["title"]]]
            else:
                texts_data = [[data_dict["content"], data_dict["content"]]]

            if category:
                if category_with_more_info:
                    category_infos = baidu_obj.category_with_more_info(texts_data)
                    result_dict.update(category_infos[0])
                else:
                    categorys = baidu_obj.category_by_sdk(texts_data)
                    result_dict["category"] = categorys[0]
                # print("category finished", flush=True)

            if summary:
                summarys = baidu_obj.newsSummary(MAX_SUMMARY_LENGTH)
                result_dict["summary"] = summarys[0]
                # print("summary finished", flush=True)

    # 如果标题和内容均没有值
    else:
        result_dict = default_result

    return result_dict


def get_sentiment_desc(pos_prob):

    POS_THD = 0.55
    NEG_THD = 0.45

    senti_desc = "正" if pos_prob > POS_THD else "负" if pos_prob < NEG_THD else "中"

    return senti_desc


def update_db_more_info(df_data):

    BATCH_NUM = 10000

    df_data = deepcopy(df_data[["event_id", "more_info"]])

    # try:
    df_data["more_info"] = df_data["more_info"].apply(lambda x: json.dumps(x, ensure_ascii=False, cls=CJsonEncoder))
    # except Exception as e:
    #     print(e, flush=True)

    data_list = df_data.to_dict(orient="records")

    sqlstr_head = "UPDATE %s SET more_info='{more_info}' WHERE event_id='{event_id}';"%event_table

    sqlstr_list = [sqlstr_head.format(**data_row) for data_row in data_list]

    for i in range(0, len(data_list), BATCH_NUM):

        event_db_obj.execute_any_sql("".join(sqlstr_list[i:i+BATCH_NUM]))

        print("&&&&&&已更新%d条more_info&&&&&&"%len(sqlstr_list[i:i+BATCH_NUM]), flush=True)

    # event_db_obj.disconnect()


def mark_more_info(test_mode=False):

    TEST_LIMIT = 10

    start_time = time.time()
    event_db_obj.get_conn()

    if test_mode:
        events_datas = get_none_more_info_data(TEST_LIMIT)
    else:
        events_datas = get_none_more_info_data()

    # event_db_obj.disconnect()

    print("{} - 取数据完毕！耗时：{}".format(str(datetime.now()), time.time() - start_time), flush=True)

    process_start = time.time()
    df_events = pd.DataFrame(events_datas)

    if not df_events.shape[0]:
        print("more_info字段无缺失！", flush=True)
        return

    # 处理关键词列表 -> 关键词字符串
    df_events["event_title"] = df_events["event_title"].apply(lambda x: get_event_title_str(x))
    # 处理非法字符
    df_events["first_content"] = df_events["first_content"].apply(lambda x:x.encode('gbk','ignore').decode('gbk'))

    print("{} - 数据预处理完毕！耗时：{}".format(str(datetime.now()), time.time() - process_start), flush=True)

    PROCESS_BATCH = 500

    for line in range(0, df_events.shape[0], PROCESS_BATCH):
        df_events_ = deepcopy(df_events.iloc[line: line+PROCESS_BATCH])

        nlp_start = time.time()
        # 输入为空数据时，默认返回的dict
        empty_result = {"positive": 0.5, "sentiment": "中", "category": "社会", "cate_score": 0.5, "sub_cates":[], "summary": ""}
        df_events_["more_info"] = df_events_.apply(lambda x: nlp_process_single(data_dict={"title": x["event_title"], "content": x["first_content"]}, default_result=empty_result), axis=1)

        print("{} - NLP分析完毕！耗时：{}".format(str(datetime.now()), time.time() - nlp_start), flush=True)

        if test_mode:
            print(df_events, flush=True)

        update_start = time.time()
        # event_db_obj.get_conn()
        update_db_more_info(df_events_)
        print("{} - 更新数据完毕！耗时：{}".format(str(datetime.now()), time.time() - update_start), flush=True)

    event_db_obj.disconnect()
    print("{} - 全程耗时：{}".format(str(datetime.now()), time.time() - start_time), flush=True)
    logger.info("[已完成调用BaiduNlp接口，更新xmd_basic_info中的more_info字段] 更新数据：%d"%df_events.shape[0])


# ===================================================
def get_none_verified_type_data(limit=None):
    """
    @功能：获取没写入post_type的数据，只取more_info已经写了的字段 // 避免和更新more_info字段搞混
    :param limit:
    :return:
    """

    # sqlstr = "SELECT event_id, event_title, url_list, more_info FROM {} WHERE more_info is not NULL AND more_info ->> 'verified_type' is NULL ORDER BY id ASC".format(
    #     event_table)
    _s_date = (datetime.now() - timedelta(days=100)).strftime("%Y-%m-%d")
    sqlstr = "SELECT event_id, event_title, url_list, more_info FROM {} WHERE event_start_time >= '{}' and more_info is not NULL AND more_info ->> 'verified_type' is NULL ORDER BY id ASC".format(
        event_table, _s_date)

    if limit:
        sqlstr += " LIMIT %d" % limit

    event_db_obj.get_conn()

    datas = event_db_obj.read_from_table(sqlstr)

    event_db_obj.disconnect()

    return datas


def get_weibo_verified_type_from_es(urls):
    url_terms = [str(url).split("status/")[-1] for url in urls]

    es_query = {
        "query": {
            "bool": {
                "filter": [{"terms": {"url": url_terms}}]
            }},
        "sort": [{"pub_time": {"order": "asc"}}],
        "_source": ["verified_type"]}

    results = g_es.search(index="zk_social", body=es_query, size=len(urls))

    hits_results = results["hits"]

    return hits_results


def check_event_verified_type(weibo_verified_types):
    """
    @功能：从事件相关所有微博的verified_types，确定事件的verified_type - 暂取最多
    :param weibo_verified_types:
    :return:
    """

    if not weibo_verified_types:
        event_verified_type = -100

    else:
        event_verified_type = max(weibo_verified_types, key=weibo_verified_types.count)

    return event_verified_type


def get_event_verified_type(url_list):
    """
    @功能：获取事件主要的发博主体
    :return:
    """
    # POST_DICT = {"民众": [-1, ], "官方": [1], "媒体": [3]}

    results = get_weibo_verified_type_from_es(url_list)

    hits_total = results["total"]

    verified_types = []

    if hits_total:
        hits_results = results["hits"]
        for hit_dict in hits_results:
            hit_verified = hit_dict["_source"]["verified_type"] if hit_dict["_source"] else -1  # 没有打verified_type的微博，默认为普通用户
            verified_types.append(hit_verified)

    event_verified = check_event_verified_type(verified_types)

    return event_verified


def mark_event_verified_type(limit=None):

    start_time = time.time()

    none_verified_type_data = get_none_verified_type_data(limit)

    df_data = pd.DataFrame(none_verified_type_data)

    print("{} - 取未标记verified_type的事件数据完毕，耗时：{}".format(str(datetime.now()), time.time() - start_time), flush=True)

    event_db_obj.get_conn()

    PROCESS_BATCH = 1000

    for line in range(0, df_data.shape[0], PROCESS_BATCH):
        pre_start = time.time()

        df_data_ = deepcopy(df_data.iloc[line: line+PROCESS_BATCH])

        df_data_["verified_type"] = df_data_["url_list"].apply(lambda x: get_event_verified_type(x))

        df_data_["more_info"] = df_data_.apply(lambda x: {**x["more_info"], "verified_type": x["verified_type"]}, axis=1)

        print("{} - 预处理-添加verified_type数据完毕，耗时：{}".format(str(datetime.now()), time.time() - pre_start), flush=True)
        # print(df_data, flush=True)

        into_db_start = time.time()

        update_db_more_info(df_data_)

        print("{} - 更新数据库more_info字段完毕，耗时：{}".format(str(datetime.now()), time.time() - into_db_start), flush=True)

    event_db_obj.disconnect()
    print("{} - 更新verified_type全程耗时：{}".format(str(datetime.now()), time.time() - start_time), flush=True)
    logger.info("[已完成more_info中verified_type标记] 更新数据：%d"%df_data.shape[0])


# ===================================================

def adjust_main(test_mode=False):
    """
    @功能：总调度 - 调整（增加信息）xmd_basic_info表
    :param test_mode:
    :return:
    """
    # 添加more_info - NLP模型
    mark_more_info(test_mode)

    # 添加发博主体判断
    LIMIT = 10 if test_mode else None
    mark_event_verified_type(LIMIT)

    logger.info("[已完成adjust_xmd_basic_info.py的运行]")


if __name__ == "__main__":
    # 每天运行
    adjust_main()
