#!/usr/bin/env
# -*- coding:utf-8 -*-
#
import re
import copy
from datetime import datetime, timedelta
#
from elasticsearch import Elasticsearch
#
# from xmd_es import es_query_supervision
import requests

# 从全文检索查询相关数据
es = Elasticsearch(["192.168.0.135"], timeout=60)


def es_get_weibo_count(es, query_count, gov_code, interval=[]):
    query = copy.deepcopy(query_count)
    filter_gov = {"terms": {"gov_code": [gov_code]}}
    query["query"]["bool"]["filter"].append(filter_gov)
    if interval:
        range_interval = {
            "range":{
            "pub_time":{"lte": datetime.strptime(interval[1], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                         "gte": datetime.strptime(interval[0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')}}}
        query["query"]["bool"]["filter"].append(range_interval)
    res = es.search(index="zk_social", body=query, size=10000)
    print("Gov Code %s, Took %s milliseconds. Got %d Hits." % (str(gov_code), res['took'], res['hits']['total']))
    total_hits = res['hits']['total']
    return total_hits


def es_get_weibo_info(es, query_complex, gov_code=0, max_size=100, interval=[]):
    query = copy.deepcopy(query_complex)
    if (gov_code != 0) and (len(str(gov_code))==6):
        filter_gov = {"terms": {"gov_code": [gov_code]}}
        query["query"]["bool"]["filter"].append(filter_gov)

    if interval:
        range_interval = {
            "range":{
            "pub_time":{"lte": datetime.strptime(interval[1], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                         "gte": datetime.strptime(interval[0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')}}}
        query["query"]["bool"]["filter"].append(range_interval)

    res = es.search(index="zk_social", body=query, size=max_size)
    print("Gov Code %s, Took %s milliseconds. Got %d Hits." % (str(gov_code), res['took'], res['hits']['total']))
    total_hits = res['hits']['total']
    if total_hits == 0:
        return []
    else:
        return res['hits']['hits']


def es_get_weibo_info_by_gov_id(es, query_complex, gov_id=0, max_size=10000,interval=[]):
    query = copy.deepcopy(query_complex)
    if (gov_id != 0):
        filter_gov = {"terms": {"gov_id": [gov_id]}}
        query["query"]["bool"]["filter"].append(filter_gov)

    if interval:
        range_interval = {
            "range": {
                "pub_time": {"lt": datetime.strptime(interval[1],'%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                             "gte": datetime.strptime(interval[0],'%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')}}}
        query["query"]["bool"]["filter"].append(range_interval)

    res = es.search(index="zk_social", body=query, size=max_size)
    # print("gov_id %s, Took %s milliseconds. Got %d Hits." % (str(gov_id), res['took'], res['hits']['total']))
    total_hits = res['hits']['total']
    if total_hits == 0:
        return []
    else:
        return res['hits']['hits']


def es_get_events_info(es, query_complex, gov_code=0, max_size=10000,interval=[]):
    query = copy.deepcopy(query_complex)
    if (gov_code != 0) and (len(str(gov_code)) == 6):
        filter_gov = {"terms": {"gov_code": [gov_code]}}
        query["query"]["bool"]["filter"].append(filter_gov)

    if interval:
        range_interval = {
            "range": {
                "event_time_start": {
                    "lt": datetime.strptime(interval[1],'%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                    "gte": datetime.strptime(interval[0],'%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')}}}
        query["query"]["bool"]["filter"].append(range_interval)

    # 改成按时间先后排，后面同一天合并时保留最初时间
    query["sort"] = {"event_time_start":{"order":"asc"}}
    res = es.search(index="zk_event", body=query, size=max_size)
    # print("Gov Code %s, Took %s milliseconds. Got %d Hits." % (str(gov_code), res['took'], res['hits']['total']))
    total_hits = res['hits']['total']
    if total_hits == 0:
        return [], total_hits
    else:
        return res['hits']['hits'], total_hits


# 汇总归纳信息，将相似或相关信息算作一条， 用于显示
def es_collect_significant_text(detail_dict, stats_words=[], similarity_threshhold=0.7):
    '''
    用于生成统计数据，和细节数据
    :param detail_dict: 包含所有命中信息的字典信息
    :param stats_words: 包含需要纳入统计的词
    :param similarity_threshhold: 判定为相似的距离门限，edit distance
    :return: 由归类好的信息组成的数组
    '''
    typical_messages = []
    for one_msg in detail_dict:
        msg_id = one_msg['_id']
        msg_src = one_msg['_source']
        msg_url = msg_src['url']
        msg_content = msg_src['content']
        msg_time = msg_src['pub_time']
        msg_n_share = msg_src['count_share']
        msg_n_comment = 0 # msg_src['count_comment']
        msg_n_thumb = msg_src['count_read']

        words = set()
        for a_word in stats_words:
            if msg_content.find(a_word) > -1:
                words.add(a_word)

        msg = {"id": msg_id, "url": msg_url,
               "content": msg_content, "time": msg_time, 'count': 1,
               "n_share": msg_n_share, "n_comment": msg_n_comment, "n_thumb": msg_n_thumb, "words": words}

        e_msg, e_topic = extract_topics(msg_content)  # 去掉微博中的话题
        e_msg = extract_chinese(e_msg)  # 去掉数字，标点符号，特殊字符，只保留中文
        e_msg_char_set = set(list(e_msg))

        new_message_flag = True
        position = 0
        for t_msg in typical_messages:
            t_msg_id = t_msg['id']

            if t_msg_id == msg_id:
                new_message_flag = False
                break

            t_msg_content = t_msg['content']
            e_t_msg, e_t_topic = extract_topics(t_msg_content)
            t_msg_time = t_msg['time']
            t_msg_count = t_msg['count']
            t_msg_n_share = t_msg['n_share']
            t_msg_n_comment = t_msg['n_comment']
            t_msg_n_thumb = t_msg['n_thumb']
            t_msg_words = t_msg['words']

            e_t_msg = extract_chinese(e_t_msg)
            e_t_msg_char_set = set(list(e_t_msg))

            # 相似, 在较长文本中
            inter_set = e_msg_char_set.intersection(e_t_msg_char_set)
            base_char_set = e_t_msg_char_set if len(e_t_msg_char_set) > len(e_msg_char_set) else e_msg_char_set

            if len(inter_set) / len(base_char_set) > similarity_threshhold:
                sub_position = position
                # 如果消息的时间更新则替换当前的
                # 设置消息统计数量的门限为 10万
                limit = 100000
                if datetime.strptime(msg_time, '%Y-%m-%d %H:%M:%S') > datetime.strptime(t_msg_time, '%Y-%m-%d %H:%M:%S'):
                    msg['count'] = t_msg_count + 1
                    if msg['count'] > limit:
                        msg['count'] = limit

                    msg['n_share'] += t_msg_n_share
                    if msg['n_share'] > limit:
                        msg['n_share'] = limit

                    msg['n_comment'] += t_msg_n_comment
                    if msg['n_comment'] > limit:
                        msg['n_comment'] = limit

                    msg['n_thumb'] += t_msg_n_thumb
                    if msg['n_thumb'] > limit:
                        msg['n_thumb'] = limit

                    msg['words'] = msg['words'].union(t_msg_words)
                    typical_messages[sub_position] = msg
                else:
                    typical_messages[sub_position]['count'] = t_msg_count + 1
                    if typical_messages[sub_position]['count'] > limit:
                        typical_messages[sub_position]['count'] = limit

                    typical_messages[sub_position]['n_share'] += t_msg_n_share
                    if typical_messages[sub_position]['n_share'] > limit:
                        typical_messages[sub_position]['n_share'] = limit

                    typical_messages[sub_position]['n_comment'] += t_msg_n_comment
                    if typical_messages[sub_position]['n_comment'] > limit:
                        typical_messages[sub_position]['n_comment'] = limit

                    typical_messages[sub_position]['n_thumb'] += t_msg_n_thumb
                    if typical_messages[sub_position]['n_thumb'] > limit:
                        typical_messages[sub_position]['n_thumb'] = limit

                    typical_messages[sub_position]['words'] = typical_messages[sub_position]['words'].union(words)

                new_message_flag = False
                break
            position += 1

        if new_message_flag:
            typical_messages.append(msg)

    return typical_messages

# 提取文本中的字符集
def extract_chinese(line):
    #line = str.strip().decode('utf-8', 'ignore')  # 处理前进行相关的处理，包括转换成Unicode等
    p2 = re.compile(u'[^\u4e00-\u9fa5]')  # 中文的编码范围是：\u4e00到\u9fa5
    zh = "".join(p2.split(line)).strip()
    #print('>> ' + zh)
    #zh = ",".join(zh.split())
    outStr = zh  # 经过相关处理后得到中文的文本
    return outStr

# 提取微博中的话题文字，包含#
def extract_topics(text_content):
    #text_content = '北京西城区 -:  # 熊梓淇# 你总是给我惊喜 #而我能做的# 只有'
    topic_sign_position = [i.start() for i in re.finditer('#', text_content)]
    weibo_topics = []
    for index in range(0, len(topic_sign_position) - 1, 2):
        weibo_topics.append(text_content[topic_sign_position[index]: topic_sign_position[index + 1] + 1])
    for topic in weibo_topics:
        text_content.replace(topic, '')
    return text_content, weibo_topics


# 兼容之前从数据库进行查询的格式
# def get_opn_style_ret(node_name, gov_code):
#     result_dict = {}
#     result_dict[node_name] = {"count": 0, "positive_count": 0, "positive_score": 0, "negative_count": 0,
#                             "negative_score": 0, "hit_words": [], "details": []}
#     es_query_supervision.init_query_set()
#     details_info = es_get_weibo_info(es, es_query_supervision.query_set[node_name], gov_code=int(str(gov_code)[0:6]), max_size=100)
#     # 计分方式可扩展
#     if details_info:
#         result_dict[node_name]["count"] = len(details_info)
#         result_dict[node_name]["positive_count"] = 0
#         result_dict[node_name]["positive_score"] = 0
#         result_dict[node_name]["negative_count"] = len(details_info)
#         result_dict[node_name]["negative_score"] = len(details_info)
#     return result_dict, []

def get_es_nodes():
    r = requests.get("http://192.168.0.52:9988/es.html")
    try:
        ret = r.json()
        if ret['code'] != '1000':
            ret = None
        else:
            ret = ret['nodes']
    except:
        ret = None

    if ret:
        return [{"host": node, "port": 9200} for node in ret]
    else:
        return None


if __name__ == "__main__":
    print('es_utils.py')
    #x, y = get_opn_style_ret("ZFZZ", 110105)
    #print(x)
    # 得到一个区县，一个时间段内的微博数量
