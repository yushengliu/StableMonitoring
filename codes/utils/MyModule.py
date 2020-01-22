"""
time:2018-01-10
author:Liu Yusheng
description: usual functions
"""

# !/usr/bin/python
# coding: utf-8

import time
from pandas.tseries.offsets import *
import pandas as pd
import numpy as np
import datetime
from datetime import date, timedelta
import calendar
from db_interface import database
import json
import re
import jieba
import jieba.analyse
import jieba.posseg as pseg
import os
import psycopg2
import sys,codecs
import io
import urllib3
import requests
import logging
import random
from urllib import parse, request

# import conf.path_manager as pm

# sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
# 或者
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# 处理json格式转换时遇到datetime的情况 / np.int64等情况
class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


# json的读写
class JsonDispose:
    # 存储到json文件
    def __init__(self, file_path, filename):
        self.file_path = file_path
        self.filename = filename

    def save_to_json(self, content, mode):
        if not os.path.exists(self.file_path):
            os.makedirs(self.file_path)
        # mode: 'w'代表覆盖写入， 'a'代表继续写入，encoding编码格式可变
        with open(self.file_path+self.filename, mode, encoding='utf-8') as f:
            # json写入时加上ensure_ascii=False,否则默认是ascii写入
            json.dump(content, f, cls=CJsonEncoder)
            # 比dumps多了写入文件的参数/dumps只负责转换格式：dict->json str
            print('Write %s down!' % self.filename)
        return

    # 从json文件中读取
    def read_from_json(self):
        with open(self.file_path+self.filename, 'r') as f:
            data = json.load(f)
            print('Read %s out!' % self.filename)
        return data


# 时间/版本相关
class TimeDispose:
    def __init__(self, input_time):
        self.input_time = input_time

    def time_normalization(self):
        if type(self.input_time) == str or str(type(self.input_time)) == "<class 'str'>":
            normalized_time = datetime.datetime.strptime(str(self.input_time).split(' ')[0], '%Y-%m-%d')
        else:
            normalized_time = self.input_time
        return normalized_time

    # 根据需要返回给定日期的当前日、周、月、季度、半年、年的起始日期
    def date_to_period(self, period_type):
        date_ = self.time_normalization().date()
        if period_type == 'day':
            s_date = e_date = datetime.datetime.strptime(str(date_), '%Y-%m-%d')
        elif period_type == 'week':
            dayscount = datetime.timedelta(days=date_.isoweekday())
            s_date = date_ - dayscount + datetime.timedelta(days=1)
            e_date = date_ - dayscount +datetime.timedelta(days=7)
        elif period_type == 'month':
            s_date = '%d-%02d-01' % (date_.year, date_.month)
            # Returns weekday-1 of first day of the month and number of days in month,
            # for the specified year and month.
            wday, monthRange = calendar.monthrange(date_.year, date_.month)
            # print(wday)
            e_date = '%d-%02d-%02d' % (date_.year, date_.month, monthRange)
            s_date = datetime.datetime.strptime(s_date, '%Y-%m-%d')
            e_date = datetime.datetime.strptime(e_date, '%Y-%m-%d')
        elif period_type == 'quarter':
            s_date = date_ + DateOffset(months=-((date_.month - 1) % 3), days=1 - date_.day)
            e_date = date_ + DateOffset(months=3 - ((date_.month - 1) % 3), days=-date_.day)
        elif period_type == 'halfyear':
            if date_.month <= 6:
                s_date = '%d-01-01' % date_.year
                e_date = '%d-06-30' % date_.year
            else:
                s_date = '%d-07-01' % date_.year
                e_date = '%d-12-31' % date_.year
            s_date = datetime.datetime.strptime(s_date, '%Y-%m-%d')
            e_date = datetime.datetime.strptime(e_date, '%Y-%m-%d')
        elif period_type == 'year':
            s_date = date_ + DateOffset(months=1 - date_.month, days=1 - date_.day)
            e_date = date_ + DateOffset(years=1, months=1 - date_.month, days=- date_.day)
        # 输入其他字符的period都当作返回当天
        else:
            s_date = e_date = date_
        # print(s_date, e_date)

        # 结束日往后加一天
        e_date = e_date + timedelta(days=1)
        return str(s_date), str(e_date)

    # 返回过去三月
    def get_past3months_period(self):
        this_date = self.time_normalization()
        # this_date = end_date.date()
        if this_date.month > 3:
            start_date = datetime.datetime(this_date.year, this_date.month-3, this_date.day)
        else:
            start_date = datetime.datetime(this_date.year-1, this_date.month-3+12, this_date.day)
        return [start_date, this_date]

    # 返回过去半年
    def get_past6months_period(self):
        this_date = self.time_normalization()
        if this_date.month > 6:
            start_date = datetime.datetime(this_date.year, this_date.month-6, this_date.day)
        else:
            start_date = datetime.datetime(this_date.year-1, this_date.month-6+12, this_date.day)
        return [start_date, this_date]

    # 返回过去一年
    def get_past1year_period(self):
        this_date = self.time_normalization()
        start_date = datetime.datetime(this_date.year-1, this_date.month, this_date.day)
        return [start_date, this_date]

    # 往前推N天的日期
    def get_ndays_ago(self, n):
        date = self.time_normalization()
        n_days_ago = date - timedelta(days=n)
        return n_days_ago

    # 返回输入日期，下个月1号的日期 —— 字符串格式:"2018-09-01"
    def get_next_month_first_day(self):
        nums = str(self.input_time).split(' ')[0].split('-')
        if int(nums[1]) == 12:
            next_date = date(int(nums[0]) + 1, 1, 1)
        else:
            next_date = date(int(nums[0]), int(nums[1]) + 1, 1)
        return str(next_date)

    # 返回输入日期，上个月1号的日期 —— 字符串格式："2018-09-01"
    def get_last_month_first_day(self):
        nums = str(self.input_time).split(' ')[0].split('-')
        if int(nums[1]) == 1:
            next_date = date(int(nums[0]) - 1, 12, 1)
        else:
            next_date = date(int(nums[0]), int(nums[1]) - 1, 1)
        return str(next_date)

    # 返回输入日期，当月1号的日期 —— 字符串格式:"2018-09-01"
    def get_this_month_first_day(self):
        nums = str(self.input_time).split(' ')[0].split('-')
        first_date = date(int(nums[0]), int(nums[1]), 1)
        return str(first_date)

    # 获取当前时间
    @staticmethod
    def get_current_time():
        str_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        return str_time

    # get最近更新的版本信息
    @staticmethod
    def get_last_version_date(version_path, version_name):
        version_name = version_path + version_name
        if os.path.exists(version_name):
            # version_date = open(version_name, 'r', encoding='utf-8').readlines()[-1].strip()
            version_dates = [line.strip() for line in open(version_name, 'r', encoding="utf-8").readlines()]
            return max(version_dates)
        else:
            print("Version_File %s not exist !"%version_name)
            return None

    # get最初更新的版本信息
    @staticmethod
    def get_first_version_date(version_path, version_name):
        version_name = version_path + version_name
        if os.path.exists(version_name):
            # version_date = open(version_name, 'r', encoding='utf-8').readlines()[0].strip()
            version_dates = [line.strip() for line in open(version_name, 'r', encoding="utf-8").readlines()]
            return min(version_dates)
        else:
            print("Version_File %s not exist !" % version_name)
            return None

    # get全部版本信息
    @staticmethod
    def get_all_version_dates(version_path, version_name):
        version_name = version_path + version_name
        if os.path.exists(version_name):
            version_dates = [line.strip() for line in open(version_name, 'r', encoding='utf-8').readlines()]
            return version_dates
        else:
            print("Version_File %s not exist !" % version_name)
            return None

    @staticmethod
    # 更新version文件
    def update_version(version_path, version_name, file_date):
        version_name = version_path + version_name
        if os.path.exists(version_name):
            with open(version_name, 'a', encoding='utf-8') as fp_out:
                fp_out.write('\n' + file_date)
        else:
            with open(version_name, 'w', encoding='utf-8') as fp_out:
                fp_out.write(file_date)
        print("Update %s with %s DONE !"%(version_name, file_date))
        return


# 仅针对公司的database的包的操作
class DataBaseFirm:
    def __init__(self, server, db, table):
        self.server = server
        self.db = db
        self.table = table
        self._conn = None

    def get_conn(self):
        self._conn = database.ConnDB(self.server, self.db)
        # 连接设置算法组可写权限
        self._conn.switch_to_arithmetic_write_mode()
        return self._conn

    # 用完后可以直接del 这个对象
    def __del__(self):
        if self._conn is not None:
            self._conn.disconnect()

    def disconnect(self):
        if self._conn is not None:
            self._conn.disconnect()

    def desc_table(self, table_name):
        """
        @功能：查看表结构
        :param table_name:表名
        :return:
        """
        sqlstr = "SELECT column_name, udt_name FROM information_schema.columns WHERE table_name = '{0}';".format(table_name)
        res = self._conn.read(sqlstr)
        result = {}
        for item in res.data:
            result[item["column_name"]] = item["udt_name"]
        return result

    # 读取数据库
    def read_from_table(self, sql):
        # time1 = time.time()
        # conn = database.ConnDB(self.server, self.db, self.table)
        retrieve = self._conn.read(sql)
        if not retrieve.code:
            print("Read DB failed:", retrieve.result)
        datas = retrieve.data
        # conn.disconnect()
        return datas

    # 读2861基本库
    @staticmethod
    def read_basic_db(gov_id, db_name, sql):
        get_server = database.get_database_server_by_nick(database.SERVER_SPIDER_BASE_DATA, 'id_' + str(gov_id))
        conn_obj = database.ConnDB(get_server, None, db_name)
        res = conn_obj.read(sql)
        if res.code:
            rows = conn_obj.read(sql).data
        else:
            print('read 2861 basic db_%d fail!%s'%(gov_id, res.result))
            rows = 0
        conn_obj.disconnect()
        return rows

    # 按时间提取2861基础库的信息
    @staticmethod
    def get_data_from_basic_by_time(gov_id, last_pub_time, latest_pub_time, local=False):
        db_name = 'gov_id_'+str(gov_id)
        if not local:
            sqlstr = "select * from base_events where pub_time >= '%s' and pub_time < '%s'"%(last_pub_time, latest_pub_time)
        else:
            # 去掉LBS微博加在前面的标记
            sqlstr = "select *, LTRIM(SUBSTRING(content,POSITION('-:' IN content)+length('-:')-1,LENGTH(content)),':') as content_new from local_base_events where pub_time >= '%s' and pub_time < '%s'"%(last_pub_time, latest_pub_time)
        rows = DataBaseFirm.read_basic_db(gov_id, db_name, sqlstr)
        return rows

    @staticmethod
    def execute_sql_through_conn(conn, sqlstr):
        """
        @功能：入参链接conn，执行操作
        :param conn:
        :param sqlstr:
        :return:
        """
        ex_res = conn.execute(sqlstr)
        if not ex_res.code:
            print("Execute Failed Reason: %s"%ex_res.result, flush=True)
        else:
            print("Successfully Executed!", flush=True)

    # 建表
    def create_table(self, field_dict, primary_key):
        # conn = database.ConnDB(self.server, self.db)
        sql_create = database.create_table_sql(self.table, field_dict, primary_key)
        print(sql_create)
        # res = conn.switch_to_arithmetic_write_mode()
        # if not res.code:
        #     print("Failed to Switch to write mode OR not Corresponding DB:", res.result)
        ex_res = self._conn.execute(sql_create)
        if not ex_res.code:
            print("Failed to Create a new table - %s: %s"%(self.table, ex_res.result))
        else:
            print('Successfully Created %s !'%self.table)
        # conn.disconnect()

    # 写表
    def insert_one_into_table(self, data_dict):
        # conn = database.ConnDB(self.server, self.db, self.table)
        sql_insert = database.create_insert_sql(self.table, data_dict)
        print(sql_insert)
        # res = conn.switch_to_arithmetic_write_mode()
        # if not res.code:
        #     print("Failed to Switch to write mode OR not Corresponding DB:", res.result)
        ex_res = self._conn.execute(sql_insert)
        if not ex_res.code:
            print("Failed to Insert table - %s: %s"%(self.table, ex_res.result))
        else:
            print("Successfully Inserted into %s !"%self.table)
        # conn.disconnect()

    # 写另一张表
    def insert_one_into_another_table(self, table_name, data_dict, on_conflict_do_nothing=True):
        # conn = database.ConnDB(self.server, self.db, table_name)
        sql_insert = database.create_insert_sql(table_name, data_dict)
        if on_conflict_do_nothing:
            sql_insert += " ON CONFLICT DO NOTHING"
        print(sql_insert)
        # res = self._conn.switch_to_arithmetic_write_mode()
        # if not res.code:
        #     print("Failed to Switch to write mode OR not Corresponding DB:", res.result)
        ex_res = self._conn.execute(sql_insert)
        if not ex_res.code:
            print("Failed to Insert table - %s: %s" % (table_name, ex_res.result))
        else:
            print("Successfully Inserted into %s !" % table_name)
        # conn.disconnect()

    # 执行操作
    def execute_any_sql(self, sql):
        # conn = database.ConnDB(self.server, self.db, self.table)
        # res = conn.switch_to_arithmetic_write_mode()
        # if not res.code:
        #     print("Failed to Switch to write mode OR not Corresponding DB:%s"%res.result, flush=True)
        ex_res = self._conn.execute(sql)
        if not ex_res.code:
            print("Execute Failed Reason: %s"%ex_res.result, flush=True)
        else:
            print("Successfully Executed %s !"%self.table)
            pass
        # conn.disconnect()


# 对Python自带的数据库的包
class DataBasePython:
    def __init__(self, host="192.168.0.117", user="readonly", pwd="123456", port="5555"):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port

    def select_data_from_db(self, db, sql):
        rows = 0
        try:
            conn = psycopg2.connect(dbname=db, user=self.user, password=self.pwd, host=self.host, port=self.port, client_encoding='utf-8')
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            print(e)
        return rows

    def select_data_from_db_one_by_one(self, db, sql):
        rows = []
        conn = psycopg2.connect(dbname=db, user=self.user, password=self.pwd, host=self.host, port=self.port, client_encoding='utf-8')
        cur = conn.cursor()
        cur.execute(sql)
        rowcount = cur.rowcount
        # row = 0
        for i in range(0, rowcount):
            try:
                row = cur.fetchone()
                # print(row)
                rows.append(row)
            except Exception as e:
                # print(row)
                # sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
                print(db+': '+str(e))
                # row = 0
                continue
        conn.close()
        return rows

    def fetch_basic_data_one_by_one(self, gov_id, sql):
        if 1 <= gov_id <= 799:
            host = "192.168.0.117"
        elif 800 <= gov_id <=1599:
            host = "192.168.0.118"
        elif 1600 <= gov_id <= 2399:
            host = "192.168.0.133"
        else:
            host = "192.168.0.138"
        self.host = host
        db = 'gov_id_'+str(gov_id)
        rows = self.select_data_from_db_one_by_one(db, sql)
        return rows

    def execute_any_sql(self, db, sql):
        try:
            conn = psycopg2.connect(dbname=db, user=self.user, host=self.host, password=self.pwd, port=self.port, client_encoding='utf-8')
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print(db+':'+e)
        conn.close()
        return


# 微博文本预处理
class TextDispose:
    def __init__(self, text):
        self.text = text

    # 去除浏览器、编码
    def get_weibo_valid_info(self, go_on=False):
        content = repr(self.text)[1:-1]
        pattern = re.compile(r'\<[\s\S]*?\>')  # 去掉中间的无效浏览器内容
        result = pattern.sub('', content)
        pattern2 = re.compile(r'&quot|\u200b|\u3000|\\u200b|\\u3000')
        result2 = pattern2.sub('', result)
        if go_on:
            self.text = result2
        # print(result)
        return result2

    # 去除[网页链接] / @ / #话题# 等
    def text_preparation(self, min_len, max_len, go_on=False):
        # if not len(text):
        #     return None
        text = self.text.replace('网页链接', '').strip().replace(r'\u200b', '').replace('[带着微博去旅行]', ' ')
        text = re.sub('#.*?#', '', text)
        if len(re.findall("@", text)) >= 3:
            text = re.sub('@.*? |@.*?\r\n|@.*?(：|:)', '', text)
        text = text.strip()
        # print(len(text))
        if min_len <= len(text) <= max_len and len(re.findall("/", text)) <= 4:
            # text = jieba_split_sentence(text)
            # words, flags = self.jieba_cut_sentences(text)
            # text = ' '.join(words)
            if go_on:
                self.text = text
            return text
        else:
            return None

    # 结巴分词并剔除停用词、数字、字母
    def jieba_cut_sentences_with_flags(self, word_flags_excluded=['x'], stopword_file=None, with_flags=False):
        # 先剔除数字和字母
        pattern = re.compile(r'[\d+a-zA-Z]')
        text = re.sub(pattern, '', self.text)
        # 去除标点符号
        pattern_pt = re.compile(r'[\s+\!\/_,$%^*(+\"\')]+|[:：+——()?【】“”！，。？、~@#￥%……&*（）]+')
        text = re.sub(pattern_pt, '', text)

        result = pseg.cut(text)
        words = []
        flags = []
        if stopword_file:
            stopWords = open(stopword_file, 'r', encoding='utf-8').read()
            for w in result:
                if w.flag not in word_flags_excluded and w.word not in stopWords:
                    words.append(w.word)
                    flags.append(w.flag)
        else:
            for w in result:
                if w.flag not in word_flags_excluded:
                    words.append(w.word)
                    flags.append(w.flag)

        if with_flags:
            return words, flags
        else:
            return words

    # 提取关键词 —— 剔除数字、字母、停用词后的关键词
    def jieba_extract_top_keywords(self, topK=20, method="tf-idf", word_flags_allowed=[], stopword_file=None, with_freqs=False, with_flags=False):
        words = []
        freqs = []
        flags = []

        # 先剔除数字和字母
        pattern = re.compile(r'[\d+a-zA-Z]')
        text = re.sub(pattern, '', self.text)
        # 去除标点符号
        pattern_pt = re.compile(r'[\s+\!\/_,$%^*(+\"\')]+|[:：+——()?【】“”！，。？、~@#￥%……&*（）]+')
        text = re.sub(pattern_pt, '', text)

        # 如果有停用词的话
        if stopword_file:
            jieba.analyse.set_stop_words(stopword_file)

        if method == "tf-idf":
            tags_with_weight = jieba.analyse.extract_tags(text, topK=topK, allowPOS=tuple(word_flags_allowed), withWeight=True, withFlag=with_flags)
        elif method == "textrank":
            # "textrank"暂时不好这么用 —— 默认词性受限+withFlag和allowPOS绑定在一起，需要改源码
            tags_with_weight = jieba.analyse.textrank(text, topK=topK, allowPOS=tuple(word_flags_allowed), withWeight=True, withFlag=with_flags)
        else:
            print("入参method错误，请输入tf-idf或textrank", flush=True)
            return False

        for word_info in tags_with_weight:
            if with_flags:
                words.append(list(word_info[0])[0])
                flags.append(list(word_info[0])[1])
            else:
                words.append(word_info[0])
            freqs.append(word_info[1])

        if with_freqs:
            if with_flags:
                return words, freqs, flags
            else:
                return words, freqs
        else:
            if with_flags:
                return words, flags
            else:
                return words

    def get_key_sentences_of_text(self, key_words=[]):
        """
        @功能：提取文章主题句
        :param key_words:
        :return:
        """

        title_r = r'【(.*?)】'

        if re.search(title_r, self.text):
            title_sentence = re.search(title_r, self.text).group(1)
            if len(title_sentence) >= 10:
                key_sentence = title_sentence
                return key_sentence

        punctuation_r = r'？|\?|\.|。|；|;|！|!|…|……'  # |/| |:|：|【|】|,|，
        split_strs = re.split(punctuation_r, self.text)

        if not key_words:
            key_sentence = "，".join(split_strs[:2])
        else:
            front_two_sentences = "，".join(split_strs[:2])

            words_r = r"|".join(key_words)
            pattern = re.compile(words_r)

            # if re.search(words_r, front_two_sentences):
            #     key_sentence = front_two_sentences
            # else:
            match_words = 0
            key_sentence = ""
            for split_str in split_strs:
                match_words_list = pattern.findall(split_str)
                if len(match_words_list) > match_words:
                    match_words = len(match_words_list)
                    key_sentence = split_str

        print("\nkey_words:{}".format(key_words), flush=True)
        print("event_content:{}".format(self.text), flush=True)
        print("key_sentence:{}".format(key_sentence), flush=True)
        return key_sentence


# 访问url
class UrlVisit:
    def __init__(self, url):
        self.url = url

    # get直接访问
    def get_url_data(self):
        res = requests.get(self.url)
        print(res.json())
        return res.json()


# 日志
class LogFile:
    def __init__(self, log_path, log_name):
        self.log_path = log_path
        self.log_name = log_name

    # 返回日志对象
    def get_logger(self, setLevel="info"):
        x = time.strftime("%Y%m%d%H%M", time.localtime())
        _logger = logging.getLogger(self.log_name)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)-8s:%(message)s")
        fh = logging.FileHandler(self.log_path + "{}_{}.log".format(self.log_name, x))
        fh.setFormatter(formatter)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        _logger.addHandler(fh)
        _logger.addHandler(ch)
        if setLevel == "info":
            _logger.setLevel(logging.INFO)
        elif setLevel == "warn":
            _logger.setLevel(logging.WARNING)
        elif setLevel == "error":
            _logger.setLevel(logging.ERROR)
        elif setLevel == "debug":
            _logger.setLevel(logging.DEBUG)
        return _logger


# 测试分词及词性的小接口
def fenci_with_flags_test(text):
    words, flags = TextDispose(text).jieba_cut_sentences_with_flags(with_flags=True)   # stopword_file=pm.STOP_WORD_PATH, _需要打开path_manager的引入，对于公用类模块，不应该引入某个工程下的路径包
    df_test = pd.DataFrame.from_dict({"words":words, "flags":flags})

    print(df_test)
    return df_test


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


# http标头
my_headers = [
        "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0"
]


# 请求网页
def get_knowledge_html_result(url):
    count = 10
    while count > 0:
        header_ = random.choice(my_headers)
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


if __name__ == '__main__':
    # test_content = "今日份的故宫 超级美 除了冻手没有一点毛病今年北京雾霾的治理真的是满分了第二次来故宫 依旧还是没有逛完那就下次再见啦还会再来的"
    # # fenci_with_flags_test(test_content)
    #
    # test_url = "http://192.168.0.52:9988/es.html"
    # UrlVisit(test_url).get_url_data()
    # pass

    # text = "@双井街派出所 @生活这一刻 @平安北京 @北京朝阳 不得不说的糟心事儿！住在垂杨柳北里39号楼！从去年十月份开始，基本晚上十一点左右，一点左右和凌晨四点左右，都有人高空抛物！目前这个棚子是上个月新修的，已经又是不少烟头，烟盒，纸格本等，对面的休息亭棚子上还有洗发液瓶子，之前有小盆友在下面玩的时候，还有人扔东西，索性没有砸中！真是不知道是哪一层的什么人扔的，物业目前也没有管！另外，窗口这么多线，如果高空抛下来的烟头烫到哪根线，都有可能引发火灾！还有就是二楼的电梯按钮一直是个窟窿，能看到电线！安全隐患存在也有快一年了，物业不知道是看不见还是不想管！希望问题能引起重视，要不是我家摄像头坏了，我都打算不睡觉，拿着监控去楼下蹲一晚上看看到底是什么人干出这事儿！"

    # print(TextDispose(text).get_key_sentences_of_text(['有限责任', '科技', '行业', '是否', '公司']))

    # from utils import path_manager
    # print(TimeDispose.get_first_version_date(path_manager.STABLE_SCORE_STORAGE, "version_gm.txt"))

    print(TimeDispose("2019-11-15").get_last_month_first_day())