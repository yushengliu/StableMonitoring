#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/3/21 14:55
@Author  : Liu Yusheng
@File    : md_events.py
@Description: 实时扫描追踪库，若入参区县有事件发生，即返回（弹窗）
"""
import os
import pandas as pd
from copy import deepcopy
import psycopg2
import time
from multiprocessing import Pool

# from xmd_events_warning_data_generation_crontab import df_2861_county

trace_db_info = {
    "host": "39.108.127.36",
    "port": "5432",
    "user": "postgres",
    "pwd": "jiatao",
    "db_name": "public_sentiment",
}
base_events_sync_table = "base_events_sync_table"
trace_seed_table = "public_sentiment_trace_seed_table"
trace_detail_table = "public_sentiment_trace_detail_table"
trace_info_table = "public_sentiment_trace_info_table"

# int4  范围 LONG_MIN， LONG_MAX   （-2147483648    +2147483647）
# 当超出这个范围时会报出错误：org.postgresql.util.PSQLException: 错误: 整数超出范围

weight_parameters = {
    "K_weibo": 6,  # 计算影响力value值的参数
    "K_thumb": 1,
    "K_comment": 3,
    "K_share": 2,
    "value2man_times": 0.0019,
    "detail_batch": 12,  # 分批取详情，每批12件事
    "process_num": 20    # 开多进程取微博详情的进程数量
}

# 事件类型编码
event_type2codes_dict = {"stb": 1000, "env": 2000}


# 对Python自带的数据库的包
class DataBasePython:
    def __init__(self, host="192.168.0.117", user="readonly", pwd="123456", port="5555"):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port

    def select_data_from_db_one_by_one(self, db, sql):
        rows = []
        j = 10
        while j >= 0:
            try:
                conn = psycopg2.connect(dbname=db, user=self.user, password=self.pwd, host=self.host, port=self.port, client_encoding='utf-8', keepalives=1, keepalives_idle=20, keepalives_interval=20, keepalives_count=3, connect_timeout=10)
                cur = conn.cursor()
                cur.execute(sql)
                break
            except Exception as e:
                print(e)
                j -= 1

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

    def execute_any_sql(self, db, sql):
        try:
            conn = psycopg2.connect(dbname=db, user=self.user, host=self.host, password=self.pwd, port=self.port, client_encoding='utf-8', keepalives=1, keepalives_idle=20, keepalives_interval=20, keepalives_count=3, connect_timeout=10)
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print(db+':'+e)
        conn.close()
        return

    def select_data_from_db(self, db, sql):
        rows = []
        try:
            conn = psycopg2.connect(dbname=db, user=self.user, password=self.pwd, host=self.host, port=self.port, client_encoding='utf-8')
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            print(e)
        return rows


# 追踪库的对象
trace_db_obj = DataBasePython(host=trace_db_info["host"], user=trace_db_info["user"],
                              pwd=trace_db_info["pwd"], port=trace_db_info["port"])

# trace_db_obj = DataBaseFirm(server=database.get_database_server_by_nick(host=))


# 取事件基本信息 —— 入参区县，当前追踪+最近（默认）3天内追踪过的事件
def get_running_trace_seed_list_by_govs(gov_ids: list, recent_days: int, event_type: str, limit=None, tracing_only=False):
    """
    @功能：返回当前（+近期）追踪的事件概要信息
    :param gov_ids: 一组gov_id
    :param recent_days: 近期多少天
    :param event_type: 事件类型
    :param limit: 返回事件数限制
    trace_state: 常态：2-正在追踪，4-停止追踪；瞬态（过渡）：7/9-需要恢复追踪；0/1-事件等待第一次追踪；98-重复事件
    sync_time: 首次扫到事件的时间；spider_sync_time: 最近一次追踪的时间（每些一次追踪表和详情表，都会更新这一字段） —— 两者可能会有较大差异，原因是这一事件持续较长时间。
    :param tracing_only: True: 只返回当前预警模块正在追踪的事件；False: 追踪+停止追踪的事件，都返回
    :return:
    """
    time_ts0 = time.time()

    if tracing_only:
        sqlstr = "SELECT events_head_id, gov_id, gov_name, key_word_str, start_date, sync_time, search_cnt FROM %s WHERE gov_id IN (%s) AND start_date >= (current_date - interval'%dday') AND events_type = %d AND ((trace_state=4 AND spider_sync_time >=(current_date - interval'%dday')) OR trace_state in (2,7,9))  ORDER BY (gov_id, start_date) DESC"%(trace_seed_table, ",".join([str(int(i)) for i in gov_ids]), recent_days, event_type2codes_dict[event_type], min(3, recent_days))

    else:
        sqlstr = "SELECT events_head_id, gov_id, gov_name, key_word_str, start_date, sync_time, search_cnt FROM %s WHERE gov_id IN (%s) AND start_date >= (current_date - interval'%dday')  AND events_type = %d AND trace_state in (2, 4) ORDER BY (gov_id, start_date) DESC"%(trace_seed_table, ",".join([str(int(i)) for i in gov_ids]), recent_days, event_type2codes_dict[event_type])

    if limit:
        sqlstr = sqlstr + " LIMIT %d" % limit

    rows = trace_db_obj.select_data_from_db(trace_db_info["db_name"], sqlstr)

    time_ts1 = time.time()
    # print("取事件基础信息总耗时：{}".format(time_ts1 - time_ts0), flush=True)

    return rows


# 取多个事件最近一次的追踪状态
def get_events_trace_info_of_latest_trace(events_head_ids: list):
    """
    @功能：多个事件，每个取最新一次的追踪状态
    :param events_head_ids:
    :return:
    """

    time_ti0 = time.time()

    sqlstr = "SELECT a.events_head_id, a.gov_id, ({1}*a.data_num+{2}*a.count_read+{3}*a.count_comment+{4}*a.count_share)/{5} AS impact_man_times, a.search_cnt FROM {0} a RIGHT JOIN (SELECT events_head_id, MAX(do_time) AS do_time FROM {0} WHERE events_head_id IN ({6}) GROUP BY events_head_id) b ON (a.events_head_id = b.events_head_id AND a.do_time = b.do_time)".format(trace_info_table, weight_parameters["K_weibo"], weight_parameters["K_thumb"], weight_parameters["K_comment"], weight_parameters["K_share"], weight_parameters["value2man_times"], "\'"+"\',\'".join(events_head_ids)+"\'")

    rows = trace_db_obj.select_data_from_db(trace_db_info["db_name"], sqlstr)

    time_ti1 = time.time()
    # print("查询多个事件追踪状态耗时：{}".format(time_ti1-time_ti0), flush=True)

    return rows


# 取多件事的最新追踪周期的全量微博详情 —— 仅取回
def get_events_all_detail_weibos_of_latest_trace(events_head_ids: list):
    """
    @功能：返回入参事件的最新追踪周期的全量微博详情
    :param events_head_ids:
    :return:
    """
    time_td0 = time.time()

    sqlstr = "SELECT * FROM (SELECT a.gov_id, a.events_head_id, a.url, a.pub_time, a.content, {1}*a.count_read+{2}*a.count_comment+{3}*a.count_share AS weibo_value FROM {0} a RIGHT JOIN (SELECT events_head_id, MAX(search_cnt) AS search_cnt FROM {5} WHERE events_head_id in ({4}) GROUP BY events_head_id) b ON (a.events_head_id = b.events_head_id AND a.search_cnt = b.search_cnt) WHERE a.content != '') c ORDER BY (c.events_head_id, c.weibo_value) DESC;".format(
        trace_detail_table, weight_parameters["K_thumb"], weight_parameters["K_comment"], weight_parameters["K_share"],
        ", ".join(["\'" + i + "\'" for i in events_head_ids]), trace_info_table)

    rows = trace_db_obj.select_data_from_db(trace_db_info["db_name"], sqlstr)

    time_td1 = time.time()

    # print("查询多个事件最新追踪态的全量微博详情耗时：{}    当前进程：{}".format(time_td1-time_td0, os.getpid()), flush=True)

    return rows


def get_events_data_by_gov_ids(gov_ids, event_type="stb", days=3, events_count=None, tracing_only=False, order_type="time_desc"):
    """
    @功能：返回入参多个区县近期发生的事件详情
    :param gov_ids:
    :param event_type: "stb": 稳定   "env": 环境
    :param days: 最近多少天
    :param events_count: 限定各区县取多少件事儿
    :param tracing_only: True: 只返回追踪中的事件（时效性更强，预警模块中可查看监控曲线）；False: 都返回，但停止追踪的不能查看监控曲线
    :param order_type: "impact_desc": 影响力倒序   "time_desc": 时间倒序
    :return: govs_events_info = {gov_id: [{"title":标题, "occur_time":发生时间, "weibo_content":微博内容, "url": 微博链接, "impact_man_times": （事件）总传播覆盖人次 = （6*weibo_num + count_read + 3*count_comment + 2*count_share）/ 1.9‰ }]}
    """

    # 多个gov_id
    running_seed_list = get_running_trace_seed_list_by_govs(gov_ids, days, event_type, events_count, tracing_only)

    # 基础信息表
    df_basic_events = pd.DataFrame(running_seed_list, columns=["events_head_id", "gov_id", "gov_name", "key_word_str", "start_time", "sync_time", "search_cnt"])

    # 对 events_head_id 去重/聚合 —— 重复的 events_head_id 取 sync_time （扫描到事件的时间）最大的那个
    df_basic_events_part = deepcopy(df_basic_events[["events_head_id", "sync_time"]])
    df_basic_events_part = df_basic_events_part.groupby(["events_head_id"]).agg({"sync_time": "max"}).reset_index()
    df_basic_events = df_basic_events.join(df_basic_events_part.set_index(["events_head_id", "sync_time"]),
                                           on=["events_head_id", "sync_time"], how="inner").reset_index(drop=True)

    # 仍有重复的就直接drop_duplicate
    df_basic_events = df_basic_events.drop_duplicates("events_head_id", 'first')
    df_basic_events = df_basic_events.set_index("events_head_id")

    events_head_ids = df_basic_events.index.values.tolist()

    # 数据库聚类输出所有事件最新的追踪状态
    trace_info_list = get_events_trace_info_of_latest_trace(events_head_ids)
    df_trace_info = pd.DataFrame(trace_info_list, columns=["events_head_id", "gov_id", "impact_man_times", "search_cnt"])

    # 再次去重
    df_trace_info = df_trace_info.drop_duplicates("events_head_id", "first")
    df_trace_info = df_trace_info.set_index("events_head_id")

    # print(df_trace_info, flush=True)
    events_head_ids = df_trace_info.index.values.tolist()   # [:1000]

    # 取微博详情表，先多进程分批次取最新一次追踪状态下所有微博详情回来，再在pandas里提取影响力值最高的微博及信息，供前端展示
    time_tb0 = time.time()
    batch_list = []
    for i in range(0, len(events_head_ids), weight_parameters["detail_batch"]):
        batch_list.append(events_head_ids[i: i+weight_parameters["detail_batch"]])

    # 多进程
    with Pool(weight_parameters["process_num"]) as p:
        results = p.map(get_events_all_detail_weibos_of_latest_trace, batch_list)

    trace_detail_list = sum(results, [])

    time_tb1 = time.time()
    # print("取回多个事件最新追踪态的全量微博-分批次-多进程，总耗时：{}    总事件数：{}    单批次事件数：{}".format(time_tb1 - time_tb0, len(events_head_ids), weight_parameters["detail_batch"]), flush=True)

    time_m0 = time.time()

    df_trace_detail = pd.DataFrame(trace_detail_list, columns=["gov_id", "events_head_id", "url", "pub_time", "content", "weibo_value"])

    # 对每个事件取最大影响力值的微博信息
    df_trace_detail_part = deepcopy(df_trace_detail[["events_head_id", "weibo_value"]])
    df_trace_detail_part = df_trace_detail_part.groupby(["events_head_id"]).agg({"weibo_value": "max"}).reset_index()
    df_trace_detail = df_trace_detail.join(df_trace_detail_part.set_index(["events_head_id", "weibo_value"]), on=["events_head_id", "weibo_value"], how="inner").reset_index(drop=True)

    # 再次去重 保险
    df_trace_detail = df_trace_detail.drop_duplicates("events_head_id", "first")
    df_trace_detail = df_trace_detail.set_index("events_head_id")

    # 根据最终拿出的详情表，对基础表做一次无效事件删减
    df_basic_events = df_basic_events.join(df_trace_detail["pub_time"], how="inner")

    time_m1 = time.time()

    # print("全量取回最近一次追踪周期的微博后，pandas筛选影响力最大的微博，耗时：{}".format(time_m1-time_m0), flush=True)

    # 数据返回格式
    govs_events_info = dict()
    for gov_id in gov_ids:
        govs_events_info[gov_id] = list()

        # 当前区县的有效事件
        # df_trace_detail_gov = deepcopy(df_trace_detail[df_trace_detail.gov_id == gov_id])
        df_basic_events_gov = deepcopy(df_basic_events[df_basic_events.gov_id == gov_id])

        # 如果没有事件，跳到下一个区县
        if not df_basic_events_gov.shape[0]:
            continue

        for events_head_id, row in df_basic_events_gov.iterrows():

            event_info = dict()

            event_info["title"] = row["key_word_str"].replace("and", " ").replace("~", " ")

            # 首发时间
            event_info["occur_time"] = row["start_time"].strftime('%Y-%m-%d %H:%M:%S')

            # 事件当前影响人次
            event_info["impact_man_times"] = int(df_trace_info.loc[events_head_id, "impact_man_times"])

            # 微博内容
            event_info["weibo_content"] = df_trace_detail.loc[events_head_id, "content"]

            # url
            event_info["url"] = df_trace_detail.loc[events_head_id, "url"]

            govs_events_info[gov_id].append(event_info)

        # 影响力倒序输出
        if order_type == "impact_desc" and len(govs_events_info[gov_id]):

            df_gov_events_info = pd.DataFrame(govs_events_info[gov_id])

            df_gov_events_info = df_gov_events_info.sort_values(by="impact_man_times", ascending=False)

            govs_events_info[gov_id] = df_gov_events_info.to_dict(orient="records")

            # 指定了输出数量
            if events_count:

                govs_events_info[gov_id] = govs_events_info[gov_id][:events_count]

        print("****** gov_id: %d  recent_days: %d   events_num: %d ******" % (gov_id, days, len(govs_events_info[gov_id])), flush=True)

    time_m2 = time.time()

    # print("调整成返回格式耗时：{}".format(time_m2 - time_m1), flush=True)

    return govs_events_info


if __name__ == "__main__":
    # 测试
    if 1:
        gov_ids = [2, 3, 4, 5, 6, 7, 8, 9]
        gov_ids = [4]
        # print("$$$$$ time_desc: $$$$$\n", get_events_data_by_gov_ids(gov_ids))
        print("$$$$$ impact_desc: $$$$$\n", get_events_data_by_gov_ids(gov_ids, days=30, order_type="impact_desc"))






