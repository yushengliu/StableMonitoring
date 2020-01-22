#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/11/18 10:38
@Author  : Liu Yusheng
@File    : mark_idx_type.py
@Description: 给热点打上指标类型的标签
"""
import re
import sys
import json
import time
import pandas as pd
from copy import deepcopy
from datetime import datetime

from utils.parameters import event_db_obj, event_table
from utils.MyModule import CJsonEncoder

from product.gov_modern.basic_data.parameters import UNIFORM_DISPOSAL_INDEXES, KeyWords, BaiduCates

TEST_LIMIT = 1000
PROCESS_BATCH = 10000


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


def judge_idx_type_with_keywords_n_cates(data_dict: dict, idx_type: str)->bool:
    """
    @功能：按照逻辑判断是否打该指标
    :param data_dict: {"content":"...", "more_info": {"category":..., "sub_cates": [{"tag":"...", "score":...}]}
    :param idx_type:
    :return:
    """
    if idx_type not in UNIFORM_DISPOSAL_INDEXES:
        print("idx_type={}  该项指标不能用百度类别+关键字的方式匹配判断，请检查入参".format(idx_type), flush=True)
        sys.exit(100)

    else:
        more_info_dict = data_dict["more_info"]

        category = more_info_dict["category"]
        sub_cates_info = more_info_dict["sub_cates"]

        # 如果子类为空
        if not len(sub_cates_info):
            sub_cates = []
        else:
            sub_cates = pd.DataFrame(sub_cates_info)["tag"].tolist()

        # Step1. 先判断大类
        idx_destined_cates = BaiduCates.cates_dict[idx_type]

        if not len(idx_destined_cates):   # 目标大类为空列表，则表示不作类别筛选
            match_cate = True
        else:
            match_cate = False

            for cate_info in idx_destined_cates:
                destined_cate = cate_info["cate"]
                destined_subs = cate_info["subs"]

                if (destined_cate == "") and (len(destined_subs) == 0):   # 目标指标的大类和子类都配空，是错误的！！！要么就整体配空列表
                    print("idx_type={}  destined_cate_info={}  类别配置文件错误，请检查！".format(idx_type, cate_info), flush=True)
                    sys.exit(123)

                else:
                    if len(destined_subs):
                        # 有子类要求时，则只有当：① 当前事件所属子类和目标子类有交集 ② 大类无要求/大类一致   同时满足，则可标记
                        sub_inter = list(set(sub_cates).intersection(set(destined_subs)))
                        if len(sub_inter) and ((destined_cate == "") or (category == destined_cate)):
                            match_cate = True
                    else:
                        # 无子类要求时，只看大类是否匹配
                        if category == destined_cate:
                            match_cate = True

                    if match_cate:
                        break

        # Step2. 再判断关键字匹配
        idx_keywords = KeyWords.kwds_dict[idx_type]
        content = data_dict["content"]

        if match_cate and re.search(idx_keywords, content):
            return True
        else:
            return False


def get_event_title_str(org_event_title):
    """
    @功能：把数据库原始存的event_title(eg:[aa, bb, cc])，转换为拼接字符串的形式（aabbcc)
    :param org_event_title:
    :return:
    """

    org_event_title = eval(org_event_title) if isinstance(org_event_title, str) else org_event_title
    title_str = " ".join(org_event_title)

    return title_str


def dispose_more_info_mong_idx(row_x: pd.Series, idx_type: str)->dict:
    """
    @功能：针对某个指标类型idx_type，匹配后处理more_info中的mong字段
    :param row_x: 表里的某一行
    :param idx_type: 指标类别
    :return:
    """
    org_more_info = row_x["more_info"]
    final_more_info = deepcopy(org_more_info)
    final_more_info["mong"] = [] if "mong" not in org_more_info.keys() else list(set(org_more_info["mong"])-{idx_type})  # 排除之前的标签

    if idx_type in UNIFORM_DISPOSAL_INDEXES:
        mark_sign = judge_idx_type_with_keywords_n_cates(data_dict={"content": row_x["first_content"] if len(row_x["first_content"]) else row_x["event_title"], "more_info": row_x["more_info"]}, idx_type=idx_type)
    else:
        # 其他方式，暂缓
        mark_sign = False

    if mark_sign:
        final_more_info["mong"].append(idx_type)

    return final_more_info


def get_events_data_from_db(mark_by, limit=None):
    """
    @功能：从数据库获取原始数据
    :param mark_by:
    "update": 更新所有指标（不含mong字段的数据）;
    "renew"：重跑（所有数据）
    :param limit:
    :return:
    """
    if mark_by == "update":
        sqlstr_ = "SELECT event_id, event_title, first_content, more_info FROM {} WHERE more_info is not NULL AND more_info->>'mong' is NULL ORDER BY id ASC".format(event_table)
    else:
        sqlstr_ = "SELECT event_id, event_title, first_content, more_info FROM {} WHERE more_info is not NULL ORDER BY id ASC".format(event_table)

    if limit is not None:
        sqlstr_ += " LIMIT %d"%limit

    datas = event_db_obj.read_from_table(sqlstr_)

    return datas


def mark_idxs_main(idx_types: list=UNIFORM_DISPOSAL_INDEXES, mark_by: str= "update", test_mode: bool=False):
    """
    @功能：判断并标记，各种指标 —— 每种指标都默认重新标注
    :param idx_types:
    :param mark_by:
    "update": 更新所有指标（不含mong字段的数据）;
    "renew"：重跑（所有数据）
    :param test_mode:
    :return: 更新数据库
    """

    start_time = time.time()

    event_db_obj.get_conn()

    if test_mode:
        data_num = TEST_LIMIT
    else:
        data_num = None

    events_data = get_events_data_from_db(mark_by, limit=data_num)

    df_events = pd.DataFrame(events_data)

    if not df_events.shape[0]:
        print("没有待更新的数据！", flush=True)
        return 0

    df_events["event_title"] = df_events["event_title"].apply(lambda x: get_event_title_str(x))
    df_events["first_content"] = df_events["first_content"].apply(lambda x: x.encode('gbk','ignore').decode('gbk'))

    print("{} - 取数据并预处理完毕！耗时：{}".format(str(datetime.now()), time.time() - start_time), flush=True)

    for line in range(0, df_events.shape[0], PROCESS_BATCH):
        data_df = deepcopy(df_events.iloc[line: line+PROCESS_BATCH])

        mark_start = time.time()
        for idx_type_ in idx_types:
            data_df["more_info"] = data_df.apply(lambda x: dispose_more_info_mong_idx(x, idx_type_), axis=1)

        print("{} - batch={}：指标判断完毕！耗时：{}".format(str(datetime.now()), int(line/PROCESS_BATCH +1), time.time() - mark_start), flush=True)

        update_db_start = time.time()
        update_db_more_info(data_df)
        print("{} - batch={}：更新数据完毕！耗时：{}".format(str(datetime.now()), int(line/PROCESS_BATCH +1), time.time() - update_db_start), flush=True)

    event_db_obj.disconnect()
    print("{} - 全程耗时：{}".format(str(datetime.now()), time.time() - start_time), flush=True)
    return df_events.shape[0]


if __name__ == "__main__":
    mark_idxs_main(UNIFORM_DISPOSAL_INDEXES, "renew", test_mode=False)


