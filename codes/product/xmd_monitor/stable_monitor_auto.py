#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/2/18 10:44
@Author  : Liu Yusheng
@File    : stable_monitor_auto.py
@Description: 自动生成每月的稳定监测数据
"""

import sys
import logging.handlers

from datetime import datetime

from product.xmd_monitor.modules import stable_score_store, stable_client_score,\
    readjust_stable_score_with_other_elements, stable_calculate_score, stable_client_page

from utils import path_manager as pm
from utils import parameters as para
from utils.MyModule import TimeDispose

# 事件信息库
event_info_obj = para.event_db_obj
event_table = para.event_table

monthly_events_thd = 9000

# logger对象
logger = logging.getLogger("stable_monitor")
# 日志输出格式
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)-8s:%(message)s")

# handler —— 输出到文件和控制台
log_path = pm.LOCAL_LOG_FILE_DIR
# 文件
fh = logging.FileHandler(log_path+"stable_monitor.log")
fh.setFormatter(formatter)
# 控制台
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

logger.setLevel(logging.INFO)

ftp_flag = True


def stable_monitor_main(provinces, last_version=""):
    logger.info("【%s开始运行】" % "stable_monitor_main")

    data_path = pm.STABLE_SCORE_STORAGE
    last_version = last_version if last_version else TimeDispose.get_last_version_date(data_path, "version.txt")
    if last_version is not None:
        # 版本信息一般是每个月的最后一天，如果要跑新月度数据，需要判断当前时间已经超过新月份的最后一天（也即新月份的下一月的第一天）——这样才能有完整的新月度基础数据
        start_time = TimeDispose(last_version).get_next_month_first_day()
        end_time = TimeDispose(start_time).get_next_month_first_day()

        if end_time <= str(datetime.now()).split(' ')[0]:
            # 计算之前要容错 —— 检查周期内是否有事件信息的数据
            sqlstr = "SELECT * from %s where event_start_time >= '%s' and event_start_time < '%s' and event_type in ('std', 'stb');"%(event_table, start_time, end_time)

            event_info_obj.get_conn()

            rows = event_info_obj.read_from_table(sqlstr)

            event_info_obj.disconnect()

            stable_calculate_score.stable_score_main(start_time, end_time)

            # 如果新周期内没有【足够】的事件信息，则打印在日志里，便于回查（不终止计算程序）
            if len(rows) <= monthly_events_thd:
                logger.warning("[%s ~ %s] 新周期内不稳定事件数仅：%d件；非正常，过少，无法评分，请检查数据源情况。"%(start_time, end_time, len(rows)))

            # 评分入库 —— 数据库和es，供其他板块调用
            version_date = str(TimeDispose(end_time).get_ndays_ago(1)).split(' ')[0]
            stable_score_store.stable_score_store_main(version_date)
            readjust_stable_score_with_other_elements.readjust_main(version_date)

            # 生成前端文件
            stable_client_page.generate_datafile(provinces)

            if ftp_flag:
                stable_client_page.upload_web_details()
                stable_client_score.stable_client_score_main(provinces)


        else:
            logger.warning("统计周期超出当前时间，请下月1号之后再运行本程序。")
    else:
        logger.error("%s 不存在!!!" % (data_path + "version.txt"))
    logger.info("【%s运行结束】\n" % "stable_monitor_main")

    return


if __name__ == "__main__":
    provinces = [
        '11', '12', '13', '14', '15',
        '21', '22', '23',
        '31', '32', '33', '34', '35', '36', '37',
        '41', '42', '43', '44', '45', '46',
        '50', '51', '52', '53', '54',
        '61', '62', '63', '64', '65'
    ]

    # last_v = "2019-11-30"      # 各版本为当月月底，更新12月版本（12-31）时，上一版本（last_v）= 2019-11-30
    stable_monitor_main(provinces)   # , last_version=last_v