#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time   :   2020/1/14 18:00
@Author :   Liu Yusheng
@File   :   gov_modern_auto.py
@Description   :    国家治理现代化（象限图）-总调度
"""
from datetime import datetime, date

from utils.MyModule import TimeDispose, LogFile
from utils import path_manager as pm

from product.gov_modern.basic_data.mark_idx_type import mark_idxs_main
from product.gov_modern.basic_data.get_stats_n_score import get_data_main
from product.gov_modern.basic_data.parameters import UNIFORM_DISPOSAL_INDEXES
from product.gov_modern.frontend_related.get_frontend_data import get_map_data, get_tree_data

logger = LogFile(pm.LOCAL_LOG_FILE_DIR, "gov_modern").get_logger()


def gov_modern_main(version="", update_client=True):
    logger.info("【%s开始运行】" % "gov_modern_main")

    data_path = pm.STABLE_SCORE_STORAGE
    if version:
        start_time = TimeDispose(version).get_this_month_first_day()
        end_time = TimeDispose(version).get_next_month_first_day()
    else:
        today_ = datetime.now().date()
        today_obj_ = TimeDispose(today_)
        start_time = today_obj_.get_last_month_first_day()
        end_time = today_obj_.get_this_month_first_day()

    aim_v = str(TimeDispose(end_time).get_ndays_ago(1).date())

    # 把事件库新增事件，都标上国家治理现代化的指标
    update_num = mark_idxs_main()
    logger.info(f"mark_idx_main - 更新事件库指标完毕, 共计:{update_num}条")
    # 基于本周期，计分
    for idx_type in UNIFORM_DISPOSAL_INDEXES:
        get_data_main(aim_v, idx_type)
        logger.info(f"get_data_main - 计分完成， idx_type={idx_type}")
    # 更新version_gm.txt
    TimeDispose.update_version(data_path, "version_gm.txt", aim_v)
    # 产品前端数据写库
    if update_client:
        get_map_data()
        logger.info(f"get_map_data - 更新map_color完成， idx_type=mong")
        for idx_type in UNIFORM_DISPOSAL_INDEXES:
            get_tree_data(idx_type)
            logger.info(f"get_tree_data - 更新tree_data完成，idx_type={idx_type}")

    logger.info("【%s运行结束】" % "gov_modern_main")


if __name__ == "__main__":
    gov_modern_main()