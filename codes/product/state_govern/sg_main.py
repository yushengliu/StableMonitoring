#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/12/4 15:56
@Author  : Liu Yusheng
@File    : sg_main.py
@Description: 【国家治理】后台数据，总调度
"""
import utils.path_manager as pm
from utils.MyModule import TimeDispose
from product.state_govern.basic_data.get_traffic_avg_speed import traffic_monthly_speed_main
from product.state_govern.basic_data.get_leaf_score import get_leaf_score
from product.state_govern.frontend_related.upper_indexes import upper_indexes_main
from product.state_govern.frontend_related.map_color import map_color_main
from product.state_govern.basic_data.indexes_to_excel import excel_main

TEST_MODE = True


def sg_data_main(traffic=True, excel=True):
    newliest_version = TimeDispose.get_last_version_date(pm.STABLE_SCORE_STORAGE, "version.txt")
    # Step1. 先跑交通平均速度
    if traffic:
        traffic_monthly_speed_main(newliest_version)
    # Step2. 跑入叶子节点数据（到产品表）
    get_leaf_score(newliest_version, TEST_MODE)
    # Step3. 按照默认权重模板，计算出中间层/顶层节点的分数
    upper_indexes_main(TEST_MODE)
    # Step4. 写入地图染色数据（到产品表）
    map_color_main(TEST_MODE)
    # Step5. 写excel表
    if excel:
        excel_main(newliest_version, TEST_MODE)


def sychron_to_formal_db():
    newliest_version = TimeDispose.get_last_version_date(pm.STABLE_SCORE_STORAGE, "version.txt")
    # Step2. 跑入叶子节点数据（到产品表）
    get_leaf_score(newliest_version, False)
    # Step3. 按照默认权重模板，计算出中间层/顶层节点的分数
    upper_indexes_main(False)
    # Step4. 写入地图染色数据（到产品表）
    map_color_main(False)


if __name__ == "__main__":
    sg_data_main(traffic=False, excel=False)

    # sychron_to_formal_db()
