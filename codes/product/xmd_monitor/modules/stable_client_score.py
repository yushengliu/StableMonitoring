#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/1/7 14:56
@Author  : Liu Yusheng
@File    : stable_client_score.py
@Description: 更新客户端分数
"""
import logging
import json
import pandas as pd
import numpy as np

from utils import utilities
from utils import path_manager as pm
from utils import parameters as para
from utils.MyModule import TimeDispose
from product.xmd_monitor.modules import stable_client_page

logger = logging.getLogger("stable_main.client_score")

df_node = pd.read_csv(pm.CONFIG_NODE, dtype="str", encoding="utf-8")
df_2861_county = para.df_2861_gaode_geo


# 客观得分给个统一的基础分 —— 100
def base_stable(sync_mode='DB', node_dataframe=None):
    if sync_mode == 'DB':
        last_version = TimeDispose.get_last_version_date(pm.STABLE_SCORE_STORAGE, "version.txt")
        data_file = pm.STABLE_SCORE_STORAGE + last_version + '/' + para.stable_monthly_score_csv

        print("file_date=%s" % last_version)

        data_stable = pd.read_csv(data_file, index_col="gov_code", encoding="utf-8")

        nrows = len(df_2861_county.index)
        ncols = len(["grade"])
        np_array = np.zeros((nrows, ncols), dtype=float)

        final_stable_df = pd.DataFrame(np_array, index=df_2861_county.index, columns=["grade"])

        # final_stable_df["grade"] = [100] * final_stable_df.shape[0]

        value_max = data_stable["stable_grade"].max()
        value_min = data_stable["stable_grade"].min()
        delta = value_max - value_min

        # 20190114 分数归一化，保持和舆情一致
        for gov_code in data_stable.index:
            # final_stable_df.loc[gov_code, "grade"] = round((100 + 100 - data_stable.loc[gov_code, "stable_grade"])/2, 2)

            # 最低50分太低了，提高一点下限，减小稳定舆论对新矛盾总评分的影响力度 —— 70~100  _已经在数据端改过了
            final_stable_df.loc[gov_code, "grade"] = data_stable.loc[gov_code, "stable_grade"]

        conn = utilities.init_XMD_DB_CONN(True)
        BASE = 1
        version = 1
        value_dict = final_stable_df["grade"].to_dict()

        utilities.insert_node_values(conn, 'TOP_STABLE', json.dumps(str(value_dict)), last_version, value_type=BASE, version=version, submitter='STABLE')

        conn.disconnect()

        return node_dataframe, last_version, 'TOP_STABLE'


# 舆情得分用 100 - 不稳定风险指数，相当于不稳定风险指数越高，稳定角的舆情得分越低
def opinion_stable(sync_mode='DB', node_dataframe=None):
    if sync_mode == "DB":
        last_version = TimeDispose.get_last_version_date(pm.STABLE_SCORE_STORAGE, "version.txt")
        data_file = pm.STABLE_SCORE_STORAGE + last_version + '/' + para.stable_monthly_score_csv

        print("file_date=%s" % last_version)

        data_stable = pd.read_csv(data_file, index_col="gov_code", encoding="utf-8")

        nrows = len(df_2861_county.index)
        ncols = len(["grade"])
        np_array = np.zeros((nrows, ncols), dtype=float)
        final_stable_df = pd.DataFrame(np_array, index=df_2861_county.index, columns=["grade"])

        value_max = data_stable["stable_grade"].max()
        value_min = data_stable["stable_grade"].min()
        delta = value_max - value_min

        for gov_code in data_stable.index:
            # final_stable_df.loc[gov_code, "grade"] = 100 - data_stable.loc[gov_code, "stable_grade"]
            # final_stable_df.loc[gov_code, "grade"] = round((100 + 100 - data_stable.loc[gov_code, "stable_grade"])/2, 2)

            # 最低50分太低了，提高一点下限，减小稳定舆论对新矛盾总评分的影响力度 —— 60~100
            final_stable_df.loc[gov_code, "grade"] = data_stable.loc[gov_code, "stable_grade"]

        conn = utilities.init_XMD_DB_CONN(True)
        OPINION = 2
        version = 1
        df_route = final_stable_df["grade"]
        value_dict = df_route.to_dict()
        utilities.insert_node_values(conn, 'TOP_STABLE', json.dumps(str(value_dict)), last_version, value_type=OPINION, version=version, submitter="STABLE")
        conn.disconnect()

        return node_dataframe, last_version, 'TOP_STABLE'


# 边角更新分数及描述
def stable_client_score_main(provinces):
    last_version = TimeDispose.get_last_version_date(pm.STABLE_SCORE_STORAGE, "version.txt")

    base_stable()
    opinion_stable()

    logger.info("[已完成前端稳定总角分数更新] 版本：%s"%last_version)

    stable_client_page.corner_description_db(provinces)

    logger.info("[已完成前端稳定各分角描述更新] 版本：%s"%last_version)

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

    if 1:
        stable_client_score_main(provinces)