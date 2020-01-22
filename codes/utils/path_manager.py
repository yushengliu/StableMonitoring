# !/usr/bin/python
# -*- coding: utf-8 -*-

import os

# 工程文件目录
PROJECT_DIRNAME = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
LOCAL_STABLE_PROJECT_STORAGE = PROJECT_DIRNAME + "/" + "codes" + "/"

# 数据文件目录
LOCAL_STABLE_DATA_STORAGE = PROJECT_DIRNAME + "/" + "data" + "/"
STABLE_SCORE_STORAGE = LOCAL_STABLE_DATA_STORAGE + "scored_origin_result/"
STABLE_CLIENT_FILE_STORAGE = LOCAL_STABLE_DATA_STORAGE + "client_data/"

# 客户端数据文件目录
LOCAL_STABLE_CLIENT_DATA_STORAGE = PROJECT_DIRNAME + "/" + "client" + "/"


# 日志文件目录
LOCAL_LOG_FILE_DIR = LOCAL_STABLE_PROJECT_STORAGE + 'log/'

FTP_IP_ADDRESS = '192.168.0.133'
ALI_FTP_IP_ADDRESS = '120.79.142.157'
WEB_PICTURE_SERVER = 'http://120.79.142.157:12000/'
PUBLIC_WEB_APPS_SERVER = 'http://120.79.142.157:8888/'
PRIVATE_WEB_APPS_SERVER = 'http://192.168.0.52:8888/'
PUBLIC_WEB_APPS_API_PREFIX = PUBLIC_WEB_APPS_SERVER + 'zk2861/story/'
PRIVATE_WEB_APPS_API_PREFIX = PRIVATE_WEB_APPS_SERVER + 'zk2861/story/'

# ftp 前端数据文件目录
FTP_CLIENT_DATA_STORAGE = './ui_client_data/'

# 前端需要的数据文件
RELATED_FILES = LOCAL_STABLE_PROJECT_STORAGE + 'related_files/'
UTILS_PATH = LOCAL_STABLE_PROJECT_STORAGE + "utils/"
# GAODE_COUNTY_LIST = RELATED_FILES + 'df_2861_gaode_geo_new.csv'

# 20191129 - 对齐最新版dict_gov_lib_online后整合的数据 // 所有行政规划信息保持和dict_gov_lib_online一致，此外修正了几个无下辖区县地级市- gov_id in [2058, 2059, 2212, 2213, 2927]，gov_type由1改为31
GAODE_COUNTY_LIST = RELATED_FILES + 'df_2861_gaode_geo_new_20191129.csv'

GAODE_COUNTY_LIST_XMD = RELATED_FILES + 'df_2861_gaode_geo_new.csv'

# STRUCURE CONFIG
CONFIG_NODE = RELATED_FILES + 'node_level_parameters_v10.csv'
# # 知识库meta
META_PATH = RELATED_FILES + 'stable_knowledgemeta.csv'

CLASS_URL = "http://192.168.0.88:8882/env/?"

STABLE_MONTHLY = LOCAL_STABLE_DATA_STORAGE+'client_data/STABLE_SCORE_FINAL_MONTHLY.csv'
STABLE_YEARLY = LOCAL_STABLE_DATA_STORAGE+'client_data/STABLE_SCORE_FINAL_YEARLY.csv'

STABLE_FINAL_MONTHLY_GRADE_CSV = "STABLE_SCORE_FINAL.csv"

grade_cols = ["stable_value", "stable_grade"]

STABLE_MONTHLY_EVENTS_STATS_CSV = "STABLE_EVENTS_STATS.csv"

stats_cols = ["event_count", "event_count_weibo", "event_count_comment", "event_count_share", "event_count_read"]


def init_directories():
    if not os.path.exists(LOCAL_STABLE_PROJECT_STORAGE):
        os.makedirs(LOCAL_STABLE_PROJECT_STORAGE)
        print('create %s.' % LOCAL_STABLE_PROJECT_STORAGE)

    if not os.path.exists(LOCAL_STABLE_DATA_STORAGE):
        os.makedirs(LOCAL_STABLE_DATA_STORAGE)
        print('create %s.' % LOCAL_STABLE_DATA_STORAGE)

    if not os.path.exists(LOCAL_STABLE_CLIENT_DATA_STORAGE):
        os.makedirs(LOCAL_STABLE_CLIENT_DATA_STORAGE)
        print('create %s.' % LOCAL_STABLE_CLIENT_DATA_STORAGE)

    if not os.path.exists(LOCAL_LOG_FILE_DIR):
        os.makedirs(LOCAL_LOG_FILE_DIR)
        print('create %s.' % LOCAL_LOG_FILE_DIR)

    return


if __name__ == "__main__":
    print('path_manager.py')
    init_directories()