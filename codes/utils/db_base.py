#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/10/29 10:43
@Author  : Liu Yusheng
@File    : db_base.py
@Description: 数据库信息
"""
from db_interface import database
from utils.MyModule import DataBaseFirm


class TableName:
    LawScore = "law_score"
    GovModernScore = "gov_modern_score"
    EventBasic = "xmd_event_basic_info"
    PWMapColor = "pw_map_color"
    PWTreeData = "pw_tree_data"
    PWAngle = "pw_angle"
    PWTreeFrame = "pw_tree_frame"
    GovIndex = "gov_idx"
    CbdGovScore = "cbd_gov_score"


class DBShortName:
    EventBasicInfo = "event_info"
    ProductTest = "product_test"
    ProductFormal = "product_formal"
    ProductPWFormal = "product_pw_formal"    # 草原在247上建的pw相关的表，用的账号和默认账号不一致
    ProductPWDataTest = "product_data_test"
    ProductPWDataFormal = "product_data_formal"
    GovIndexLocal = "gov_index_local"
    GovIndexFormal = "gov_index_formal"
    CbdDataTest = "cbd_test"    # 环境、稳定数据需要写cbd数据库
    CbdDataFormal = "cbd_formal"



class DBObj:
    def __init__(self, db_short):
        self.db_short = db_short
        if self.db_short == DBShortName.EventBasicInfo:
            self.server = database.create_user_defined_database_server(host="192.168.0.133", port="6500", user="etherpad", pwd="123456")
            self.db = "text-mining"
            self.table = TableName.EventBasic

        elif self.db_short == DBShortName.ProductPWDataTest:
            self.server = database.create_user_defined_database_server(host="192.168.0.46", port="5432", user="postgres", pwd="pg123456")
            self.db = "zk_pw"
            self.table = TableName.LawScore

        elif self.db_short == DBShortName.ProductPWDataFormal:
            self.server = database.create_user_defined_database_server(host="47.112.126.83", port="5432", user="zkpw", pwd="zkpw123!")
            self.db = "zk_pw"
            self.table = TableName.LawScore

        elif self.db_short == DBShortName.ProductTest:
            self.server = database.create_user_defined_database_server(host="192.168.0.133", port="5434", user="postgres", pwd="123456")
            self.db = "product"
            self.table = TableName.PWMapColor

        elif self.db_short == DBShortName.ProductFormal:
            self.server = database.get_database_server_by_nick(database.SERVER_PRODUCT)
            self.db = "product"
            self.table = TableName.PWMapColor

        elif self.db_short == DBShortName.ProductPWFormal:
            self.server = database.create_user_defined_database_server(host="120.78.222.247", port="5432", user="product_user", pwd="zhiku_product")
            self.db = "product"
            self.table = TableName.PWMapColor

        elif self.db_short == DBShortName.GovIndexLocal:
            self.server = database.create_user_defined_database_server(host="192.168.0.88", port="5432", user="postgres", pwd="zk2861@*^!")
            self.db = "zk_gov_index"
            self.table = TableName.GovIndex

        elif self.db_short == DBShortName.GovIndexFormal:
            self.server = database.create_user_defined_database_server(host="47.112.126.83", port="5432", user="govidx", pwd="zkgov2861")
            self.db = "zk_gov_index"
            self.table = TableName.GovIndex

        # cbd
        elif self.db_short == DBShortName.CbdDataTest:
            self.server = database.create_user_defined_database_server(host="192.168.0.46", port="5432", user="postgres", pwd="pg123456")
            self.db = "zk_location"
            self.table = TableName.CbdGovScore

        elif self.db_short == DBShortName.CbdDataFormal:
            self.server = database.create_user_defined_database_server(host="47.112.126.83", port="5432",user="zkfdc", pwd="zK2861")
            self.db = "zk_location"
            self.table = TableName.CbdGovScore

        else:
            self.server = None
            self.db = ""
            self.table = ""

        self.obj = DataBaseFirm(server=self.server, db=self.db, table=self.table)


