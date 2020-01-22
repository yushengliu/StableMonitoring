#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/10/30 15:36
@Author  : Liu Yusheng
@File    : scatter_areas_api.py
@Description: 和前端象限有关的接口
"""


def get_scatter_point_desc(data_rltpct, prefix):
    """
    @功能：返回每个点的描述信息
    :param data_rltpct:
    :param prefix:
    :return:
    """
    if data_rltpct >= 0:
        data_rltsign = "高于"
    else:
        data_rltsign = "低于"
        data_rltpct = -data_rltpct

    data_desc = "{}{}全国平均{:.2f}%".format(prefix, data_rltsign, data_rltpct)

    return data_desc


def judge_area(x, y, x_cthd, y_cthd, center_shape="rectangle"):
    """
    @功能：判断点属于哪个象限
    :param x:
    :param y:
    :return:
    """
    if (x > -x_cthd) & (x <= x_cthd) & (y > -y_cthd) & (y <= y_cthd):
        return "soso"
    else:
        if (x > 0) & (y > 0):
            return "struggle"
        elif (x > 0) & (y <= 0):
            return "contradict"
        elif (x <= 0) & (y <= 0):
            return "peace"
        else:
            return "lead"
