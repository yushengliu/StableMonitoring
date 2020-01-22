#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/11/20 19:50
@Author  : Liu Yusheng
@File    : parameters.py
@Description: 前端相关参数配置
"""

AREAS_SHORT_APPLY = ["struggle", "contradict", "soso", "peace", "lead"]   # 应用侧的顺序
AREAS_LONG = ["对立矛盾", "民意突出", "相对安宁", "有效引导", "一般态"]
AREAS_NAMES_DICT = {"struggle": "对立矛盾", "contradict": "民意突出", "peace": "相对安宁", "lead": "有效引导", "soso": "一般态"}

AREAS_COLORS_DICT = {"struggle": "RGB(255,117,117)", "contradict": "RGB(255,220,116)", "peace": "RGB(220,220,220)", "lead": "RGB(152,218,232)", "soso": "RGB(26,179,157)"}

AREAS_COLOR_VALUES_DICT = {key: int(100/len(AREAS_SHORT_APPLY))*AREAS_SHORT_APPLY.index(key) for key in AREAS_SHORT_APPLY}


SP_DISPOSAL_INDEXES = ["economics", "elite_get", "fair_protect"]
UNIFORM_DISPOSAL_INDEXES = ["politics", "law", "culture", "env", "party", "resource_get", "law_put", "supervise_put", "scientific_decide", "crisis_respond", "politics_renew", "system_build", "accept_put", "politics_integrate", "strategy_plan", "politics_communicate"]


# 控制参数
class DataParas:
    CENTER_THD = 45   # 中间区域的x，y门限都设为45
    Y_CTHD = 45
    X_CTHD = 45

if __name__ == "__main__":
    print(AREAS_COLOR_VALUES_DICT)