#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/10/16 19:45
@Author  : Liu Yusheng
@File    : parameters.py
@Description:
"""

import pandas as pd
from copy import deepcopy

from db_interface import database
from utils.MyModule import DataBaseFirm
from utils import path_manager as pm

# ===============================环境口碑分类参数=============================
# 2861区县信息表
df_2861_gaode_geo_all = pd.read_csv(pm.GAODE_COUNTY_LIST_XMD, index_col='gov_code', encoding='utf-8')
df_2861_gaode_geo = df_2861_gaode_geo_all[(df_2861_gaode_geo_all['gov_type'] > 2) & (df_2861_gaode_geo_all["gov_type"] != 31)]

municipal_with_no_counties = ["广东省|东莞市", "广东省|中山市", "海南省|三沙市", "海南省|儋州市", "甘肃省|嘉峪关市"]

municipal_with_no_county_ids = [2058, 2059, 2212, 2213, 2927]

# 江北新区，历史的特殊处理 —— 相当于再加上另两个区县的和 （浦口区+六合区）—— 20190225
sp_gov_disposal = {5026: [5026, 814, 818]}


# # 环境样本数据库
# env_text_server = database.create_user_defined_database_server(host='192.168.0.133', port='6500', user='etherpad', pwd='123456')
# env_text_db = "text-mining"
# cnn_env_table = "xmd_env_samples_cnn"
#
# env_sample_cnn_obj = DataBaseFirm(env_text_server, env_text_db, cnn_env_table)

# 事件信息表
event_server = database.create_user_defined_database_server(host="192.168.0.133",port="6500",user="etherpad", pwd="123456")
event_db = "text-mining"
event_table = "xmd_event_basic_info"

event_db_obj = DataBaseFirm(event_server, event_db, event_table)


# 环境大类关键词
env_r = '霾|空气不好|空气污染|浓烟|废气|焚烧秸秆|排污|尾气|刺激性气体|扬尘|粉尘|降尘|脱硫|空气糟糕|污染空气|污染大气|大气污染|水质' \
        '|排放|排污|臭水|水污染|水体|干旱|水化物|污水|采滤|供水|酸性|赤潮|持水|废水|地下水|富集|富营养化|河道污染|净水|枯水期|水处理|' \
        '水蚀|水土流失|水土保持|河长|噪音|扰民|噪音污染|噪声|养殖粪污|粪尿|养殖异味|畜禽尸体|养鸡场臭气|养鸡场异味|养殖场臭气|' \
        '养殖异味|养鸡场粪便|养殖场污染|猪舍的废水|畜禽养殖|养猪场|畜禽粪便|农村养殖|生猪养殖|鸡鸭养殖|粪尿直排|猪场污染|' \
        '养殖场整治|非法养殖|养殖业|养殖污染|养殖粪便|猪粪|猪屎|养猪场|垃圾坑|化工厂|偷排|刺鼻|造纸厂|垃圾成堆|工业污染|建筑垃圾|工业垃圾|' \
        '倾倒垃圾|臭气熏天|横流|镉|重金属|铅中毒|毒大米'

env_s = '气象台|气象局'


# 之后把微博提出来，在Python里过滤
env_sensitive_word_list = ['有毒气体','有毒浓烟','有毒烟雾','烧秸秆','烧垃圾','垃圾焚烧','污染空气',
                           '臭氧超标','臭氧污染','油烟污染','餐饮油烟','排放油烟','刺鼻','臭气熏天','排污',
                           '非法排放','超标排','乱排放','乱排乱放','夜间排放','夜晚排放','半夜排放',
                           '偷排','臭水','水臭','污水','污浊','废水','水体污染','河道污染','污染河道',
                           '河流污染','污染河流','污染物','污染源','水污染','水发黄','水发臭','水有气味',
                           '水质污染','水质差','水质很差','水质非常差','扬尘','噪音扰民','夜间施工','半夜施工',
                           '夜晚施工','养殖粪污','养殖异味','养殖场污染','猪舍的废水','粪尿直排','猪场污染','养殖污染',
                           '垃圾坑','垃圾填埋','垃圾成堆','垃圾堆','垃圾成山','工业污染','工业园污染','化学污染',
                           '化工污染','餐饮污染','食品污染','农业污染','城市污染','农村污染','生活污染','医疗污染',
                           '医疗垃圾','农药残留','农药超标','抗生素超标','抗生素污染','电子垃圾','建筑垃圾','工业垃圾',
                           '生活垃圾','倒垃圾','倒建渣','夜间倾倒','夜晚倾倒','半夜倾倒','镉大米','镉中毒',
                           '镉超标','重金属中毒','重金属超标','重金属污染','铅中毒','血铅','毒大米','破坏环境',
                           '破坏生态','破坏植被','破坏绿化','植被被破坏','水土流失','砍伐','乱砍乱伐','环境破坏',
                           '严重污染','污染严重','污染环境','环境污染','污染事故', '水质异常', '彻夜施工', '垃圾山','无人清理',
                           '垃圾堆积', '无人安排清理', '污水满溢', '污水直排', '鱼虾死亡', '鱼虾出现大面积死亡', '鱼虾大量死亡',
                           '鱼虾蟹大量死亡', '鱼虾蟹死亡', '鱼虾已经全部死亡', '水体呈黑色', '工业废水', '水面有油渍漂浮', '水呈黑色',
                           '黑臭水体', '黑色水体', '水体有白色泡状物', '水体呈灰色或黑色', '违规燃煤', '举报污染', '氨气泄漏',
                           '水面漂杂物', '随意砍伐', '生活污水', '高浓度污水']

env_stop_words = ['气象局', '信访件', '妨害公务', '气象条件', '检察院', '开展专项整治', '应急处置情况', '向我局报告',
                  '即拆即清', '复耕复绿', '绿色检察', '未造成人员伤亡和环境污染', '未造成环境污染', '联合整治', '环保局二中队',
                  '获奖作品展播', '微电影', '环保局直属中队', '进行现场检查', '路面清障', '秦岭保卫战', '欢迎收看', '实施意见',
                  '聚焦民生问题', '每日聚焦', '创新工作方式', '寻宝', '进行现场检查', '深化政企合作', '大家共同努力', '人人有责',
                  '民间资本', '防治工作考核', '身亡', '死亡', '联合发布', '环境保护专项行动', '每日聚焦', '感谢检察院','清理完毕',
                  '得到有效改善', '有所信仰', '火灾']


# http标头
my_headers = [
        "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0"
]


#区县部门名
gov_department_keyword_new = \
    u"县委|区委|市委|旗委|党委|纪委|政法委|县政协|区政协|市政协|旗政协|县人大|区人大|市人大|旗人大|县政府|区政府|市政府|旗政府" \
    u"人武部|法院|检察院|管理处|信息办|保密局|防邪办|组织部|老干局|党建办|编办|宣传部|国土资源局|" \
    u"文明办|社科联|新闻宣传中心|文联|政研室|统战部|民宗局|机关工委|工信局|群工局|国资办|法制办|应急指挥中心|" \
    u"应急办|公共资源交易中心|安监局|人社局|审计局|公安局|公安分局|派出所|司法局|民政局|档案局|督查办|督查室|扶贫开发局|" \
    u"扶贫办|扶贫局|发改局|统计局|交通运输局|交通局|商旅局|工商局|旅游局|商业局|商务局|招商局|供销联社|财政局|" \
    u"住房公积金中心|住房公积金管理处|农办|农业局|林业局|水务局|管理局|管理处|住建局|国土局|国土分局|环保局|防洪办|" \
    u"教体局|教育局|体育局|卫计局|卫生局|防震减灾局|文广新局|文化局|新闻局|文广局|投促局|投资促进局|国保大队|邮政局|" \
    u"气象局|食药监局|质监局|烟草专卖局|烟草局|供电局|国税局|地税局|税务局|惠民帮扶中心|帮扶中心|金融办|城管局|规划局|" \
    u"规划分局|消防大队|消防局|消防队|党校|总工会|妇联|台办|工商联|残联|红十字会|机关事务局|机关事务管理局|网信办|土地局|" \
    u"信访办|信访局|建设局|工商税务|园林局|管理委员会|接待办|人防|人民防空|农牧局|渔业局|保障局|药监局|交警队|刑警队|" \
    u"特警队|防暴警察|维稳办|安全局|档案局|物价局|空管局|执法局|管委会|测绘局|勘测局|勘探局|" \
    u"有关部门|相关部门|政府部门|医院|学校|城管|督察组|中学|小学|幼儿园"


#区县官职名
gov_guanzhi_keyword_new = \
    u"县委副书记|区委副书记|市委副书记|党委副书记|纪委副书记|政法委副书记|政协副主席|人大副主任|副县长|副区长|副市长|副旗长|" \
    u"县委书记|区委书记|市委书记|党委书记|纪委书记|政法委书记|政协主席|人大主任|县长|区长|市长|旗长|" \
    u"副书记|副局长|副处长|副科长|副部长|副主任|大队长|中队长|副镇长|副乡长|副村长|副秘书长|" \
    u"书记|局长|处长|科长|部长|主任|队长|镇长|乡长|村长|秘书长|领导|干部|当官的|官员"


# ===============================环境口碑评分参数======================================

# measure有杂质，算分时提取比例
m2p_prob = 0.05  # measure to positive
m2n_prob = 0.3   # measure to negative
m2m_prob = 0.65  # measure to measure

# measure的sentiment_extent降比
ms_prob = 0.5

# 单条微博影响力指数计算公式 —— 转发/点赞/评论数权重  weibo_index = ws*(count_share+1)+wt*(count_read+1)+wc*(count_comment+1)
ws = 0.4       # 转发数的权重
wt = 0.3       # 点赞(named 阅读）数的权重
wc = 0.3       # 评论数的权重

# 污染分类
env_labels = ['air', 'noise', 'solidwaste', 'aquaculture', 'chemical']
# 总类别
env_labels_new = ['air', 'noise', 'chemical', 'solidwaste', 'aquaculture', 'water', 'env']
env_titles = ['空气污染', '噪音污染', '化工污染', '固废污染', '养殖污染', '水土污染', '环境污染']

# 情绪分类
sentiment_labels = ['positive', 'negative', 'measure']

bad_air_provs_codes = ["61", "51", "11", "13", "14"]
bad_air_provs_names = ["陕西省", "四川省", "北京市", "河北省", "山西省"]

bad_noise_cities_codes = ["1101", "3101", "4401", "4403", "4101", "6101"]
bad_noise_cities_names = ["北京", "上海", "广州", "深圳", "郑州", "西安"]

env_monthly_score_csv = "ENV_neg_count_monthly_com.csv"
stable_monthly_score_csv = "STABLE_SCORE_FINAL.csv"


# =============================环境口碑入库存储参数===================================

# 全国库的环境月度评分表
score_server = database.get_database_server_by_nick(database.SERVER_SPIDER_BASE_DATA_MANAGE)
score_db = 'doc_base_datas'
score_table = 'env_score_data'                   # 环境月度值表；累计值以年为单位比较才有意义

# 产品测试服务器
product_test_server = database.create_user_defined_database_server('192.168.0.133', '5434', 'postgres', '123456')
# 产品服务器
product_official_server = database.get_database_server_by_nick(database.SERVER_PRODUCT)
product_test_db = 'product'
env_product_table = 'interface_env'

# 知识库
# META_URL = "http://192.168.0.88:9400/knowledgemeta/?"
# DATA_URL = "http://192.168.0.88:9400/knowledgebase/?"
# 知识库服务切换服务器
META_URL = "http://192.168.0.47:9400/knowledgemeta/?"
DATA_URL = "http://192.168.0.47:9400/knowledgebase/?"

# df_meta = pd.read_csv(pm.META_PATH, encoding='utf-8-sig')




# ============================环境口碑客户端展示参数===================================
# 常用颜色

FT_SOBER_BLUE = "rgba(84, 180, 415, 1)"  # 字体常用
FT_DEEP_BLUE = "#5696d8"
FT_ORANGE = "orange"
FT_BLUE_WITH_LG = "#000937"
FT_LIGHT_RED = "#EE4000"
FT_PURE_WHITE = "#FFFFFF"
FT_YELLOW_WITH_DB = "#ffde79"
FT_BLACK = "#111"
FT_LIGHT_GRAY = "LightSlateGray"   #778899

# 按钮专用
BT_TIFFANY_BLUE = "#00ada9"
BT_DEEP_GREEN = "rgba(15, 163, 102, 1)"

# 背景常用
BG_YELLOW = "#ffb10a"
BG_GREEN = "#00c574"
BG_BLUE = "#224276"
BG_RED = "#ff1c47"

# 统一之后
UN_TITLE_YELLOW = "#ffcc00"
UN_STRESS_RED = "#ff1133"
UN_LIGHT_GREEN = "#6cf2cb"

# 划线常用
LN_GOLDEN = "rgba(251,189,4,1)"
LN_YELLOW = "rgba(253,253,2,1)"
LN_RED = "rgba(227,8,10,1)"
ZZ_BLUE = "#9ff2e7"
LN_WARNING_PINK = "rgba(250,192,230,1)"

WCOLORS = {'A':LN_GOLDEN, "B":LN_YELLOW, "C":LN_RED, 'Z':ZZ_BLUE}



# 子类占比的颜色
EC_AIR_BLUE = "rgb(0,173,169)"         # "#00ada9"   # Tiffany蓝
# EC_NOI_PURPLE = "rgb(153, 158, 255)"  # 噪音 - 紫
EC_NOI_PURPLE = "rgb(237,85,101)"   # 噪音 - 粉红
EC_WATER_ORANGE = "#ffcc00"          # "rgb(237, 125, 49)"         #   水土 - 橙色

EC_COLOR_DICT = {"air": EC_AIR_BLUE, "noise": EC_NOI_PURPLE, "water": EC_WATER_ORANGE}


# 高于市均变化柱子的颜色
HIGHER_RED = "#FF0000"
LOWER_GREEN = "#00c574"

# 饼状图的背景色
PIE_BACK_WHITE = "#e8edef"

# 地图配色
map_color_list = [
    'red',  # 0 高
    '#FA343F',  # 1
    '#FB6F74',  # 2
    '#FCA9AB',  # 3
    '#FEE3E3',  # 4
    '#E3F1E3',  # 5 中
    '#ACD5AB',  # 6
    '#74B973',  # 7
    '#3F9D3D',  # 8
    'green'  # 9 低
]

class_7colors = [map_color_list[c] for c in [0, 2, 3, 5, 7, 8, 9]]  # 绿色到红色
map_color_7classes = [{"from": 60, "to": 65, "color": class_7colors[0], "name": "极强"},
                      {"from": 65, "to": 70, "color": class_7colors[1], "name": "强"},
                      {"from": 70, "to": 75, "color": class_7colors[2], "name": "较强"},
                      {"from": 75, "to": 81.25, "color": class_7colors[3], "name": "中度"},
                      {"from": 81.25, "to": 87.5, "color": class_7colors[4], "name": "较轻"},
                      {"from": 87.5, "to": 93.75, "color": class_7colors[5], "name": "轻微"},
                      {"from": 93.75, "color": class_7colors[6], "name": "几乎没有"}]

map_color_7classes_prov = [{"from": 60, "to": 65.7, "color": class_7colors[0], "name": "极强"},
                      {"from": 65.7, "to": 71.4, "color": class_7colors[1], "name": "强"},
                      {"from": 71.4, "to": 77.1, "color": class_7colors[2], "name": "较强"},
                      {"from": 77.1, "to": 82.8, "color": class_7colors[3], "name": "中度"},
                      {"from": 82.8, "to": 88.5, "color": class_7colors[4], "name": "较轻"},
                      {"from": 88.5, "to": 94.2, "color": class_7colors[5], "name": "轻微"},
                      {"from": 94.2, "color": class_7colors[6], "name": "几乎没有"}]


weight_dict = {"极强": 11, "强": 9, "较强": 7, "中度": 5, "较轻":3, "轻微": 2, "几乎没有": 1}


value_limits_prov = [65.7, 71.4, 77.1, 82.8, 88.5, 94.2]

pct_limits = [1, 3, 5, 10]

class_5colors = [map_color_list[c] for c in [0, 3, 5, 7, 9]]  # 红色到绿色

map_color_5classes = [{"from":90, "color":class_5colors[0], "name": "危"},
                      {"from":70, "to":90, "color":class_5colors[1], "name":"高"},
                      {"from":40, "to":70, "color":class_5colors[2], "name":"中"},
                      {"from":20, "to":40, "color":class_5colors[3], "name":"较低"},
                      {"from":0, "to":20, "color":class_5colors[4], "name":"低"}]

grade2color = {"危":class_5colors[0], "高": class_5colors[1], "中":class_5colors[2], "较低": class_5colors[3], "低": class_5colors[4]}

value_limits = [90, 70, 40, 20]

if __name__ == "__main__":
    print("parameters.py")