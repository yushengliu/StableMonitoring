# coding=utf-8
# !/usr/bin/python
"""
@Time:2019/5/9
@Author: WangZQ
"""

"""
依赖《产品边角web页面详情.xlsx》生成《智库2861产品模块.xmind》
"""


import xmind
import pandas as pd
from xmind.core import workbook, saver
from xmind.core.topic import TopicElement
root_path = 'E:/2861-TemporaryTask/产品介绍/'
data = pd.read_excel(root_path + '产品边角web页面详情.xlsx')
xmind_file_name = '智库2861产品模块.xmind'

w = xmind.load(root_path + xmind_file_name)
s = w.createSheet()
s.setTitle('智库2861')

r = s.getRootTopic()
r.setTitle('智库2861')
max_level = data['层级'].max()

level_node = list()
for i in range(max_level):
    row_data = data[data['层级'] == (i+1)]
    level_node.append(dict())
    if i == 0:
        for j in range(row_data.shape[0]):
            sub_topic = TopicElement(ownerWorkbook=w)
            sub_topic.setTitle(row_data.iloc[j]['结点名称'])
            r.addSubTopic(sub_topic)
            level_node[i][row_data.iloc[j]['结点名称']] = sub_topic
    else:
        for j in range(row_data.shape[0]):
            sub_topic = TopicElement(ownerWorkbook=w)
            sub_topic.setTitle(row_data.iloc[j]['结点名称'])

            if str(row_data.iloc[j]['是否有视频']) != 'nan':
                sub_topic.addLabel("video")

            if str(row_data.iloc[j]['展示方式']) != 'nan':
                if str(row_data.iloc[j]['注释说明']) != 'nan':
                    sub_topic.addLabel(
                        row_data.iloc[j]['展示方式'] + '(' + row_data.iloc[j]['注释说明'] + ')')
                else:
                    sub_topic.addLabel(row_data.iloc[j]['展示方式'])

            level_node[i-1][row_data.iloc[j]['上级结点']].addSubTopic(sub_topic)
            level_node[i][row_data.iloc[j]['结点名称']] = sub_topic

xmind.save(w, root_path + xmind_file_name)
