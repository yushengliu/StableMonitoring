#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/11/8 16:44
@Author  : Liu Yusheng
@File    : web_charts.py
@Description: 生成网页各种图表结构
"""
from copy import deepcopy
from utils import parameters as para

df_2861_geo_county = para.df_2861_gaode_geo_all
df_2861_county = para.df_2861_gaode_geo


# 通用接口：生成左侧的map地图的dict; area_mode=1 全国, area_mode=2 全省; df_id里至少包括三栏——gov_code, value, rank;
#  tips为自己传入的tips字典——规定地图标签上显示的数据种类和名字;注：如果要传入tips的话，df_id里需以tips对应key命名column
def get_colored_map_dict(area_mode, gov_code, df_id, section_title, section_subtitle, map_color_level_id, tips=None):
    rank_map_dict = {}
    # rank_map_dict["id"] = map_id
    # rank_map_dict["name"] = map_id_name[map_id]
    rank_map_dict["type"] = "map"
    rank_map_dict["title"] = section_title
    rank_map_dict["subtitle"] = section_subtitle
    section_data = {}
    if not tips:
        tips = {"value":"指数", "rank":"排名"}
        section_data["tips"] = tips
    else:
        section_data["tips"] = tips
    section_data["classes"] = map_color_level_id

    # 全国图
    if area_mode == 1:
        # geo_data = "2861_county_echars_geo"
        geo_data = "all_province_echars_geo"
        rank_map_dict["geo_data"] = geo_data
        datas_info = []
        for gov_code in df_id.index:
            if gov_code not in df_2861_geo_county.index:
                continue
            data_dict = {}
            data_dict['name'] = df_2861_geo_county.loc[gov_code, 'full_name']
            if not tips:
                data_dict['value'] = df_id.loc[gov_code, 'value']
                if str(type(df_id.loc[gov_code, 'rank'])) == "<class 'str'>":
                    data_dict['rank'] = int(df_id.loc[gov_code, 'rank'][:-2])
                else:
                    data_dict['rank'] = int(df_id.loc[gov_code, 'rank'])
            else:
                for key in tips.keys():
                    data_dict[key] = df_id.loc[gov_code, key]
            datas_info.append(data_dict)
        section_data["datas"] = datas_info
        rank_map_dict["detail"] = section_data

    # 省图
    if area_mode == 2:
        reg = str(gov_code)[0:2]
        geo_data = "province_" + reg + "_county_echars_geo"
        rank_map_dict["geo_data"] = geo_data
        datas_info = []
        df_prov_id = df_id.filter(regex='\A'+reg, axis=0)
        for gov_code in df_prov_id.index:
            if gov_code not in df_2861_county.index:
                continue
            data_dict = {}
            data_dict['name'] = df_2861_county.loc[gov_code, 'full_name']
            if not tips:
                data_dict['value'] = df_prov_id.loc[gov_code, 'value']
                # 环境存在无数据不参与排名的情况
                if str(type(df_prov_id.loc[gov_code, 'rank'])) == "<class 'str'>" and df_prov_id.loc[gov_code, 'rank'] != '本区县暂无数据':
                    data_dict['rank'] = int(df_prov_id.loc[gov_code, 'rank'][:-2])
                else:
                    data_dict['rank'] = int(df_prov_id.loc[gov_code, 'rank'])
            else:
                for key in tips.keys():
                    data_dict[key] = df_prov_id.loc[gov_code, key]
            datas_info.append(data_dict)
        section_data["datas"] = datas_info
        rank_map_dict["detail"] = section_data

    # 市图
    if area_mode == 3:
        # 在外面控制，非直辖市才传数据进来生成市图
        # gov_type = df_2861_county.loc[gov_code, 'gov_type']
        reg = str(gov_code)[0:4]
        geo_data = "city_"+reg+"_county_echars_geo"
        rank_map_dict["geo_data"] = geo_data
        datas_info = []
        df_city_id = df_id.filter(regex='\A'+reg, axis=0)
        for gov_code in df_city_id.index:
            if gov_code not in df_2861_county.index:
                continue
            data_dict = {}
            data_dict['name'] = df_2861_county.loc[gov_code, 'full_name']
            if not tips:
                data_dict['value'] = df_city_id.loc[gov_code, 'value']
                data_dict['rank'] = int(df_city_id.loc[gov_code, 'rank'])
            else:
                for key in tips.keys():
                    data_dict[key] = df_city_id.loc[gov_code, key]
            datas_info.append(data_dict)
        section_data["datas"] = datas_info
        rank_map_dict["detail"] = section_data

    return rank_map_dict


# 通用接口：生成左侧的mixed折线柱状混合图的dict; df_id里至少包括三栏【value, rank可以二选一】——x_name, value(柱子),
#  color(没有的话默认为lightblue), rank(折线); line_type=False: 平滑的曲线, line_type=True: 折线; point_width: 控制柱子大小
def get_mixed_line_dict(title, subtitle, df_id, line_type=False, point_width=None, tips=None):
    line_mixed_dict = {}
    line_mixed_dict["type"] = "mixed1"
    line_mixed_dict["title"] = title
    line_mixed_dict["subtitle"] = subtitle
    detail_dict = {}
    detail_dict["tips"] = {"left":"指数", "right":"排名"} if tips is None else tips
    extra_dict = {}
    if line_type:
        extra_dict["line_type"] = "line"
    else:
        extra_dict["line_type"] = ""
    if point_width is not None:
        extra_dict["point_width"] = point_width
    detail_dict["extra"] = extra_dict
    columns = list(df_id)
    detail_dict["x_name"] = list(df_id["x_name"])
    if 'rank' in columns:
        detail_dict["right"] = list(df_id["rank"])
    if 'value' in columns:
        left_info = []
        # if 'color' not in columns:
        #     default_color = 'lightblue'
        #     for index in df_id.index:
        #         left_dict = {}
        #         left_dict["y"] = df_id.loc[index, 'value']
        #         left_dict["color"] = default_color
        #         # if "text" in columns:
        #         #     left_dict["text"] = df_id.loc
        #         left_info.append(left_dict)
        # else:
        #     for index in df_id.index:
        #         left_dict = {}
        #         left_dict["y"] = df_id.loc[index, 'value']
        #         left_dict["color"] = df_id.loc[index, 'color']
        #         left_info.append(left_dict)
        for index, row in df_id.iterrows():
            left_dict = {}
            left_dict["y"] = row["value"]
            left_dict["color"] = row["color"] if "color" in columns else "lightblue"
            if "text" in columns:
                left_dict["text"] = row["text"]
            left_info.append(left_dict)
        detail_dict["left"] = left_info
    line_mixed_dict["detail"] = detail_dict
    return line_mixed_dict


# 通用接口：生成多色混合柱状折线图的dict
def get_colored_mixed_line_dict(title, subtitle, df_id, df_info, yAxis=None):
    line_mixed_dict = dict()
    line_mixed_dict["type"] = "mixed2"
    line_mixed_dict["title"] = title
    line_mixed_dict["subtitle"] = subtitle
    detail_dict = dict()
    if yAxis is not None:
        detail_dict["yAxis"] = yAxis
    detail_dict["xAxis_name"] = list(df_id["x_name"])
    # print(type(df_id))
    columns = [x for x in list(df_id) if x != "x_name"]
    column_list = []
    # if colors is not None:
    #     for col in columns:
    #         column_list.append({"type":"column", "name":col, "data": list(df_id[col]), "color":colors[col]})
    # else:
    #     for col in columns:
    #         column_list.append({"type":"column", "name":col, "data": list(df_id[col])})
    for col in columns:
        # column_info = {"data":list(df_id[col])}
        column_info = {"data": [eval(i) if isinstance(i, str) else i for i in df_id[col].tolist()]}
        for info_type in list(df_info):
            # a = np.isnan(df_info.loc[col, info_type])
            if df_info.loc[col, info_type] == df_info.loc[col, info_type]:
                column_info[info_type] = df_info.loc[col, info_type]
        column_list.append(column_info)
    # column_list.append({"type": "line", "name": line_name, "data": list(np.sum([list(df_id[col]) for col in columns], axis=0))})
    detail_dict["column_list"] = column_list
    line_mixed_dict["detail"] = detail_dict
    return line_mixed_dict


# 通用接口：生成3dpie饼状图的dict
def get_3dpie_dict(title, subtitle, df_data, xfont=18):
    pie_dict = {}
    pie_dict["type"] = "3dpie"
    pie_dict["title"] = title
    pie_dict["subtitle"] = subtitle

    # 细节数据
    detail_dict = {}
    detail_dict["xfont"] = xfont
    data_list = []
    for index, row in df_data.iterrows():
        data_dict = {}
        for col in list(df_data):
            data_dict[col] = row[col]
        data_list.append(data_dict)
    detail_dict["colcompare_list"] = data_list
    pie_dict["detail"] = detail_dict
    return pie_dict


# 2018/11/8 这个colcompare也太复杂了吧。。。。。
# df_cols_info：每个组别各组元素的信息；——# 每个组别中，某个元素的各项性质 —— name, color, pointPadding(柱子宽度), pointPlacement(每组从中心铺开来，相对坐标范围是[-0.5, 0.5]，这个即控制某个元素在该组中的相对位置；
# df_data：组别内各个元素的取值；column（列名）和df_cols_info里面的name保持一致
def get_colcompare_dict(title, subtitle, df_data, df_cols_info, yAxis_name, xAxis_names, y_range):
    colcompare_dict = {}
    colcompare_dict["type"] = "colcompare"
    colcompare_dict["title"] = title
    colcompare_dict["subtitle"] = subtitle

    detail_dict = {}
    yAxis_dict = {"name":yAxis_name}
    detail_dict["yAxis"] = yAxis_dict
    detail_dict["range"] = y_range
    detail_dict["xAxis_name"] = xAxis_names

    colcompare_list = []

    for index, row in df_cols_info.iterrows():
        colcom_dict = {}
        # 每个组别中，某个元素的各项性质 —— name, color, pointPadding(柱子宽度), pointPlacement(每组从中心铺开来，相对坐标范围是[-0.5, 0.5]，这个即控制某个元素在该组中的相对位置
        for col in list(df_cols_info):
            colcom_dict[col] = row[col]

        # 某个元素在各组间的所有值
        data_list = [eval(i) for i in df_data[row["name"]].tolist()]
        colcom_dict["data"] = data_list

        colcompare_list.append(colcom_dict)

    detail_dict["colcompare_list"] = colcompare_list

    colcompare_dict["detail"] = detail_dict

    return colcompare_dict


# 通用接口：生成stackbar——横着的mixedline的dict，注意，xAxis_name是横（其实是纵）坐标的list，yAxis_dict是纵（其实是横）坐标的dict，如：yAxis = {"name":"区县数量占比（%）"}
def get_stackbar_dict(title, subtitle, xAxis_name, yAxis_dict, df_data, percentage=True, xfont=18, xcolor=para.FT_PURE_WHITE):
    stackbar_dict = {}
    stackbar_dict["type"] = "stacking"
    stackbar_dict["title"] = title
    stackbar_dict["subtitle"] = subtitle

    # 细节数据
    detail_dict = {}
    detail_dict["xAxis_name"] = xAxis_name
    detail_dict["xfont"] = xfont
    detail_dict["xcolor"] = xcolor
    # yAxis_dict = {"name": yAxis_name, "color":para.FT_PURE_WHITE, ""}
    detail_dict["yAxis"] = yAxis_dict
    detail_dict["percentage"] = percentage

    # 数据
    data_list = []
    for index, row in df_data.iterrows():
        data_dict = {}
        for col in list(df_data):
            if col == "data":
                data_dict[col] = eval(row[col])
            else:
                data_dict[col] = row[col]
        data_list.append(data_dict)

    detail_dict["colcompare_list"] = data_list

    stackbar_dict["detail"] = detail_dict
    return stackbar_dict


# 通用接口：生成文本框的dict
def get_textbox_dict(title, subtitle, toptitle, toplist, toptitledict=None, fielddict=None, centerlist=None, footerlist=None):
    textbox_dict = {}
    textbox_dict["type"] = "empty"
    textbox_dict["title"] = title
    textbox_dict["subtitle"] = subtitle

    detail_dict = {}

    # 每个list内的元素之间加个换行
    top_str = "<br/><p>"+"</p><br/><p>".join(toplist)+"</p><br/>"

    # field-题目的标签
    if toptitledict:

        title_desc = ";".join([key + ":" + str(toptitledict[key]) for key in toptitledict.keys()])

        # IE 居中兼容性问题
        if toptitledict["text-align"] == "center":
            toptitle_str = "<legend style='margin:0 auto;%s'>%s</legend>"%(title_desc, toptitle)
        else:
            toptitle_str = "<legend style='%s;margin-%s:50px'>%s</legend>"%(title_desc, toptitledict["text-align"], toptitle)

    # 不传field-题目标签，则默认居左，颜色为橙色
    else:

        toptitle_str = "<legend style='text-align:left;margin-left:50px;color:%s;font-weight:bold;font-size:30px'>%s</legend>"%(para.UN_TITLE_YELLOW, toptitle)

    # field-内容的格式标签
    if fielddict:

        field_desc = ";".join([key + ":" + str(fielddict[key]) for key in fielddict.keys()])

    # 框内内容 —— 默认居左
    else:
        field_desc = "text-align:left;font-size:18px;color:%s;width:80%%;padding-left:30px"%para.FT_PURE_WHITE

    top_desc_str = "<fieldset style='%s;margin:auto'>%s%s</fieldset>"%(field_desc, toptitle_str, top_str)

    detail_dict["top"] = top_desc_str

    # fieldset 框外的center 和 footer 字段先直接写死格式，不传标签
    if centerlist:
        center_desc_str = "<div style='text-align:center;font-size:18px;color:%s;width:80%%;margin:auto'><span style='margin-top:40px;color:%s'>%s</span></div>"%(para.FT_PURE_WHITE, para.FT_PURE_WHITE, "</p><p>".join(centerlist))

        detail_dict["center"] = center_desc_str

    if footerlist:
        footer_desc_str = "<div style='text-align:left;font-size:18px;color:%s;width:80%%;margin:auto'><span style='margin-top:80px;color:%s'>%s</span></div>"%(para.FT_LIGHT_GRAY, para.FT_PURE_WHITE, "</p><p>".join(footerlist))

        detail_dict["footer"] = footer_desc_str

    textbox_dict["detail"] = detail_dict

    return textbox_dict


# 通用接口：生成文本框-版本2的dict，df_id的列——text-align(fieldset里的文字居左/中/右)、scroll（自动滚动否：0/1）、height（每个fieldset的高度：占屏高的百分数）；top_title——fieldset的标题内容（默认fieldset的标题格式legend写死——居左，30px，加粗，标黄——不接受反驳！！！）；content_list：内容条目
def get_textbox2_dict(title, subtitle, df_id, margin_top=True):
    textbox2_dict = {}
    textbox2_dict["type"] = "empty2"
    textbox2_dict["title"] = title
    textbox2_dict["subtitle"] = subtitle

    top_list = []

    for index,row in df_id.iterrows():
        top_dict = {}
        top_str = ""

        style_desc = ";".join(["%s:%s"%(x, row[x]) for x in list(df_id) if x not in ["scroll", "top_title", "content_list"]])

        # 统一加的fieldset内容的格式：居左、字体白色、宽度80%、margin:auto ……
        style_desc += ";font-size:18px;color:#ffffff;width:80%;margin:auto;padding-top:20px;padding-left:30px;"

        # 只有一个时，设置加上margin-top:100px —— 离屏幕上方100px，居中更好看
        if margin_top:
            if df_id.shape[0] == 1:
                style_desc += "margin-top:100px;"

            # 默认fieldset的标题格式legend写死——居左，30px，加粗，标黄
        legend_desc = "<legend style='text-align:left;margin-left:50px;color:#ffcc00;font-weight:bold;font-size:30px'>%s</legend>"%row["top_title"]

        if row["scroll"] == 1:
            top_dict["scrollbool"] = True
            style_desc += "overflow:hidden"
            top_str += "<fieldset class='topscroll' style='%s'>%s<div class='empty_scroll'><p style='padding-bottom:15px;'>%s</p></div></fieldset>"%(style_desc, legend_desc, "</p><p style='padding-bottom:15px;'>".join(eval(row["content_list"])))

        else:
            # style_desc += "overflow:auto;"
            if row["top_title"] != "":
                height = 80
            else:
                height = 100
            top_str += "<fieldset style='%s'>%s<div style='height:%d%%;overflow:auto'><p style='padding-bottom:15px;'>%s</p></div></fieldset>"%(style_desc, legend_desc, height, "</p><p style='padding-bottom:15px;'>".join(eval(row["content_list"])))


        top_dict["text"] = top_str

        top_list.append(top_dict)

    textbox2_dict["detail"] = {}
    textbox2_dict["detail"]["top"] = top_list

    return textbox2_dict


# 通用接口：生成词云图的dict
def get_wordcloud_dict(title, subtitle, words_list):
    wordcloud_dict = {}
    wordcloud_dict["type"] = "wordcloud"
    wordcloud_dict["title"] = title
    wordcloud_dict["subtitle"] = subtitle
    wordcloud_dict["detail"] = {}
    wordcloud_dict["detail"]["text"] = ".".join(words_list)
    return wordcloud_dict


# 通用接口：象限图接口
def get_quadrant_dict(title, subtitle, df_area_n_axis, df_scatter, with_middle_area=False, middle_shape="diamond",middle_points=None, font_positions=None, area_colors=None):
    """
    @功能：返回象限图结构
    :param title:
    :param subtitle:
    :param df_area_n_axis: columns - [xAxis_name, aera_name, aera_desc]
    :param df_scatter: columns - [x, y, (z, color), name, x_desc, y_desc]
    :param with_middle_area:
    :param middle_shape:
    :param middle_points:
    :param font_positions:
    :param area_colors:
    :return:
    """
    quadrant_dict = dict()
    quadrant_dict["type"] = "scatter_point"
    quadrant_dict["title"] = title
    quadrant_dict["subtitle"] = subtitle

    detail_dict = dict()

    # 一、结构 - 可默认
    # Step1. 中间区域与轴的交点 - 从y正轴交点开始，顺时针旋转
    if with_middle_area:
        # 有中间区域
        if middle_points is not None:
            detail_dict["middlePoint"] = middle_points
        else:
            detail_dict["middlePoint"] = [[0, 50], [50, 0], [0, -50], [-50, 0]]  # 默认为半点
    else:
        # 无中间区域
        detail_dict["middlePoint"] = [[0, 0], [0, 0], [0, 0], [0, 0]]

    # Step2. 区域标题位置 - 从一象限起，顺时针旋转
    if font_positions is not None:
        detail_dict["middleFont"] = font_positions
    else:
        detail_dict["middleFont"] = [[55, 47], [55, -63], [-55, -63], [-55, 47], [0, -8]]    # 每个区域中间描述的位置，如果没传则默认为先前调好的 - 逆时针

    # Step3. 区域颜色 - 从一象限起，顺时针旋转
    if area_colors is not None:
        detail_dict["xcolors"] = area_colors
    else:
        # 没传则默认之前调好的颜色 - 红黄灰蓝绿
        if with_middle_area:
            detail_dict["xcolors"] = ["RGB(255,117,117)", "RGB(255,220,116)", "RGB(220,220,220)", "RGB(152,218,232)", "RGB(26,179,157)"]
        else:
            detail_dict["xcolors"] = ["RGB(255,117,117)", "RGB(255,220,116)", "RGB(220,220,220)", "RGB(152,218,232)"]

    # 二、内容 - 需自定义
    # Step1. 坐标轴 - 从x正轴起，顺时针旋转
    detail_dict["name_desc"] = df_area_n_axis["xAxis_name"].tolist()[:4]

    # Step2. 区域标题及描述
    df_area_desc = deepcopy(df_area_n_axis[["area_name", "area_desc"]])
    df_area_desc = df_area_desc.rename(columns={"area_name": "name", "area_desc": "name2"})
    detail_dict["xAxis_name"] = df_area_desc.to_dict(orient="records")

    # Step3. 描点
    common_color = "rgba(141, 70, 83, 1)"  # 不传默认棕色
    df_scatter["color"] = df_scatter["color"].fillna(common_color) if "color" in df_scatter.columns else common_color

    df_scatter["z"] = df_scatter["z"].fillna(0.5) if "z" in df_scatter.columns else 0.5

    data_cols = ["x", "y", "z", "color"]
    df_scatter["data"] = df_scatter.apply(lambda x: {data_col_: x[data_col_] for data_col_ in data_cols}, axis=1)

    df_scatter = df_scatter.rename(columns={"x_desc": "degree", "y_desc": "nodegree"})

    # 点的要素 // degree,nodegree 是指两行描述
    scatter_cols = ["name", "degree", "nodegree", "data"]
    df_scatter_ = deepcopy(df_scatter[scatter_cols])

    detail_dict["scatter_list"] = df_scatter_.to_dict(orient="records")

    quadrant_dict["detail"] = detail_dict

    return quadrant_dict


def format_list_line(texts, color=None, size=None):
    """
    @功能：右侧list的某一行的数据格式
    :param texts: 可能存在1~3个区段，如果没有，list内就只传一句话
    :param color:
    :param size:
    :return:
    """
    text_dict = dict()
    text_dict["cols"] = [{"text": x} for x in texts]
    if color:
        text_dict["color"] = color
    if size:
        text_dict["size"] = size
    return text_dict


# 通用接口，把文字 ->> list中datas字段的格式
def get_list_lines(texts_list, colors=None, sizes=None):
    lines_datas = []
    for i in range(0, len(texts_list)):
        if (colors is not None) and (sizes is not None):
            line_dict = format_list_line(texts_list[i], colors[i], sizes[i])
        elif colors is not None:
            line_dict = format_list_line(texts_list[i], colors[i])
        elif sizes is not None:
            line_dict = format_list_line(texts_list[i], size=sizes[i])
        else:
            line_dict = format_list_line(texts_list[i])
        lines_datas.append(line_dict)

    return lines_datas


# 通用接口：生成一个页面的list_dict - {list_key: list_desc}
def get_list_dict_with_list_key(list_key, title, subtitle, data_list, width=None, desc=None):
    """
    @功能：生成一个页面的list_dict //含list_key - 便于后续多个页面时，join dict - {list_key: {"title": "", "sub_title": "", "datas": [], "width": "", "desc": "" }}
    :param list_key:
    :param title:
    :param subtitle:
    :param data_list:
    :param width:
    :param desc:
    :return:
    """
    list_desc = dict()
    list_content = dict()
    list_content["title"] = title
    list_content["sub_title"] = subtitle
    list_content["width"] = width if width else "30%"   # 默认30%
    list_content["desc"] = desc if desc else "详见左图"
    list_content["datas"] = data_list

    list_desc[list_key] = list_content

    return list_desc


def get_list_dict(title, subtitle, data_list, width=None, desc=None):
    """
    @功能：生成一个页面的list_dict  - {"title":"", "sub_title": "", "datas": [], "width": "", "desc": ""}
    :param title:
    :param subtitle:
    :param data_list:
    :param width:
    :param desc:
    :return:
    """
    list_desc = dict()
    list_desc["title"] = title
    list_desc["sub_title"] = subtitle
    list_desc["width"] = width if width else "30%"  # 默认30%
    list_desc["desc"] = desc if desc else "详见左图"
    list_desc["datas"] = data_list

    return list_desc


# 通用接口：生成多个页面的list_dict // 翻页的情况
def get_lists_dict(list_dicts_list, setting_names, node_code=None, region_code=None, catalog_dict=None):
    lists_dict = dict()

    for list_desc_ in list_dicts_list:
        for list_key, list_content in list_desc_.items():
            lists_dict[list_key] = list_content

    if (node_code is not None) and (region_code is not None):
        setting_list = ["{}/{}/{}".format(node_code, region_code, setting_) for setting_ in setting_names]
    else:
        setting_list = setting_names

    lists_dict["page_list"] = dict()
    lists_dict["page_list"]["setting_list"] = setting_list

    if catalog_dict is not None:
        lists_dict["catalog"] = catalog_dict

    return lists_dict


# 通用接口：生成setting - 单个页面
def get_setting(title, list_key, node_code, region_code, chart_name, set_name=None):
    setting_dict = dict()
    setting_dict["title"] = title

    datas_ = dict()
    datas_["id"] = list_key
    datas_["node_code"] = node_code
    datas_["name"] = set_name if set_name else "123"
    datas_["data"] = "{}/{}".format(region_code, chart_name)

    setting_dict["datas"] = [datas_]

    return setting_dict


if __name__ == "__main__":
    pass