# encoding: utf-8
"""
Author:yusheng.liu
Time:2019/10/2 13:43
file:get_2861_gaode_geo_gov_id_info.py
"""

"""
几个原则
① 保证所有县/市/省都有数据，包括高新区、经开区、江北新区等等 —— 数据全量，对方需要什么给什么
② 提到县，就是行政区划上的县，省级直辖县也是县，直辖市下的县也是县，只和区县比；提到市，就是行政区划上的市，省辖地级市也是市；提到省，就是行政区划上的省，直辖市也是省
③ 以上原则适用于指标计算过程中，计分、计数、排名等；若有特殊需求，如需把省级单位和市级单位同等比较（把北京和成都比），单独写接口提取
"""

import pandas as pd
import sys
from copy import deepcopy

from utils import path_manager

GOV_TYPE_IS_PROVINCE = 0                                   # 省
GOV_TYPE_IS_CITY = 1                                       # 市
GOV_TYPE_IS_CITY_DIRECTLY_UNDER_COUNTRY = 2                # 国家级直辖市 或 行政区
GOV_TYPE_IS_COUNTY = 3                                     # 区县
GOV_TYPE_IS_COUNTY_IN_CITY_DIRECTLY_UNDER_COUNTRY = 4      # 国家级直辖市下的区县
GOV_TYPE_IS_COUNTY_DIRECTLY_UNDER_PROVINCE = 5             # 省级直辖县
# 商圈模型不考虑高新区、经开区
GOV_TYPE_IS_HIGH_TECH_ZONE = 6                             # 高新区
GOV_TYPE_IS_HIGH_TECH_ZONE_ZX = 7                          # 直辖市下辖的高新区
GOV_TYPE_IS_ECO_DEVELOP_ZONE = 8                           # 经开区
GOV_TYPE_IS_HIGH_TECH_ZONE_S = 10                          # 省辖高新区
GOV_TYPE_IS_NO_COUNTY_CITY = 31                            # 省辖地级市（市辖无区县）

PROVINCE_SUB_CITY = 1
PROVINCE_SUB_COUNTY = 2
CITY_SUB_COUNTY = 3

df_2861_gaode_geo_all = pd.read_csv(path_manager.GAODE_COUNTY_LIST, index_col='gov_id',encoding="utf8")


def get_all_gov_id_info(with_hitec=False):
    """
    @功能：获取所有行政区划列表
    :param with_hitec:
    :return:
    """
    if with_hitec:
        df_geo = df_2861_gaode_geo_all
    else:
        df_geo = df_2861_gaode_geo_all[df_2861_gaode_geo_all["gov_type"].isin([0, 1, 2, 3, 4, 5, 31])]

    return df_geo


def get_all_county_gov_id_info(with_hitec=True):
    """
    @功能：获取所有行政区划-区县的信息表
    :return:
    """
    if with_hitec:
        df_county = df_2861_gaode_geo_all[(df_2861_gaode_geo_all["gov_type"]>=3)&(df_2861_gaode_geo_all["gov_type"]!=31)]
    else:
        df_county = df_2861_gaode_geo_all[df_2861_gaode_geo_all["gov_type"].isin([3, 4, 5])]

    return df_county


def get_all_city_gov_id_info():
    """
    @功能：获取所有行政区划-市的信息表   // 市+省辖地级市（市辖无区县）
    :return:
    """
    df_city = df_2861_gaode_geo_all[df_2861_gaode_geo_all["gov_type"].isin([1, 31])]

    return df_city


def get_all_province_gov_id_info():
    """
    @功能：获取所有行政区划-省的信息表   // 省+直辖市
    :return:
    """
    df_province = df_2861_gaode_geo_all[df_2861_gaode_geo_all["gov_type"].isin([0, 2])]

    return df_province


def get_gov_id_info(gov_id):
    """
    @功能：根据入参id，返回具体行政区信息
    :param gov_id:
    :return:
    """
    gov_id_info = df_2861_gaode_geo_all.loc[gov_id].to_dict()

    return gov_id_info


def get_parent_city_id_info(gov_id):
    """
    @功能：获取所在市的信息
    :param gov_id:
    :return:
    """
    city_id = df_2861_gaode_geo_all.loc[gov_id, "in_city"]

    if city_id == 0:
        print("入参gov_id={}，没有上级市的信息，请确认输入".format(gov_id), flush=True)
        return None
    else:
        city_id_info = df_2861_gaode_geo_all.loc[city_id].to_dict()
        city_id_info["gov_id"] = city_id
        return city_id_info


def get_parent_prov_id_info(gov_id):
    """
    @功能：获取所在省的信息
    :param gov_id:
    :return:
    """
    prov_id = df_2861_gaode_geo_all.loc[gov_id, "in_province"]

    if prov_id == 0:
        print("入参gov_id={}，没有上级省的信息，请确认输入".format(gov_id), flush=True)
        return None
    else:
        prov_id_info = df_2861_gaode_geo_all.loc[prov_id].to_dict()
        prov_id_info["gov_id"] = prov_id
        return prov_id_info


def get_prov_sub_city_ids(gov_id):
    """
    @功能：获取省下辖市的gov_id列表
    :param gov_id: 省的gov_id
    :return:
    """
    df_city = get_all_city_gov_id_info()

    sub_city_ids = df_city[df_city["in_province"]==gov_id].index.values.tolist()

    return sub_city_ids


def get_prov_sub_county_ids(gov_id, with_hitec=False):
    """
    @功能：获取省下辖区县的gov_id列表
    :param gov_id:
    :param with_hitec:
    :return:
    """
    df_county = get_all_county_gov_id_info(with_hitec)

    sub_county_ids = df_county[df_county["in_province"]==gov_id].index.values.tolist()

    return sub_county_ids


def get_city_sub_county_ids(gov_id, with_hitec=False):
    """
    @功能：获取市下辖区县的gov_id列表
    :param gov_id:
    :param with_hitec:
    :return:
    """
    df_county = get_all_county_gov_id_info(with_hitec)

    sub_county_ids = df_county[df_county["in_city"]==gov_id].index.values.tolist()

    return sub_county_ids


def get_hot_city_infos(debug=False):
    """
    @功能：获取热门城市信息表
    :param debug:
    :return:
    """
    df_city = df_2861_gaode_geo_all[df_2861_gaode_geo_all["gov_type"].isin([GOV_TYPE_IS_CITY, GOV_TYPE_IS_CITY_DIRECTLY_UNDER_COUNTRY, GOV_TYPE_IS_NO_COUNTY_CITY])]

    hot1_city_infos = []
    hot2_city_infos = []
    hot3_city_infos = []
    for gov_id in df_city.index.values:
        gov_code = df_city.loc[gov_id, 'gov_code']
        gov_type = df_city.loc[gov_id, 'gov_type']
        gov_name_full = df_city.loc[gov_id, 'full_name']
        hot_city_code = '0100000000'
        if gov_type == GOV_TYPE_IS_CITY_DIRECTLY_UNDER_COUNTRY or gov_id in [1936,1959]:
            if gov_type == GOV_TYPE_IS_CITY_DIRECTLY_UNDER_COUNTRY:
                city_info = {'city_id': int(gov_id), 'city_name': gov_name_full}
            else:
                name_list = gov_name_full.split(r'|')
                city_name = name_list[1]
                city_info = {'city_id': int(gov_id), 'city_name': city_name}
            hot1_city_infos.append(city_info)
        elif hot_city_code in str(gov_code):
            name_list = gov_name_full.split(r'|')
            city_name = name_list[1]
            city_info = {'city_id': int(gov_id), 'city_name': city_name}
            hot3_city_infos.append(city_info)
        elif gov_id in [1158,1362,479,934,847]:
            name_list = gov_name_full.split(r'|')
            city_name = name_list[1]
            city_info = {'city_id': int(gov_id), 'city_name': city_name}
            hot2_city_infos.append(city_info)

    hot_city_infos = hot1_city_infos + hot2_city_infos + hot3_city_infos
    if debug == True:
        print(hot_city_infos)
        print(len(hot_city_infos))
    return hot_city_infos


def add_some_id_data(df_data, dst_id, src_ids, by):
    if by == "sum":
        df_data.loc[dst_id] = df_data.loc[src_ids].sum()

    elif by == "average":
        df_data.loc[dst_id] = df_data.loc[src_ids].mean()

    elif by == "median":
        df_data.loc[dst_id] = df_data.loc[src_ids].median()

    else:
        # df_data.loc[dst_id] = df_data.loc[src_ids].sum()  # 默认为求和
        print("补齐数据入参（by）错误，请在/sum, avg, median/中选择", flush=True)
        return False

    return df_data


def complement_type_region_by_median(df_data, region_type, with_hitec=False):
    """
    @功能：对不同等级的区域，用同级区域中位值补全；如果都缺失，则补0
    :param df_data:
    :param region_type: "prov", "city", "county"
    :param with_hitec:
    :return:
    """
    if region_type == "county":
        df_region = get_all_county_gov_id_info(with_hitec)
    elif region_type == "city":
        df_region = get_all_city_gov_id_info()
    else:
        df_region = get_all_province_gov_id_info()

    df_data = df_data.reindex(df_data.index | df_region.index)
    region_median = df_data.loc[df_region.index, :].median().to_dict()
    df_data.loc[df_region.index, :] = df_data.loc[df_region.index, :].fillna(value=region_median)
    # 如果还有nan， 也就是完全缺失
    df_data.loc[df_region.index, :] = df_data.loc[df_region.index, :].fillna(value=0)

    return deepcopy(df_data)


def complement_prov_city_data_with_counties(df_data, columns, by="sum", with_country=False, verify_non_counties_with_zero=False, verify_non_counties_with_median=False, with_hitec=False, prov_by_sub_citys=False, city_by_median=False, prov_by_median=False):
    """
    @功能：用区县数据补齐省市(国)数据
    :param df_data:
    :param columns:
    :param by: 补齐方法
    :param with_country: 补齐国的数据
    :param verify_non_counties_with_zero: 校验区县数据是否齐全，不全则用0补齐
    :param verify_non_counties_with_median: 校验区县数据是否齐全，不全则用已有区县中位值补齐，如果区县数据全部缺失，则补0
    :param with_hitec: 包不包括高新区的数据
    :param prov_by_sub_citys: True - 省的数据用下辖市的数据填充； False - 省的数据用下辖区县的数据填充
    :param city_by_median: True - 市的数据用同级市数据的中位值填充（如果所有市数据缺失，则补0）；False - 市的数据用下辖区县数据填充
    :param prov_by_median:
    :return:
    """

    df_data = deepcopy(df_data[columns])

    for col in df_data.columns:
        df_data[col] = df_data[col].apply(lambda x: x[col] if isinstance(x, dict) else x)

    # 如果还需补齐无数据的区县 - 只是补齐无数据的区县，原有的索引和数据都保留（并集）
    if verify_non_counties_with_zero:
        df_county = get_all_county_gov_id_info(with_hitec)
        df_data = df_data.reindex(df_data.index | df_county.index, fill_value=0)

    # 用中位值填充 无数据的区县
    if verify_non_counties_with_median:
        df_data = complement_type_region_by_median(df_data, region_type="county", with_hitec=with_hitec)

    df_city = get_all_city_gov_id_info()
    city_ids = df_city.index.values.tolist()

    if city_by_median:  # 市的数据用同级中位值补全
        df_data = complement_type_region_by_median(df_data, region_type="city")

    else:   # 市的数据用下辖区县数据补齐
        for city_id in city_ids:
            # 下属无区县的市，先不补（在后面取中位数补）
            if city_id in [2058, 2059, 2212, 2213, 2927]:
                continue
            if city_id not in df_data.index:
                sub_ids = get_city_sub_county_ids(city_id, with_hitec=with_hitec)
                df_data = add_some_id_data(df_data, city_id, sub_ids, by)

        # 补上省辖地级市（市辖无区县）的数据 - 该省下辖市数据的中位数
        for city_id_ in [2058, 2059, 2212, 2213, 2927]:
            if city_id_ not in df_data.index:
                prov_id_ = get_parent_prov_id_info(city_id_)["gov_id"]
                sub_city_ids_ = get_prov_sub_city_ids(prov_id_)
                df_data = add_some_id_data(df_data, city_id_, sub_city_ids_, by="median")

    df_prov = get_all_province_gov_id_info()
    prov_ids = df_prov.index.values.tolist()

    if prov_by_median:  # 省的数据用同级中位值填充
        df_data = complement_type_region_by_median(df_data, region_type="prov")

    for prov_id in prov_ids:
        if prov_id not in df_data.index:
            # 用省下辖市的数据来填充
            if prov_by_sub_citys:
                sub_ids = get_prov_sub_city_ids(prov_id)
                if not sub_ids:  # 直辖市
                    df_data = df_data.reindex(df_data.index.values.tolist()+[prov_id])  # 先把索引加进去，具体字段为Nan，之后补中位值
                else:
                    df_data = add_some_id_data(df_data, prov_id, sub_ids, by)
            # 用省下辖区县的数据来填充
            else:
                sub_ids = get_prov_sub_county_ids(prov_id, with_hitec=with_hitec)
                df_data = add_some_id_data(df_data, prov_id, sub_ids, by)

    # 考虑到直辖市存在补不上的情况 —— 如果用市的数据来补的话
    prov_median = df_data.loc[prov_ids, :].median().to_dict()
    df_data.loc[prov_ids, :] = df_data.loc[prov_ids, :].fillna(value=prov_median)

    # 补上全国的数据
    if with_country:
        if 0 not in df_data.index:
            df_data = add_some_id_data(df_data, 0, prov_ids, by)

    df_whole_geo = get_all_gov_id_info(with_hitec)

    df_data = df_data.reindex(df_whole_geo.index)

    return df_data


def add_gov_name_n_code_based_on_gov_id(df_data, name_only=False):
    # 都以gov_id为index，可直接赋值
    df_data["gov_name"] = df_2861_gaode_geo_all["full_name"]
    if not name_only:
        df_data["gov_code"] = df_2861_gaode_geo_all["gov_code"]

    return df_data


def get_pct_rank_regionally(data_df, rank_ascending=True, with_hitec=False):
    df_county = get_all_county_gov_id_info(with_hitec=with_hitec)
    df_city = get_all_city_gov_id_info()
    df_prov = get_all_province_gov_id_info()

    data_df.loc[df_county.index, "pct_rank"] = data_df.loc[df_county.index, "value"].rank(method="max", pct=True, ascending=rank_ascending)*100
    data_df.loc[df_county.index, "rank"] = data_df.loc[df_county.index, "pct_rank"].rank(method="min", ascending=False)

    data_df.loc[df_city.index, "pct_rank"] = data_df.loc[df_city.index, "value"].rank(method="max", pct=True, ascending=rank_ascending)*100
    data_df.loc[df_city.index, "rank"] = data_df.loc[df_city.index, "pct_rank"].rank(method="min", ascending=False)

    data_df.loc[df_prov.index, "pct_rank"] = data_df.loc[df_prov.index, "value"].rank(method="max", pct=True, ascending=rank_ascending)*100
    data_df.loc[df_prov.index, "rank"] = data_df.loc[df_prov.index, "pct_rank"].rank(method="min", ascending=False)

    return data_df


if __name__ == "__main__":
    # get_hot_city_infos(True)

    # get_all_county_gov_id_info()
    # get_all_city_gov_id_info(True)
    # get_all_province_gov_id_info(True)
    # get_gov_id_info(2,True)

    # gov_id = 2057
    # print(get_sub_gov_id_info(gov_id, PROVINCE_SUB_CITY))
    # print(get_sub_gov_id_info(gov_id, PROVINCE_SUB_COUNTY))
    # print(get_sub_gov_id_info(gov_id, CITY_SUB_COUNTY))
    # print(get_parent_gov_id_info(2058))

    print(get_prov_sub_city_ids(1))
    pass

