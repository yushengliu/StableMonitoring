#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/5/10 19:17
@Author  : Liu Yusheng
@File    : NlpApi.py
@Description:  尝试了几个公开的文本分析接口，总结各类
"""
import pandas as pd
import numpy as np
# 玻森nlp
from bosonnlp import BosonNLP
# 百度nlp
from aip import AipNlp
import requests
import json
import time


class BosonNlpApi:
    __TOKEN = r"qe_p2XFd.34316.cAnsxiFL9yr7"
    RATE_LIMIT_URL = "http://api.bosonnlp.com/application/rate_limit_status.json"
    SENT_GENERAL_URL = "http://api.bosonnlp.com/sentiment/analysis"
    SENT_DAY_LIMIT = 500

    def __init__(self, texts, model="weibo"):
        self.texts = texts
        self.model = model

    @staticmethod
    def senti_limits_remaining():
        HEADERS = {"X-Token": BosonNlpApi.__TOKEN}
        result = requests.get(BosonNlpApi.RATE_LIMIT_URL, headers=HEADERS).json()

        return result["limits"]["sentiment"]["count-limit-remaining"]

    # 玻森情感分析接口
    def senti_by_sdk(self):
        nlp_obj = BosonNLP(self.__TOKEN)
        senti_results = nlp_obj.sentiment(self.texts, model=self.model)
        print(senti_results, flush=True)

        # 查验剩余调用次数
        limit_remain = self.senti_limits_remaining()
        print("BosonNLP 剩余调用次数：{}".format(limit_remain), flush=True)

        return senti_results

    # 用http的方式，看调用和剩余次数 —— 调用和sdk损耗次数是一样的。。。以及试过后发现，model="weibo"比“general"更好
    def senti_by_http(self):
        SENTIMENT_URL = self.SENT_GENERAL_URL if self.model =="general" else self.SENT_GENERAL_URL+"?{}".format(self.model)
        HEADERS = {"X-Token": self.__TOKEN}

        # 文本数据包装成json
        data = json.dumps(self.texts)

        # http访问返回
        resp = requests.post(SENTIMENT_URL, headers=HEADERS, data=data.encode("utf-8"))

        senti_res = resp.text
        print(senti_res, flush=True)

        # 查验剩余调用次数
        limit_remain = self.senti_limits_remaining()
        print("BosonNLP 剩余调用次数：{}".format(limit_remain), flush=True)

        return eval(senti_res)


class BaiduNlpApi:
    __APP_ID = '16222729'
    __API_KEY = 'dQ5XOnC1aV8KXWi5Yqj8MbrB'
    __SECRET_KEY = '4WvLdHbBGiIb9yT9lEZvS2hGoqLM6mPr'
    # 又申请了一个
    __APP_ID_1 = '16236180'
    __API_KEY_1 = 'IDfyuuxE381P8wHVGiZiUtkx'
    __SECRET_KEY_1 = 'T9z0AE6hEZeDrlvyrsafqHox9TnwCNxN'

    __APP_IDS = ['16222729', '16236180']
    __API_KEYS = ['dQ5XOnC1aV8KXWi5Yqj8MbrB', 'IDfyuuxE381P8wHVGiZiUtkx']
    __SECRET_KEYS = ['4WvLdHbBGiIb9yT9lEZvS2hGoqLM6mPr', 'T9z0AE6hEZeDrlvyrsafqHox9TnwCNxN']

    def __init__(self, texts, id_num=0):
        self.client = AipNlp(self.__APP_IDS[id_num], self.__API_KEYS[id_num], self.__SECRET_KEYS[id_num])
        self.texts = texts

    def set_texts(self, texts):
        self.texts = texts
        return self

    def senti_auto_split(self):
        def get_senti_api(text):
            for retry in range(10):
                try:
                    ret = self.client.sentimentClassify(text)
                    senti_items = ret["items"][0]
                    return senti_items
                except Exception as e:
                    if 'ret' in locals().keys() and 'error_code' in ret:
                        if ret['error_code'] in [18]:
                            time.sleep(1)
                            continue

                        print('err', ret['error_code'], e, text)
                        time.sleep(1)
                        continue

                    else:
                        print('err', e, text)
                        time.sleep(10)
                        continue
            else:
                return None

        senti_results = []
        for text in self.texts:

            text_len = len(text)
            if text_len < 1000:
                a_text_list = [text]
            else:
                s_ = 0
                a_text_list = []
                for offset in range(1000, text_len + 1, 1000):
                    a_text_list.append(text[s_: offset])
                    s_ = offset
                if s_ < text_len:
                    a_text_list.append(text[s_: text_len])

            total_pos = 0
            total_neg = 0
            for frag_cnt, frag in enumerate(a_text_list, start=1):
                ret = get_senti_api(frag)
                if ret:
                    total_pos += ret["positive_prob"]
                    total_neg += ret["negative_prob"]
                else:
                    total_pos = -1
                    total_neg = -1
                    break
            else:
                total_pos /= frag_cnt
                total_neg /= frag_cnt

            print(total_pos, total_neg, text)
            senti_results.append([total_pos, total_neg])

        return senti_results

    # 情感分析接口
    def senti_by_sdk(self, probs_only=True):
        """
        +sentiment	是	number	表示情感极性分类结果, 0:负向，1:中性，2:正向
        +confidence	是	number	表示分类的置信度
        +positive_prob	是	number	表示属于积极类别的概率
        +negative_prob	是	number	表示属于消极类别的概率
        :return:
        """
        senti_results = []
        for text in self.texts:
            senti_items = []
            n = 10

            while n > 0:
                try:
                    ret = self.client.sentimentClassify(text)
                    senti_items = ret["items"][0]
                    break
                except Exception as e:
                    print(e)
                    n -= 1
                    continue

            if senti_items:
                if not probs_only:
                    senti_results.append(senti_items)
                else:
                    senti_results.append([senti_items["positive_prob"], senti_items["negative_prob"]])
            else:
                if not probs_only:
                    senti_results.append({'positive_prob': -1, 'confidence': 0, 'negative_prob': -1, 'sentiment': -1})
                else:
                    senti_results.append([-1, -1])
        return senti_results

    # 分类 - 仅返回最终大类
    def category_by_sdk(self, texts_data):
        def get_senti_api(text):
            title, text = text

            for retry in range(10):
                try:
                    ret = self.client.topic(title, text)
                    print(ret, flush=True)
                    category_info = ret["item"]['lv1_tag_list'][0]
                    return category_info
                except Exception as e:
                    if 'ret' in locals().keys() and 'error_code' in ret:
                        if ret['error_code'] in [18]:
                            time.sleep(1)
                            continue

                        elif ret['error_code'] in [282131]:
                            # text = text[:50]
                            # title不超过80字节 - 即20个汉字
                            title = title[:20]
                            time.sleep(1)
                            continue

                        print('err', ret["error_code"], e, text)
                        time.sleep(1)
                        continue
                    else:
                        print('err', e, text)
                        time.sleep(10)
                        continue
            else:
                return None

        cat_results = []
        for title, text in texts_data:
            text_len = len(text)
            if text_len < 1000:
                a_text_list = [[title, text]]
            else:
                s_ = 0
                a_text_list = []
                for offset in range(1000, text_len + 1, 1000):
                    a_text_list.append([title, text[s_: offset]])
                    s_ = offset
                if s_ < text_len:
                    a_text_list.append([title, text[s_: text_len]])

            category_total = {}
            for frag_cnt, frag in enumerate(a_text_list, start=1):
                ret = get_senti_api(frag)
                if ret:
                    if ret['tag'] in category_total:
                        category_total[ret['tag']] += ret['score']
                    else:
                        category_total[ret['tag']] = ret['score']

            if len(category_total) == 0:
                cat_results.append(-1)
            else:
                ret_cat = max(category_total, key=category_total.get)
                print(ret_cat, category_total[ret_cat], text)
                cat_results.append(ret_cat)

        return cat_results

    # 分类 - 返回大类和子类 类别信息+概率
    def category_with_more_info(self, texts_data):
        def deal_with_ret(ret_data):
            """
            @功能：将返回数据格式，转换成最终存储入库的格式
            :param ret_data:
            :return:
            """
            aim_dict = dict()
            if ret_data is not None:
                category_info = ret_data["item"]
                aim_dict["category"] = category_info["lv1_tag_list"][0]["tag"]
                aim_dict["cate_score"] = category_info["lv1_tag_list"][0]["score"]
                aim_dict["sub_cates"] = category_info["lv2_tag_list"]
            else:
                aim_dict["category"] = -1
                aim_dict["cate_score"] = -1
                aim_dict["sub_cates"] = []

            return aim_dict

        def get_cate_api(text):
            """
            @功能：调接口分类
            :param text:[title, content]
            :return:
            """
            title, content = text

            for retry in range(10):
                try:
                    ret = self.client.topic(title, content)
                    aim_dict = deal_with_ret(ret)
                    return aim_dict
                except Exception as e:
                    if 'ret' in locals().keys() and 'error_code' in ret:
                        # 请求超过QPS限额
                        if ret["error_code"] == 18:
                            time.sleep(1)
                            continue
                        # # 文本超限 - 在外侧处理
                        # elif ret["error_code"] == 282131:
                        #     # 正文在外部切段处理，这里报错一定是因为标题超限
                        #     title = title[:20]
                        #     continue

                        print("err", ret["error_code"], e, text)
                        time.sleep(1)
                        continue

                    else:
                        print("err", e, text)
                        time.sleep(10)
                        continue
            else:
                return None

        def seg_text(title, content):
            """
            @功能：把文本按长度限制切段
            :param title:
            :param content:
            :return:
            """
            CONTENT_LIMIT = 2000
            TITLE_LIMIT = 20

            text_segments = []

            title = title[: TITLE_LIMIT]

            for c in range(0, len(content), CONTENT_LIMIT):
                text_segments.append([title, content[c: c+CONTENT_LIMIT]])

            return text_segments

        def merge_same_subcates(subcates_list):
            """
            @功能：相同的二级分类合并（键保留一个，概率相加） - 针对长文本切段分类的情况可能出现
            :param subcates_list:
            :return:
            """
            if not len(subcates_list):
                subcates_list_ = subcates_list

            else:
                df_subcates = pd.DataFrame(subcates_list)
                df_subcates_gb = df_subcates.groupby("tag", as_index=False).agg({"score":"sum"})
                df_subcates_gb = df_subcates_gb.sort_values(by="score", ascending=False).reset_index(drop=True)
                subcates_list_ = df_subcates_gb.to_dict(orient="records")

            return subcates_list_

        def deal_seg_results(cates_info_list):
            """
            @功能：根据切段了的分类结果，融合成原文本的结果
            :param cates_info_list:
            :return:
            """
            df_segs = pd.DataFrame(cates_info_list)
            df_segs_groupby = df_segs.groupby("category", as_index=False).agg({"cate_score": "sum", "sub_cates": lambda x: sum(x, [])})

            df_segs_groupby = df_segs_groupby.sort_values(by="cate_score", ascending=False)  # .reset_index(drop=True)

            # sub_cates去重
            df_segs_groupby["sub_cates"] = df_segs_groupby["sub_cates"].apply(lambda x: merge_same_subcates(eval(x)) if isinstance(x, str) else merge_same_subcates(list(x)) if isinstance(x, np.ndarray) else merge_same_subcates(x))

            final_cate_info = df_segs_groupby.iloc[0].to_dict()
            return final_cate_info

        final_cates = []
        for title, content in texts_data:
            texts = seg_text(title, content)

            seg_cates_results = []
            for text_ in texts:
                aim_cate_dict = get_cate_api(text_)
                seg_cates_results.append(aim_cate_dict)

            cate_dict = deal_seg_results(seg_cates_results)
            final_cates.append(cate_dict)

        return final_cates

    def newsSummary(self, max_summary_len=100):
        def get_newsSummary_api(text):
            for retry in range(10):
                try:
                    ret = self.client.newsSummary(text, max_summary_len)
                    summary = ret["summary"]
                    return summary
                except Exception as e:
                    if 'ret' in locals().keys() and 'error_code' in ret:
                        if ret['error_code'] in [18]:
                            time.sleep(1)
                            continue

                        print('err', ret["error_code"], e, text)
                        time.sleep(5)
                        continue
                    else:
                        print('err',  e, text)
                        time.sleep(30)
                        continue
            else:
                return None

        summary_results = []
        for text in self.texts:
            if len(text) > 2000:
                # summary_results.append(None)
                # continue
                text = text[:2000]

            ret = get_newsSummary_api(text.encode("GBK", errors='ignore').decode('GBK'))
            summary_results.append(ret)

        return summary_results


if __name__ == "__main__":

    baidu_test = ["【华为向美通信运营商索取专利费】多家外媒报道称，今年二月给美国通信运营商Verizon及其供应商发去信件，要求为其所使用的230余项华为专利支付10亿美金的使用费。专利涉及核心网络设备、有线基础设施和物联网技术等。上周华为已经与Verizon探讨过相关问题，而Verizon拒绝公布会议内容，并已经将此事上报美国政府，称该事件涉及地缘政治问题。", "【技术说话！华为收专利费时代到来 索要美运营商10亿专利费】据《华尔街日报》消息人士透露，华为要求美国电信运营商Verizon通讯公司支付230多项专利授权费，总金额超过10亿美元。这些专利涉及了Verizon及其20多家网络设备供应商，其中包括一些美国主要的科技公司。", "【华为要求美电信巨头支付10亿美元专利费】“消息人士透露，华为已经要求美国电信运营商威瑞森通讯公司(Verizon)支付 230多项专利授权费，总金额超10亿美元。”12日，这条消息从美国《华尔街日报》发出，迅速被路透社等媒体转载，成为华为被美国政府列入实体名单事件的最新进展。目前，华为公司尚未对此消息做出回应。", "【华为要求美电信巨头Verizon支付超10亿美元专利许可费】据环球网援引《华尔街日报》等媒体报道称，消息人士透露，华为已告知美国电信巨头Verizon，要求其为逾230项华为专利支付授权费，总计金额超过10亿美元。截至记者发稿，华为方面未对此作出正式回应。", "【连遭美国打压 华为反击：要美电信巨头支付10亿美元专利费】 据环球网报道，消息人士透露，华为已经要求美国电信运营商威瑞森通讯公司支付 230多项专利授权费，总金额超10亿美元。6月12日，这条消息从美国《华尔街日报》发出，迅速被路透社等媒体转载，成为华为被美国政府列入实体名单事件的最新进展。", "【10亿美元！华为要求美国最大电信运营商Verzion支付专利费】路透社12日报道称，华为已要求Verizon支付230多项专利的费用，总计超过10亿美元，金额相当于Verizon去年四季度净利润的一半。知情人士称，Verizon虽然不是华为的客户，但其20多个供应商在核心网络设备、有线基础建设和物联网技术三个领域所使用的的设备和技术，涉嫌侵害华为的专利权。", "【华为要求美运营商付10亿美元专利费】据外媒报道，华为要求美国最大移动运营商Verizon支付230多项专利的费用，超过10亿美元。Verizon不是华为的客户，但其供应商在核心网络设备、有线基础建设和物联网技术领域涉嫌侵害华为的专利权。", "【 ：超十亿美元，涉及230项专利】据外媒报道，华为要求美国最大移动运营商Verizon支付230多项专利的费用，超过10亿美元。Verizon不是华为的客户，但其供应商在核心网络设备、有线基础建设和物联网技术领域涉嫌侵害华为的专利权。（老板联播）", "其他"]

    # nlp_obj = BaiduNlpApi(baidu_test)
    # print(nlp_obj.newsSummary())

    event_titles_ = ['祷告 怒气 相信 死亡 人们 赐给', '蜀黍 调查 活动 参与 节目 提供 供电 千家万户', '探讨 民生 卢作孚 科学院 理化地质', '纠纷 婚恋 嫌疑人 现场 犯罪', '看到 光头 左某 盗窃 电瓶车 赃款', '维护 发展 村庄 黑心 风景 妥善 处理 破坏 生活 用水 截流', '论坛 带来 失踪 回家 转发 掌纹 希望', '消防员 坠井 担架 救援 指战员 人员 利用 现场 中队', '时间段 油管 真不知道 差不多 爆炸 不够', '明磊 父亲 母亲 丽丽 被告人', '遇弯 注意 拖拉机 交警 货车 路段 赶集 欢聚 出行']

    event_contents_ = ['新京报快讯，7月18日晚，甘肃省临夏回族自治州多地发生特大暴雨，其中广河、东乡县灾情严重，东乡县最大降水量为113.8毫米。截至7月19日7时，暴雨已造成当地7人死亡、8人失踪、22人受伤住院治疗。通报中称，7月18日晚，东乡县发生暴雨灾害，最大降水量为113.8毫米，导致果园、达板等乡镇部分农家被山洪冲毁。据7月19日凌晨3:18分统计，已确认7人死亡(其中：果园镇4人、达板镇3人)，8人失踪(其中：果园镇4人、达板镇3人、凤山乡1人)，22人受伤住院治疗···（视频仅供参考）我们一同为中国各种的自然灾害而献上恳切的祷告，求主怜悯侧耳垂听：亲爱的恩主啊！我们要感谢赞美你！因为你是我们的神，你也是普天下的人的神，因为万物都是按着你的心意所创造，人内也是按着你的形象所造，主啊，你让我们看到这样自然灾害的环境，这说明你来的日子真的近了。主啊我们中国还有很多很多的人还没有信靠你，主啊求你不要将我们的中国百姓丢下，主啊中国本是属于你的，中华本是神洲大地，是属于你的，求主以你的慈杯为怀，求你怜悯中国的百姓，减少这些自然灾害，将你的恩典赐给我们，给我们留下悔改机会。主啊求你赦免我们中国人不信你的罪。赦免我们犯的一切的罪，拯救灾区的人们，保护救助队的每一个人的人身安全，赐福给我们的同胞，给他们众人悔改相信的心，愿赦罪的恩临到他们。求你不要按我们的过犯对待我们，主啊，求你可怜我们，东离西有多远，你叫我们的过犯离我们有多远，天离地有多高，你的慈爱就有多高。主啊我们切切求告你，切切的求你可怜我们，救我们脱离罪恶。脱离死亡。赐给我们永远的生命。全能的上帝啊！天上地上除你以外，我们没有别的名可以靠着得救，耶和华的名是应当永远称颂的。因为你是天地万物的主宰，主啊，恳求你收回怒气，饶恕人们悖逆无知、不认真神、贪恋世界的罪过。主啊！这世上满了淫乱和邪恶，你都知道。恳求你掩面不看我们的亏欠和你罪恶，让人从灾难中看出有一位真神创造主的存在。主啊！凡你手所造的相信你都看顾保守。主，求你看顾神州大地，免去世上一切灾难，你的怒气在眨眼之间，你的慈爱却直到永远！全能慈爱的主啊！恳求你保守中国，眷顾中国960万平方公里土地上的你千千万万儿女，因为除你以外我们别无拯救！你是我们的避难所！你是我们生命的保障！恳求主你垂听我祷告的话语，让我们的祈求上达到你天庭宝座，震动你的耳旁，摇动你的双臂。主你说有就有，你命立就立，主你的话语都带着能力，带着权柄。主啊：求祢赐给我们中国教会警醒祷告的心，求祢赐给我们中国教会敬畏尊崇的心，担当福音的使命，作代祷的祭司。愿祢在我们中国行走、来往、居住、掌权！愿荣耀、尊贵、权柄、能力、国度，都归给祢！直到永永远远，世世代代！求你垂听我们的祷告，按照你的旨意成全在我们这片神州大地上，感谢你与我们同在，以上祷告，奉救主耶稣的圣名所求，阿们！', '【非常满意！你就一定要说给供电蜀黍听！】  9月26日晚，南方电网吴川供电蜀黍将在吴川金沙广场中庭开展满意度调查宣传活动——“电”亮幸福生活，满意千家万户。供电蜀黍将会认真倾听您的意见和建议，为您提供更优质的服务。在今晚的活动现场，除了能看到精彩的表演，还能参与趣味活动，更能领取一份供电局的礼品！供电蜀黍的活动有意义又有趣，期待你来参与哦！ 活动时间： 活动地点：湛江市吴川市金沙广场中庭', '中国第一家民办科学院中国西部科学院，1930年10月，在重庆北碚创立，卢作孚出任院长。科学院以“从事于科学之探讨，以开发宝藏，富裕民生”为目的，下设理化、地质、生物、农林4个研究所。科学院经费，主要依靠峡防团务局、民生公司、省教育经费等补助以及军政各界和私人捐款。科学院的标志建  ...全文', '军转警官的亮剑精神——记曾都区府河镇派出所教导员罗云中随州日报通讯员 姜育庆脱下戎装，换上警服；扎根基层，求知创新；废寝忘食，有警必出……十二年如一日，舍小家顾大家，他是辖区群众的守护神，他是单位同事的定盘心，他就是“军转”铁警——罗云中。2007年罗云中从部队连职军转至随州市曾都区府河镇派出所，他义无反顾、默默扎根基层一干就是十二年，一直是分局的业务尖兵和先进模范，被同事和辖区群众公认为“拼命三郎”和“铁包公”，更让违法犯罪分子谈之色变，闻风丧胆。他先后荣记个人三等功1次、嘉奖5次、多次被评为优秀公务员、先进工作者，该所还被荣记集体三等功，成绩斐然。打击犯罪保一方平安2018年9月，罗云中由组织决定主持全所工作，临危受命，困难重重。府河镇位于曾都区、广水市、安陆市三个市区接壤的治安复杂地区，治安管理难度十分巨大。派出所工作责任重于泰山，罗云中在队伍管理上实行军事化管理，装备摆放整齐划一，办公环境宽敞明亮，在业务工作中雷厉风行，令行禁止。2019年1月9日12时许，根据群众举报，罗云中迅即带领民警，将乘坐皮卡车打猎的犯罪嫌疑人宋某、刘某抓获，缴获单管猎枪、双管猎枪各一支、子弹24发、对讲机三部、皮卡车一辆。2018年12月13日10时许，罗云中通过布控，抓获自当年9月至12月多次盗窃家禽的流窜犯李某、白某，并扣押弓弩、毒镖等作案工具，破案多起，群众拍手称快。警民情深为百姓解难2017年，府河桥头，六层住宅楼因煤气罐泄露起火，接警后罗云中迅即带领民警第一时间赶到现场。住户厨房煤气罐正喷着浓浓大火，被烧得通红、滚烫的煤气罐随时有爆炸的危险，紧要关头，罗云中带领民警们一边疏散群众，一边不顾安危冲进屋里，用湿毛巾包住滚烫的煤气罐拧紧阀门，抬出室外进行扑灭工作，及时排除了险情，赢得了在场群众的一致称赞。2019年3月份的一天，一位姓魏的大爷提了一篮土鸡蛋专程到派出所找到罗云中，魏大爷说：“小罗呀，好几年，我家养鸡，别说吃鸡蛋，鸡都被贼惦记走了。去年你们加强了防范。鸡没有被盗。一开年，鸡下的蛋都吃不完。今天我拎一点给你们尝尝鲜。”罗云中一再推脱，魏大爷执意要送，罗云中只好收下这份心意，悄悄地把钱塞进大爷的口袋。细微之处见真情！老大爷的举动是辖区百姓对治安稳定满意度的认可。勇搏激流 显一身正气府河镇偏僻山区常常是赌徒聚集之地，在日常治安防控工作中，罗云中定下规矩“对赌博零容忍，见赌必打，不留情面”。同时利用交通管理职能，对过往车辆严防严查，跟踪可疑车辆，查酒驾，查吸贩毒人员，让偷鸡摸狗人员无处藏身。在其主持工作期间，该所共查处赌博案3起，处罚赌博人员28人；查处醉驾6起、酒驾4起、无证驾驶5起、交通违章100余起；在治砂行动中10余人受到处罚，受到了党委政府的高度肯定。罗云中以一名共产党员的忠诚和担当，以忘我的精神保一方平安，以高昂的斗志带领府河派出所迈向更高的台阶！ 随州·随州市中心医院', '【咦，这个光头不是我吗？】近日，上海市公安局浦东分局惠南派出所抓获了一盗窃嫌疑人左某。被捕后，左某第一时间企图狡辩，可看到作案视频中自己那明晃晃的大光头，心里顿时拔凉拔凉地。接下来，这个大光头表现得非常“自觉”，供述了盗窃行为、从卡内领出了赃款、还交出用赃款购买的电瓶车。@警民直通车-浦东', '新邵县某功利部门招引黑心开发商罔视民意要在潭府乡车峙村老龙潭截流引水，此举首先破坏天赐大自然秀丽风景破坏生态平衡发展，二视该流域三村约五千多人生存与发展于不顾，已引公愤。广大人民正自发组织护水维权，希上层领导重视并妥善处理以免激发矛盾造成不可收拾的局面。', '姓    名：黄    性    别：女 出生日期：1994年10月10日 失踪时身高：未知 失踪时间：1994年10月10日 失踪人所在地：广东省,潮州市 失踪地点：广东省,潮州市,广东省潮州市潮安县金石镇大寨村     现身高：153厘米日前体重：53KG     其他特征描述：一个发旋，后脑平   小眼睛   塌鼻子    双手不是断掌纹【求助】约1994年10月出生后被送养到广东省潮州市潮安县金石镇黄寻亲330013(出处: 宝贝回家论坛)请大家转发各群及相关论坛，并将转发后的链接回复在帖子里。您的每一次转发都给孩子回家带来一份希望，谢谢 ！！！', '#延安身边事# 【六旬老人不慎坠井  延安消防火速救援】2019年3月5日11时28分延安消防支队子长中队接市消防119指挥中心调度称：子长县西门平靠近火车洞旁有人坠井。时间就是生命、灾情就是命令，中队立即两辆消防车，12名指战员火速赶赴现场展开救援。#延安新闻# 到达事故现场，经现场勘察，一名六十多岁的老太太不慎落入枯井中，井深约有3米，井口宽两米左右，左手手骨骨折，意识较为清醒。随即，指挥员下令展开救援，首先由中队特勤班班长首先利用安全绳进入到井下，安抚被困人员情绪，并对被困人员进行简单的伤口处理，随后一名指战员携带担架进入井下，为防止被困人员受到二次伤害，两名指战员小心翼翼将其放置到担架上，随后中队指战员利用担架、绳索、挂钩等工具于12时21分成功将被困人员救出，随后中队指战员将伤员移交现场医护人员。（by：延安消防）', '贵州省黔西南晴隆县沙子岭2018年6月10号再次发生的油管爆炸 去年差不多时间段发生了第一次，今年又发生了，是去年死伤的人数还不够吗，这血的教训还不够深刻？真不知道这些技术人员是做什么的。我觉得真的很有必要深查。', '【愤怒！西宁一女孩多次被亲生父亲强奸至怀孕！最终…】家丑不可外扬。家住大通回族土族自治县某村的丽丽（化名）就因这样的想法，让自己本不该承受的伤害发展到了最大化，让自己本不该承受的屈辱发酵到了极限。但最终纸没有包住火，她想竭力隐藏的“家丑”被揭开。丽丽，她的人生本应充满阳光，像绽放的花儿一样绚烂。然而从2015年1月至2017年4月间，她多次被亲生父亲强奸，为了不让家丑外扬，丽丽未曾提起。懵懵懂懂的她不知自己怀有身孕，直到2017年9月份，丽丽被他人发现怀有身孕，由此“窗户纸”被捅破，丽丽不得不将事情一五一十地告诉家人。经医院检查，丽丽当时已经怀孕8个月，同年10月产下一子。2017年9月18日，丽丽父亲明磊（化名）被大通县公安局刑事拘留，26日被依法逮捕。今年1月12日，大通县人民检察院以明磊涉嫌强奸罪向县人民法院提起公诉。2月8日，大通县法院经审理后依法对被告人明磊作出判决，被告人明磊犯强奸罪判处有期徒刑13年，剥夺政治权利3年。当民警问及，作为父亲，又是亲生女儿，怎么下得了手？这位父亲却回答说：“没有压制住自己内心的欲望。”民警问道：“后悔吗？”这位父亲回答：“追悔莫及。”父亲强奸亲生女儿一事终究被揭开，正所谓，若要人不知，除非己莫为。据了解自2015年起对于当时年仅16岁的丽丽（女儿化名）来说，多姿多彩的生活还未开启，就已凋零；这年，丽丽的父亲明磊（父亲化名）和母亲为了生计到哈密市打工，租住了一处民宅。一个月后，丽丽在母亲的带领下来到了哈密市，一家三口住在了一起。有一天，明磊喝醉了酒，次日清晨，酒后未清醒的他看着丽丽楚楚动人的背影，突然萌生一股冲动，乘着丽丽母亲外出打工，将丽丽强奸了。丽丽挣扎过、反抗过，但没有什么作用，仍被父亲明磊强行发生了性关系。两个月后，丽丽回到大通县上学，此事便搁置了。其间，丽丽想过跟母亲谈谈，但由于内心的耻辱感，加上涉事的是自己的父亲，她实在难以启齿，便把所有的事压在了心底。就这样，丽丽以为只要自己不说，事情就会很快过去，但现实远没有她想的这么简单，她的噩梦没有结束而是才开始。2015年冬天，丽丽的母亲和父亲打工结束，回到大通的家后母亲回娘家看看，明磊再次强行与丽丽发生性关系。丽丽再次选择了沉默，周而复始，明磊在此后的一段时间里，乘着家人外出或睡觉之际，多次强行与丽丽发生性关系。其间，明磊还威胁丽丽，不要将事情告诉他人。丽丽的沉默、明磊的威胁，能否隐藏家丑？2017年9月16日，丽丽跟随母亲到西宁市的一家小餐馆打工。老板娘发现丽丽怀孕，便让丽丽的母亲带她去医院看看。母亲再三追问，丽丽都只是沉默不语。一夜后，丽丽经过思想斗争决定把事情原委说出。当她把事情的经过说完后，心里感觉到无比轻松，但她的母亲却是目瞪口呆、无比心痛。一个是丈夫、一个是女儿，两个人都是自己的亲人，但有违伦理的事情却在自己家中发生了，顿感晴天霹雳。经过反复考虑，丽丽和母亲决定报警。2017年9月18日，丽丽向大通县公安局电话报警。当日，明磊以涉嫌强奸罪被大通县公安局依法刑事拘留。2018年2月8日，大通县法院依法不公开开庭审理了此案。法院审理认为，被告人明磊作为与被害人有直接血缘关系的监护人，不但没有履行好监护义务，反而违背人伦，利用监护关系，采取暴力手段，多次强行与被害人发生性关系，致其怀孕生子，其行为已构成强奸罪，应予从重惩处，依法判处被告人明磊有期徒刑13年，剥夺政治权利3年。俗话说“虎毒不食子”，但是面对这样的父亲，我们除了愤怒之外，是不是更多是应该反思一下，悲剧的根源在哪里？！父亲,竟然这样对待自己的亲生女儿,真是令人发指!但作为弱势群体的女性到底应该如何保护自己？快留言说说你的看法吧！', '目前，衡阳县辖区交通状况正常，交警温馨提示：春节期间，亲友欢聚，举家团圆增多，请不要酒后开车、醉酒驾驶，  不要超员载客、  在货厢违法载人，遇弯坡路段请提前减速，  注意观察道路情况，在确保安全的情况下慢速通过。']

    texts_data = [[event_titles_[i], event_contents_[i]] for i in range(0, len(event_contents_))]

    test_title = "".join(event_titles_)
    # test_content = "".join(baidu_test)

    test_content = ""

    texts_data_ = [[test_title, test_content]]

    # results = BaiduNlpApi([]).category_by_sdk(texts_data)
    # df_res = pd.DataFrame(results)

    # results = BaiduNlpApi([]).category_with_more_info(texts_data)
    # print(pd.DataFrame(results))

    results_ = BaiduNlpApi([]).category_with_more_info(texts_data_)
    print(pd.DataFrame(results_))
    print(results_[0]["sub_cates"])

    # print(df_res, flush=True)

    # senti_results = BaiduNlpApi(event_contents_).senti_auto_split()
    #
    # df_senti = pd.DataFrame(senti_results)
    #
    # # print(len(event_contents_[0]), flush=True)
    #
    # summary_results = BaiduNlpApi(event_contents_).newsSummary(max_summary_len=80)
    #
    # # print(summary_results, flush=True)
    #
    # df_summary = pd.DataFrame(summary_results)
    #
    # df_whole = pd.concat([df_res, df_senti, df_summary], axis=1)
    #
    # print(df_whole, flush=True)







