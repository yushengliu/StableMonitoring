#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2019/10/30 16:50
@Author  : Liu Yusheng
@File    : parameters.py
@Description:
"""

# INDEX_NAMES = {"law": "社会法治化"}


def transform_and_relation_to_regex(relation_str):
    parts = relation_str.split("and")
    regex_str = ""
    for part_ in parts:
        # if part_.startswith("("):
        #     regex_str += "(?=.*{})".format(part_.replace())
        regex_str += "(?=.*{})".format(part_.replace("or", "|"))

    regex_str = "({})".format(regex_str)

    return regex_str


def transform_or_relation_to_regex(relation_str):
    regex_str = relation_str.replace("or", "|")
    return regex_str


SP_DISPOSAL_INDEXES = ["economics", "elite_get", "fair_protect"]
UNIFORM_DISPOSAL_INDEXES = ["politics", "law", "culture", "env", "party", "resource_get", "law_put", "supervise_put", "scientific_decide", "crisis_respond", "politics_renew", "system_build", "accept_put", "politics_integrate", "strategy_plan", "politics_communicate"]


class KeyWords:
    # 政治民主化
    politics_words = ['((?=.*(民主|政党|人大|政府|政协|人民团体|基层|社会组织))(?=.*(协商)))', '((?=.*(民主))(?=.*(制度|权利|形式|选举|协商|管理|监督|渠道|知情|参与|表达|意识|观念|作风|集中制)))', '((?=.*(坚持|完善))(?=.*(党))(?=.*(领导)))', '((?=.*(党员))(?=.*(主体))(?=.*(地位)))', '((?=.*(党代表))(?=.*(大会))(?=.*(制度)))', '((?=.*(党))(?=.*(选举))(?=.*(制度)))', '((?=.*(党))(?=.*(集中))(?=.*(统一)))', '((?=.*(党))(?=.*(民主))(?=.*(决策))(?=.*(机制)))', '知情权|参与权|表达权|监督权', '((?=.*(接受))(?=.*(党|人民))(?=.*(监督)))', '人民公仆', '((?=.*(权利))(?=.*(来源))(?=.*(人民)))', '((?=.*(树立))(?=.*(民主观)))', '((?=.*(人民|党员))(?=.*(民主))(?=.*(第一)))', '((?=.*(党内))(?=.*(平等)))', '个人服从党|少数服从多数', '((?=.*(下级))(?=.*(服从))(?=.*(上级)))', '((?=.*(组织|党员))(?=.*服从)(?=.*(全国代表大会|中央|全国人民代表大会)))', '((?=.*领导)(?=.*选举)(?=.*产生))', '((?=.*党)(?=.*最高)(?=.*(全国代表大会|全国人民代表大会|中央)))', '((?=.*上级)(?=.*(听取|解决))(?=.*下级)(?=.*(意见|问题)))', '((?=.*集体领导)(?=.*个人分工))', '((?=.*禁止)(?=.*个人)(?=.*崇拜))', '((?=.*法律)(?=.*平等))', '((?=.*(权利|言论|出版|集会|结社|游行|示威|宗教|信仰|文化|教育|科研|文艺|活动))(?=.*自由))', '((?=.*(人身|人格|尊严|住宅|住房|房屋|房子|通信|秘密))(?=.*(维护|保护|侵犯|侵害)))', '保护权|保障权|权利|权益|选举权|被选举权|人格权|监督权|生活保障|社会保障|社保']
    politics = "|".join(politics_words)

    # 社会法治化 - 不用筛选关键词
    law = ""

    # 经济市场化 - 用别的指数

    # 文化多样化
    culture_words = ['((?=.*(古代|现代|精英|大众|官方|民间))(?=.*文化))', '((?=.*(科技|车|珠宝|时尚|影像|摄影|设计|咖啡|茶|艺术|文化|体验|个|书|画|文物|美术|沉浸|设计|作品|动漫|漫画|主题|电竞|精品|互动|文学|纪念|植物|创意|美食))(?=.*(节|展|周|演出)))', '演唱会|摇滚|音乐节|音乐会|视听会|演奏会|清音会|交响乐|河北梆子|丝弦|保定老调|唐剧|武安落子|高腔|深泽坠子|威县乱弹|曲剧|越调|二夹弦|宛梆|枣梆|山东梆子|吕剧|滇剧|眉户戏|沪剧|太康道情|河南坠剧|芗剧|花鼓戏|莆仙戏|梨园戏|上党梆子|蒲剧|晋剧|北路梆子|粤剧|二人转|汉剧|川剧|秦腔|昆剧|昆曲|京剧|曲艺|相声|脱口秀|马戏|杂技|魔术|话剧|歌剧|儿童剧|歌舞剧|音乐剧|舞剧|舞台剧|舞蹈|芭蕾|展览|展会|巡演|巡展|巡回展|博览会', '((?=.*(行政|政治))(?=.*(学术|研究|科研|艺术))(?=.*(强制|禁止|禁区|干涉|干预|限制|制约)))', '((?=.*(倚重|霸权|垄断|不允许))(?=.*(学术|研究|科研|艺术)))', '((?=.*(学术|研究|科研|艺术))(?=.*(民主|平等|公平|尊重|争鸣|自由)))']
    culture = "|".join(culture_words)

    # 生态制度化
    env = ""

    # 党建科学化
    party_words = ['党内基层民主|健全基层组织|人民意愿|党心民意|政治根基|政治建设|开门纳谏|汲取众智', '((?=.*(政治|党|会议|领导|主席|总理|发言人|书记|省长|市长|区长|县长|局长|部长|首长|处长|科长|镇长|乡长|村长|支书))(?=.*(标准|能力|自觉|定力|历练|担当|自律|过硬|理想|信念|方向|意识|核心|创新|信念|信仰|理想|精神|品格|意志|立场|纪律|规矩|担当|作风|修养|品质|境界|意识|觉悟|决策|部署|讲话|决定|选举|表彰|指示|反腐)))', '党务公开', '((?=.*监督)(?=.*党)(?=.*(组织|干部)))', '参与党内事务', '((?=.*党组织)(?=.*提出)(?=.*(意见|建议)))', '((?=.*(制度|机制|体系|体制|政策|规范))(?=.*党建)(?=.*考评))']
    party = "|".join(party_words)

    # 资源提取能力
    resource_get_words = ['一般税|个税|中央地方共享税|中央税|临时商业税|临时税|交易税|交税|产品税|从价税|从量税|以税代利|价内税|价外税|使用税|偷税|免征额|免税|关税|农业税|出口税|利改税|劳役税|包税|协税|占用税|印花税|反倾销税|发票|吨税|商品流通税|土地税|地产税|地方税|城市房地产税|城市维护建设税|城建税|增值税|增值额|增税|契税|奖金税|完税|定率税|定额税|实物税|对事税|对人税|对物税|屠宰税|工商业税|工商税|应税|建筑税|征税|房产税|所得税|所得额级距|抗税|摊贩业税|文化娱乐税|普通税|未税|棉纱统销税|欠税|正税|消费税|漏税|牧业税|特产税|特别税|盐税|直接税|税制|税务|税收|税政|税率|税目|税种|税负|税额|筵席税|纳税|经常税|统一税|营业税|行为税|补税|补贴税|计税|课税|调节税|负税|财税|货币税|货物税|资源税|起征点|车船使用牌照税|车船使用税|车船税|过境税|进口税|进税|退税|速算扣除数|配赋税|间接税|附加税|降税']
    resource_get = "|".join(resource_get_words)

    # 法律实施能力 - 暂同社会法治化
    law_put = ""

    # 监管能力
    supervise_put_words = ['监督|监管|抽查|抽检|超标', '((?=.*(不|未))(?=.*(合格|达标|标准|要求|规范)))']
    supervise_put = "|".join(supervise_put_words)

    # 科学决策能力
    scientific_decide_words = ['((?=.*科学发展)(?=.*(观|理念|理论)))', '((?=.*科学)(?=.*决策))', '((?=.*(专家|方案|智囊团|智库))(?=.*(评估|咨询|论证|研讨|调研|调查|分析|拟定|实验|修正|合作)))']
    scientific_decide = "|".join(scientific_decide_words)

    # 危机应对能力
    crisis_respond = ""

    # 政治革新能力
    politics_renew_words = ['((?=.*(政治|政策|理论|思想))(?=.*(革新|改革|创新|改造|调整|变化|改进|调节|巩固|完善)))']
    politics_renew = "|".join(politics_renew_words)

    # 制度构建能力
    system_build_words = ['制度|机制|体系|体制|政策|规范']
    system_build = "|".join(system_build_words)

    # 接纳参与能力
    accept_put_words = ['听证|司法救济|舆论监督|民意调查|利益|诉求|主张|建议|意见|投诉|反馈|举报|信箱|热线|信访|上访|游行|示威']
    accept_put = "|".join(accept_put_words)

    # 政治整合能力
    politics_integrate_words = ['富强|民主|文明|和谐|自由|平等|公正|法治|爱国|敬业|诚信|友善']
    politics_integrate = "|".join(politics_integrate_words)

    # 精英录用能力 - 过

    # 战略规划能力
    strategy_plan_words = ['((?=.*(战略|顶层|规划))(?=.*(思维|设计|实施)))', '((?=.*(调整|优化|改革|强化|完善|提升|构建|培育|落实|深化))(?=.*(制度|政策|规划|设计|结构|供给|产业链|体系|布局|格局)))', '((?=.*有效供给|脱贫|扶贫|乡村)(?=.*振兴|三农|二个百年))', '((?=.*(示范|实验|试点))(?=.*(省|市|区|县|乡|镇|村|点|项目|工程)))', '产业融合|融合发展|互联网+|新业态|新产业', '((?=.*坚持)(?=.*不动摇))', '((?=.*淘汰)(?=.*落后))', '((?=.*以)(?=.*依托))', '((?=.*(坚持|发挥|协调|提供|转变|打破|贯彻|牢固|梳理|运用|统筹|提升))(?=.*(管|揽|作用|能力|水平|保障|观念|理念|思想|思维|发展|治理|能力)))']
    strategy_plan = "|".join(strategy_plan_words)

    # 公平保障能力 - 过

    # 政治沟通能力
    politics_communicate = ""

    kwds_dict = {'politics': politics, 'law': law, 'culture': culture, 'env': env, 'party': party, 'resource_get': resource_get, 'law_put': law_put, 'supervise_put': supervise_put, 'scientific_decide': scientific_decide, 'crisis_respond': crisis_respond, 'politics_renew': politics_renew, 'system_build': system_build, 'accept_put': accept_put, 'politics_integrate': politics_integrate, 'strategy_plan': strategy_plan, 'politics_communicate': politics_communicate}


class BaiduCates:
    # 政治民主化
    politics = [{"cate": "教育", "subs": []},
                {"cate": "文化", "subs": []},
                {"cate": "旅游", "subs": []},
                {"cate": "时事", "subs": []},
                {"cate": "社会", "subs": []},
                {"cate": "综合", "subs": []},
                {"cate": "财经", "subs": []}]

    # 社会法治化
    law = [{"cate": "", "subs": ["法制", "刑法"]}]

    # 经济市场化 - 用别的指数

    # 文化多样化
    culture = [{"cate": "娱乐", "subs": []},
               {"cate": "教育", "subs": []},
               {"cate": "文化", "subs": []},
               {"cate": "", "subs": ["文玩"]}]

    # 生态制度化
    env = [{"cate": "", "subs": ["环境污染", "新能源", "环境保护", "能源", "石油"]}]

    # 党建科学化
    party = [{"cate": "", "subs": ["时政"]}]

    # 资源提取能力
    resource_get = [{"cate": "财经", "subs": []}]

    # 法律实施能力
    law_put = [{"cate": "", "subs": ["法制", "刑法"]}]

    # 监管能力
    supervise_put = [{"cate": "体育", "subs": []},
                     {"cate": "健康养生", "subs": []},
                     {"cate": "娱乐", "subs": []},
                     {"cate": "家居", "subs": []},
                     {"cate": "教育", "subs": []},
                     {"cate": "文化", "subs": []},
                     {"cate": "旅游", "subs": []},
                     {"cate": "时事", "subs": []},
                     {"cate": "母婴育儿", "subs": []},
                     {"cate": "汽车", "subs": []},
                     {"cate": "游戏", "subs": []},
                     {"cate": "社会", "subs": []},
                     {"cate": "科技", "subs": []},
                     {"cate": "综合", "subs": []},
                     {"cate": "财经", "subs": []},
                     {"cate": "音乐", "subs": []}]

    # 科学决策能力
    scientific_decide = [{"cate": "", "subs": ["时政"]}]

    # 危机应对能力
    crisis_respond = [{"cate": "教育", "subs": []},
                      {"cate": "文化", "subs": []},
                      {"cate": "旅游", "subs": []},
                      {"cate": "时事", "subs": []},
                      {"cate": "社会", "subs": []},
                      {"cate": "综合", "subs": []},
                      {"cate": "财经", "subs": []}]

    # 政治革新能力
    politics_renew = [{"cate": "", "subs": ["时政"]}]

    # 制度构建能力
    system_build = [{"cate": "", "subs": ["时政"]}]

    # 接纳参与能力
    accept_put = [{"cate": "教育", "subs": []},
                  {"cate": "文化", "subs": []},
                  {"cate": "旅游", "subs": []},
                  {"cate": "时事", "subs": []},
                  {"cate": "社会", "subs": []},
                  {"cate": "科技", "subs": []},
                  {"cate": "综合", "subs": []},
                  {"cate": "财经", "subs": []}]

    # 政治整合能力
    politics_integrate = [{"cate": "教育", "subs": []},
                          {"cate": "文化", "subs": []},
                          {"cate": "时事", "subs": []},
                          {"cate": "社会", "subs": []},
                          {"cate": "综合", "subs": []},
                          {"cate": "财经", "subs": []}]

    # 战略规划能力
    strategy_plan = [{"cate": "", "subs": ["时政", "三农", "农村改革"]}]

    # 公平保障能力 - 过

    # 政治沟通能力
    politics_communicate = [{"cate": "", "subs": ["时政"]}]

    cates_dict = {'politics': politics, 'law': law, 'culture': culture, 'env': env, 'party': party, 'resource_get': resource_get, 'law_put': law_put, 'supervise_put': supervise_put, 'scientific_decide': scientific_decide, 'crisis_respond': crisis_respond, 'politics_renew': politics_renew, 'system_build': system_build, 'accept_put': accept_put, 'politics_integrate': politics_integrate, 'strategy_plan': strategy_plan, 'politics_communicate': politics_communicate}


if __name__ == "__main__":
    # test_relation = "法律and平等"

    test_relations = ["(战略or顶层or规划)and(思维or设计or实施)", "(调整or优化or改革or强化or完善or提升or构建or培育or落实or深化)and(制度or政策or规划or设计or结构or供给or产业链or体系or布局or格局)", '有效供给or脱贫or扶贫or乡村and振兴or三农or二个百年', "(示范or实验or试点)and(省or市or区or县or乡or镇or村or点or项目or工程)", '产业融合or融合发展or互联网+or新业态or新产业', "坚持and不动摇", "淘汰and落后", "以and依托", "(坚持or发挥or协调or提供or转变or打破or贯彻or牢固or梳理or运用or统筹or提升)and(管or揽or作用or能力or水平or保障or观念or理念or思想or思维or发展or治理or能力)"]

    test_regexs = []

    if 0:
        for test_relation_ in test_relations:
            if "and" in test_relation_:
                test_regex = transform_and_relation_to_regex(test_relation_)
            else:
                test_regex = transform_or_relation_to_regex(test_relation_)

            test_regexs.append("'"+test_regex+"'")

        print(", ".join(test_regexs))

    test_str = "“以前对孩子的教育非常迷茫，不敢管不愿管也管不了，通过这次家风宣讲才知道，原来是自己没掌握好方式方法，以后一定要重视家风建设，学好家教知识，通过言传身教潜移默化引导孩子养成良好习惯，传承好家风，宣扬好家训。”樟树市店下镇官堂村贫困户郑喜兵认真的说。6月23日上午9时30分，由宜春市妇联组织的“121”家风宣讲团来到官堂村，在该村委三楼会议室，一场“好家风•好家训”的讲座正如火如荼的开展。不大的会议室里前来听讲的群众坐的满满当当，其中贫困户29人，师生11人，其他群众47人。在一个多小时的讲座中，宣讲老师围绕“亲子关系”的主题，以一些家喻户晓的名人故事、典型的社会事件、身边的实例以及当前的舆论热点入手讲述家风家训的古今意义。生动活泼的语言和操作性强的游戏引发在场听众的共鸣，现场气氛活跃，把抽象的道理具体化、形象化，给大家带来一场“家风家训”大餐。据悉，此次宣讲活动将走进该镇6个行政村，预计惠及贫困户176人，师生70余人，群众达到400余人。"

    import re
    xxx = re.search(KeyWords.kwds_dict["culture"], test_str)
    if xxx:
        print("yyyyy")
