# <center>StableMonitoring

### 基于事件库提炼的子产品：后台数据及前端文件 
#### 包括：
- 新矛盾 - 稳定板块（稳定监测+趋势）
- 国家治理现代化（象限图）
- 国家治理（固定树结构的可调权重树）
- 商业地产报告，环境和治安满意度的数据

1、工程结构说明

    StableMonitoring/client    新矛盾稳定版块的前端文件
    StableMonitoring/codes     代码
    StableMonitoring/data      新矛盾稳定版块生成的数据（计数、得分等）
    StableMonitoring/docs      产品/工程相关的文档
 
2、主体代码架构说明
    
    StableMonitoring/codes/
        log/                    日志
        product/                子产品代码
        related_files/          公用的配置文件
        utils/                  公用的接口
        
        -> 【以下两个py文件需每日运行】（我是用windows自带的任务计划程序配置的）
        
        adjust_xmd_basic_info.py   更新事件表xmd_event_basic_info的more_info字段，
                                    含百度接口返回的情绪概率、分类、摘要，以及发博主体（verified_type）
                                    
        fetch_trifle_data_into_db.py   更新事件追踪表xmd_trifles_under_gov_control
        
        （之后智奇的代码，每天根据xmd_trifles_under_gov_control更新xmd_event_basic_info）

3、 子产品说明

①  新矛盾 - 稳定版块： 

    StableMonitoring/codes/product/**xmd_monitor**/
        modules/
            stable_calculate_score.py                       算分
            readjust_stable_score_with_other_elements.py    结合GDP、人口等调整稳定评分
            stable_score_store.py                           分数存储（数据库、知识库）
            stable_client_page.py                           前端文件生成
            stable_client_score.py                          前端边角分数+描述
        
        stable_monitor_auto.py            总调度；每月5号，直接运行

② 国家治理现代化（象限图）：

    StableMonitoring/codes/product/**gov_modern**/
        basic_data/
            parameters.py                                   基础数据的参数配置
            mark_idx_type.py                                标上“国家治理现代化”的子指标，如政治民主化等
            get_stats_n_score.py                            计数+计分，各子指标的官宣+民怨等指数
        frontend_related/
            parameters.py                                   前端文件所需参数配置
            data_related_api.py                             提取/生成前端数据的接口
            scatter_areas_api.py                            前端象限图处理相关的接口
            get_frontend_data.py                            前端数据总接口
            (sychron_tree_data.py                           把247的数据同步至46和83，现不用运行）
        
        gov_modern_auto.py  “国家治理现代化”数据+前端总调度；每月5号，直接运行；如不需再更新前端，将入参update_client=False

③ 国家治理（固定结构的可调权重树）

    StableMonitoring/codes/product/**state_govern**/
        basic_data/                                         
            get_traffic_avg_speed.py                        国家治理中用到了“早晚高峰平均速度”，知识库指标无人维护，我重新计算后写入
            get_leaf_score.py                               叶子节点数据补齐、归一处理
            indexes_to_excel.py                             将树状结构各节点数据，存储到excel里
            
            === 其他 ===
            deal_tree_frame.py                              生成该树的tree_frame.csv
            get_leaf_func.py                                给智奇的处理叶子节点指标的接口
            test.py                                         临时测试文件，不用管 
        data/                                               储存生成的excel表
        files/                                              用到的配置文件
        frontend_related/
            map_color.py                                    前端所需地图渲染数据的接口
            upper_indexes.py                                前端所需上层节点的数据接口
        
        parameters.py                                       参数文件
        sg_utils.py                                         公用接口
        sg_main.py                                          总调度；（产品已改版为“数据打榜平台”，无需再运行）
        
④ 商业地产报告，环境和治安满意度数据
    
    StableMonitoring/codes/product/**cbd_env_stable**/
        env_stable_utils.py                                 数据接口，每次杜哥更新版本后，改version跑一遍即可

4、其他备注
	
	查错日志：StableMonitoring/codes/log/以模块命名的.log文件
	
	StableMonitoring/client/ 中的文件可定期清除
	StableMonitoring/data/ 中的文件不能删           
    

    

    

    
      