from pyspark.sql import SparkSession

import sys

"""
# 基于块 轮 的数据 聚合相关指标到dws层   局粒度
CREATE EXTERNAL TABLE hungry_studio.dws_block_blast_ios_block_action_game_di(
  device_id string COMMENT '设备唯一标识', 
  app_id string COMMENT 'app标识', 
  install_timestamp bigint COMMENT 'app安装时间戳', 
  install_datetime timestamp COMMENT 'app安装时间,UTC日期格式', 
  os string COMMENT '系统', 
  screen_width int COMMENT '屏幕宽度', 
  screen_height int COMMENT '屏幕高度', 
  device_model string COMMENT '机型', 
  device_type string COMMENT '设备类型', 
  bundle_id string COMMENT '包名', 
  ip string COMMENT 'ip', 
  country string COMMENT '国家中文全称', 
  province string COMMENT '省份英文', 
  city string COMMENT '城市英文', 
  manufacturer string COMMENT '设备制造商', 
  simulator int COMMENT '是否模拟器', 
  server_timestamp bigint COMMENT '服务器时间戳', 
  server_datetime timestamp COMMENT '服务器时间,UTC日期格式', 
  model_type string COMMENT '事件类型', 
  uuid string COMMENT '事件唯一标识', 
  distinct_id string COMMENT '本次安装唯一标识', 
  account_id string COMMENT '用户id,同一用户可在多个设备上登录', 
  event_name string COMMENT '事件名称', 
  event_timestamp bigint COMMENT '事件时间戳', 
  event_datetime timestamp COMMENT '事件时间,UTC日期', 
  start_timestamp double, 
  start_time string, 
  zone_offset int COMMENT '时区', 
  network_type string COMMENT '网络类型', 
  carrier string COMMENT '运行商', 
  app_version string COMMENT 'app版本', 
  os_version string COMMENT '系统版本', 
  lib_version string COMMENT 'SDK 版本号', 
  system_language string COMMENT '系统语言', 
  cpu string COMMENT 'cpu型号', 
  ram string COMMENT '剩余内存/内存', 
  disk string COMMENT '剩余磁盘大小/磁盘大小', 
  fps double COMMENT '游戏帧率', 
  ab_test_id string COMMENT '线上ab测试的分组id', 
  properties string COMMENT '其余业务参数', 
  duration double COMMENT '时长', 
  s_ad_type string COMMENT '', 
  revenue double COMMENT '收入', 
  ad_format string COMMENT '', 
  sdk_data_source string COMMENT '数据来源,new or old', 
  p_0 string, 
  p_1 string, 
  p_2 string, 
  p_3 string, 
  p_4 string, 
  p_5 string, 
  ab_status string, 
  combo_cnt string comment 'combo总次数', 
  dead_block_list string comment '死亡方块列表', 
  five_clean string comment '5消次数', 
  four_clean string comment '4消次数', 
  game_id string  comment '前局ID，玩家终身累积，从1开始', 
  game_time string comment '游戏时长。单位秒', 
  game_type string comment '游戏模式（0无尽 2关卡 6土耳其方块 10谜题 99新手引导）', 
  his_max_score string comment '历史最高分（仅无尽、土耳其有）', 
  item_collect_detail string comment '旅行模式物品收集关展示结束时物品收集详情。物品id：物品数量。', 
  matrix string comment '盘面数据', 
  max_combo string comment '最大combo数', 
  one_clean string comment '1消次数', 
  oneclean string comment '-', 
  process string comment '旅行模式关卡进度，范围为0-100', 
  rec_strategy string comment '理论推荐策略', 
  rec_strategy_fact string comment '实际推荐策略', 
  result_type string comment '结束类型（0失败1胜利）-仅旅行模式有', 
  revice_success_cnt string comment '玩家复活成功次数', 
  revive_show_cnt string comment '复活广告界面展示次数', 
  reward_ad_click_cnt string comment '复活广告点击观看次数（玩家点击复活广告按钮）', 
  round_id string comment '局内累计轮数', 
  score string comment '结束时分数，无尽及旅行分数关展示分数。', 
  six_clean string comment '6消次数', 
  three_clean string comment '3消次数', 
  time string comment '-', 
  travel_id string comment '旅行期数', 
  travel_lv string comment '该期关卡号', 
  two_clean string comment '2消次数',

block_step_cnt bigint comment '步数',
round_cnt bigint comment '轮数',
round_012_cnt bigint comment '轮数-012',
round_021_cnt bigint comment '轮数-021',
round_102_cnt bigint comment '轮数-102',
round_120_cnt bigint comment '轮数-120',
round_201_cnt bigint comment '轮数-201',
round_210_cnt bigint comment '轮数-210',

game_combo_cnt bigint comment 'combo段数',
game_combo_step_cnt bigint comment 'combo 总长度',
game_combo_block_step_cnt bigint comment 'combo内消除步数',
multiple_block_step_cnt bigint comment '多消步数',
game_combo_block_step_cnt_rate double comment 'combo消除比例 一局，在combo内消除的次数/combo总长度',
game_combo_step_cnt_rate double comment 'combo浓度 一局，combo总长度/摆放块次数',
avg_game_combo_step_cnt double comment 'combo平均段长度 一局，combo总长度/combo段数',
block_step_cnt_rate double comment '消除比例 一局，消除次数/摆放块次数',
multiple_block_step_cnt_rate double comment '多消比例'  


  )
COMMENT 'block blast ios 游戏-局粒度汇总数据'
PARTITIONED BY ( 
  dt string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://hungry-studio-data-warehouse/dws/block_blast/ios/_block_action_game_di'
TBLPROPERTIES (
  'classification'='parquet', 
  'transient_lastDdlTime'='1701767686')

ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (time_diff_in_seconds double COMMENT '落块-整体时间'); 
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (time_action_in_seconds double COMMENT '落块-动作时间');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (time_think_in_seconds double COMMENT '落块-思考时间');


-- mddified by zhaodawei on 20240812 for 
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (inter_ad_pv bigint COMMENT '插屏广告展示次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (rewarded_ad_pv bigint COMMENT '激励广告展示次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (banner_ad_pv bigint COMMENT 'banner广告展示次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (inter_ad_revenue double COMMENT '插屏广告展示收入');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (rewarded_ad_revenue double COMMENT '激励广告展示收入');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (banner_ad_revenue double COMMENT 'banner广告展示收入');

ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS ( roundinfo array<array<string>> COMMENT '轮信息流汇总')

-- 20240816 新增 游戏时间 权重
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (real_time bigint COMMENT '游戏时长');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (weight double COMMENT '权重');


-- 20240826  zhaodawei  新增复活等指标
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (blast_cnt bigint COMMENT '消除次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (clean_cnt bigint COMMENT '消除行列数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (clear_screen_cnt bigint COMMENT '清屏次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (revive_round_id_list array<String> COMMENT '复活轮id列表');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (all_puzzle_round_id_list array<String>  COMMENT '遇到死亡难题的轮id列表(三个策略均为死亡难题)');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (dead_reason int COMMENT '死亡原因,1:死亡难题,2:故意怼死, -1:其他');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (is_sdk_sample int comment '是否抽样');


ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (session_id int COMMENT '每次冷启，截止下一次冷启');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (combo5_cnt string COMMENT '到combo5的次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (combo10_cnt string COMMENT '到combo10的次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (combo15_cnt string COMMENT '到combo15的次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (matrix_before_dead string COMMENT '死亡前本轮的初始盘面');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (block_list_before_dead string COMMENT '死亡前本轮的出题');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (game_popup_revive_click_cnt string COMMENT '本局复活弹窗 点击次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (inter_ad_request_pv string COMMENT '本局 插屏请求次数');
ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (rewarded_ad_request_pv string COMMENT '本局 激励请求次数');


ALTER TABLE hungry_studio.dws_block_blast_ios_block_action_game_di ADD COLUMNS (init_matrix string COMMENT '本局初始牌面');


"""

if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("dws_block_blast_ios_block_action_round_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]

    # 读取原始数据
    sdk_user_info_source = "hungry_studio.dim_block_blast_ios_user_ha"
    user_label_source = "hungry_studio.dim_block_blast_ios_user_label_ha"
    game_source = "hungry_studio.dwd_block_blast_ios_block_action_game_di"
    round_source = "hungry_studio.dws_block_blast_ios_block_action_round_di"
    white_event_source = "hs_lakehouse.dwd_block_blast_ios_white_event_unique_data_hi"
    target_table = "hungry_studio.dws_block_blast_ios_block_action_game_di"

    spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict;''')

    sdk_user_info_query = f"""
    SELECT 
        distinct_id AS user_distinct_id,
        is_sdk_sample
    FROM {sdk_user_info_source}
    WHERE dt = '{DT}' AND hour = '23'
    """
    df = spark.sql(sdk_user_info_query)
    df.createOrReplaceTempView("sdk_user_info_source")

    user_label_query = f"""
        select 
            distinct_id as user_way_distinct_id,
            game_way_type as game_way_type,
            user_waynum as user_waynum,
            active_ab_game_way_type as active_ab_game_way_type,
            nowwayarr as nowwayarr,
            case when nowwayarr is not null then nowwayarr[size(nowwayarr) -1] end as latest_now_way
            from {user_label_source} where dt = '{DT}' and hour = '23'
        """
    df = spark.sql(user_label_query)
    df.createOrReplaceTempView("user_label_source")

    game_query = f"""
        select device_id,
            app_id,
            install_timestamp,
            install_datetime,
            os,
            screen_width,
            screen_height,
            device_model,
            device_type,
            bundle_id,
            ip,
            country,
            province,
            city,
            manufacturer,
            simulator,
            server_timestamp,
            server_datetime,
            model_type,
            uuid,
            distinct_id,
            account_id,
            event_name,
            event_timestamp,
            event_datetime,
            start_timestamp,
            start_time,
            zone_offset,
            network_type,
            carrier,
            app_version,
            os_version,
            lib_version,
            system_language,
            cpu,
            ram,
            disk,
            fps,
            ab_test_id,
            properties,
            duration,
            s_ad_type,
            revenue,
            ad_format,
            sdk_data_source,
            p_0,
            p_1,
            p_2,
            p_3,
            p_4,
            p_5,
            ab_status,
            combo_cnt,
            dead_block_list,
            five_clean,
            four_clean,
            game_id,
            game_time,
            game_type,
            his_max_score,
            item_collect_detail,
            matrix,
            max_combo,
            one_clean,
            oneclean,
            process,
            rec_strategy,
            rec_strategy_fact,
            result_type,
            revice_success_cnt,
            revive_show_cnt,
            reward_ad_click_cnt,
            round_id,
            score,
            six_clean,
            three_clean,
            time,
            travel_id,
            travel_lv,
            two_clean,
            get_json_object(properties , '$.is_solvable')  as is_solvable,
            session_id,
            dt
        from {game_source} t where dt='{DT}'
        """
    df = spark.sql(game_query)
    df.createOrReplaceTempView("game_source")

    round_query = f"""
        select 
                distinct_id,
                game_id,
                game_type,
                sum(block_step_cnt) block_step_cnt, -- bigint comment '步数',
                count(round_id) round_cnt, -- bigint comment '轮数',
                count(case when index_pos_list='012' then round_id end ) round_012_cnt, -- bigint comment '轮数-012',
                count(case when index_pos_list='021' then round_id end ) round_021_cnt, -- bigint comment '轮数-021',
                count(case when index_pos_list='102' then round_id end ) round_102_cnt, -- bigint comment '轮数-102',
                count(case when index_pos_list='120' then round_id end ) round_120_cnt, -- bigint comment '轮数-120',
                count(case when index_pos_list='201' then round_id end ) round_201_cnt, -- bigint comment '轮数-201',
                count(case when index_pos_list='210' then round_id end ) round_210_cnt, -- bigint comment '轮数-210',
                --修复字段
                sum(game_combo_cnt) game_combo_cnt, --  bigint comment 'combo段数',
                
                --新增字段
                sum(combo5_cnt) combo5_cnt, --  bigint comment 'combo5段数',--到combo5的次数
                sum(combo10_cnt) combo10_cnt, --  bigint comment 'combo10段数',--到combo10的次数
                sum(combo15_cnt) combo15_cnt, --  bigint comment 'combo15段数',--到combo15的次数
                max(case when is_final_round=true then matrix end) as matrix_before_dead,--死亡前本轮的初始盘面
                max(case when is_final_round=true then block_list end) as block_list_before_dead,--死亡前本轮的出题
                    
                sum(game_combo_step_cnt) game_combo_step_cnt, --  bigint comment 'combo 总长度',
                sum(game_combo_block_step_cnt) game_combo_block_step_cnt, --  bigint comment 'combo内消除步数',
                sum(multiple_block_step_cnt) multiple_block_step_cnt, --  bigint comment '多消步数',

                round(sum(game_combo_block_step_cnt)/sum(game_combo_step_cnt),4) game_combo_block_step_cnt_rate, -- double comment 'combo消除比例 一局，在combo内消除的次数/combo总长度',
                round(sum(game_combo_step_cnt)/sum(block_step_cnt),4) game_combo_step_cnt_rate, -- double comment 'combo浓度 一局，combo总长度/摆放块次数',
                round(sum(game_combo_step_cnt)/sum(game_combo_cnt),4) avg_game_combo_step_cnt, -- double comment 'combo平均段长度 一局，combo总长度/combo段数',
                round(sum(blast_cnt)/sum(block_step_cnt),4) block_step_cnt_rate, -- double comment '消除比例 一局，消除次数/摆放块次数',
                round(sum(multiple_block_step_cnt)/sum(block_step_cnt),4) multiple_block_step_cnt_rate, -- double comment '多消比例' ,
                sum(time_diff_in_seconds) time_diff_in_seconds, -- 单轮耗时
                sum(time_action_in_seconds) time_action_in_seconds, -- 单轮耗时-动作
                sum(time_think_in_seconds) time_think_in_seconds, -- 单轮耗时-思考
                sort_array(collect_list(array(
                                              COALESCE(min_event_timestamp, '-1'), 
                                              COALESCE(max_event_timestamp, '-1'), 
                                              COALESCE(game_type, '-1'), 
                                              COALESCE(round_id, '-1'), 
                                              COALESCE(travel_id, '-1'), 
                                              COALESCE(travel_lv, '-1'), 
                                              CAST(COALESCE(clear_screen_cnt, -1) AS String), 
                                              CAST(COALESCE(blast_cnt, -1) AS String), 
                                              CAST(COALESCE(blast_row_col_cnt, -1) AS String), 
                                              CAST(COALESCE(is_combo_status_cnt, -1) AS String), 
                                              CAST(COALESCE(common_block_cnt, -1) AS String), 
                                              CAST(COALESCE(round_score, -1) AS String), 
                                              CAST(round(COALESCE(time_diff_in_seconds, -1),3) AS String), 
                                              COALESCE(matrix_complex_value, '-1'), 
                                              COALESCE(rec_strategy, '-1'), 
                                              COALESCE(rec_strategy_fact, '-1'), 
                                              COALESCE(index_pos_list, '-1'), 
                                              CAST(COALESCE(block_step_cnt, -1) AS String), 
                                              CAST(COALESCE(game_combo_cnt, -1) AS String), 
                                              CAST(COALESCE(game_combo_step_cnt, -1) AS String), 
                                              CAST(COALESCE(game_combo_block_step_cnt, -1) AS String), 
                                              CAST(COALESCE(multiple_block_step_cnt, -1) AS String), 
                                              CAST(round(COALESCE(game_combo_block_step_cnt_rate, -1),4) AS String), 
                                              CAST(round(COALESCE(game_combo_step_cnt_rate, -1),4) AS String), 
                                              CAST(round(COALESCE(block_step_cnt_rate, -1),4) AS String), 
                                              COALESCE(block_list, '-1'), 
                                              CAST(round(COALESCE(time_action_in_seconds, -1),4) AS String), 
                                              CAST(round(COALESCE(time_think_in_seconds, -1),4) AS String), 
                                              CAST(COALESCE(gain_score, -1) AS String), 
                                              CAST(COALESCE(gain_score_per_done, -1) AS String)
                                            ))) roundinfo
                ,sum(blast_cnt)         as blast_cnt
                ,sum(clean_cnt)         as clean_cnt
                ,sum(clear_screen_cnt)  as clear_screen_cnt
                --迁移过来的字段
                ,sort_array(collect_set(case when rec_strategy_fact = '["死亡难题","死亡难题","死亡难题"]' then round_id end))  as all_puzzle_round_id_list
                ,max(if(is_final_round=true,rec_strategy_fact, null))  as dead_rec_strategy_fact
        from {round_source} t where dt='{DT}'
        group by 
        distinct_id,
        game_id,
        game_type
        """
    df = spark.sql(round_query)
    df.createOrReplaceTempView("round_source")

    white_event_query = f"""
         select
                    distinct_id
                   ,coalesce(get_json_object(properties, '$.game_type'), get_json_object(properties, '$.GameType'))  as game_type
                   ,coalesce(get_json_object(properties, '$.game_id'), get_json_object(properties, '$.gameId'))   as game_id
                   ,sum(if(event_name in ('admob_sdk_ad_revenue', 'ironSource_sdk_postbacks', 'appLovin_sdk_ad_revenue', 'hs_sdk_ad_revenue') 
                   and LOWER(get_json_object(properties, '$.adFormat')) = 'inter', 1, 0))                                                            as inter_ad_pv
                   ,sum(if(event_name in ('admob_sdk_ad_revenue', 'ironSource_sdk_postbacks', 'appLovin_sdk_ad_revenue', 'hs_sdk_ad_revenue') 
                   and LOWER(get_json_object(properties, '$.adFormat')) = 'rewarded', 1, 0))                                                         as rewarded_ad_pv
                   ,sum(if(event_name in ('hs_ad_banner_revenue'), 1, 0))                                                                            as banner_ad_pv
                   ,sum(if(event_name in ('admob_sdk_ad_revenue', 'ironSource_sdk_postbacks', 'appLovin_sdk_ad_revenue', 'hs_sdk_ad_revenue') 
                   and LOWER(get_json_object(properties, '$.adFormat')) = 'inter', cast(get_json_object(properties, '$.revenue') as double), 0))     as inter_ad_revenue
                   ,sum(if(event_name in ('admob_sdk_ad_revenue', 'ironSource_sdk_postbacks', 'appLovin_sdk_ad_revenue', 'hs_sdk_ad_revenue') 
                   and LOWER(get_json_object(properties, '$.adFormat')) = 'rewarded', cast(get_json_object(properties, '$.revenue') as double), 0))  as rewarded_ad_revenue
                   ,sum(if(event_name in ('hs_ad_banner_revenue'), cast(get_json_object(properties, '$.revenue') as double), 0))                     as banner_ad_revenue
                   ,max(case when event_name='usr_data_game_end' then cast(get_json_object(properties, '$.RealTime') as bigint) end)   as real_time
                   ,max(case when event_name='usr_data_game_end' then cast(get_json_object(properties, '$.weight') as double) end) as weight
                   ,sort_array(collect_set(case when event_name = 'game_revive' then coalesce(get_json_object(properties,'$.round_id'), get_json_object(properties,'$.roundId')) end )) as revive_round_id_list
                   ,sum(if(event_name in ('game_popop_revive_click'), 1, 0))   as game_popup_revive_click_cnt --本局复活弹窗 点击次数 
                   ,sum(if(event_name in ('usr_data_ad_show') and cast(get_json_object(properties, '$.adType') as double) >= 400 and cast(get_json_object(properties, '$.adType') as double) < 500, 1, 0)) as inter_ad_request_pv -- 本局 插屏请求次数
                   ,sum(if(event_name in ('usr_data_ad_show') and cast(get_json_object(properties, '$.adType') as double) >= 500, 1, 0)) as rewarded_ad_request_pv -- 本局 激励请求次数
                   ,max(case when event_name='usr_data_game_start' then get_json_object(properties, '$.matrix') end) as init_matrix
            from {white_event_source} t
            where dt='{DT}'
            and event_name in ('admob_sdk_ad_revenue', 
                               'ironSource_sdk_postbacks', 
                               'appLovin_sdk_ad_revenue', 
                               'hs_sdk_ad_revenue', 
                               'hs_ad_banner_revenue',
                               'usr_data_game_end',
                               'game_revive',
                               'game_popop_revive_click',
                               'usr_data_ad_show')
            group by 1,2,3
        """
    df = spark.sql(white_event_query)
    df.createOrReplaceTempView("white_event_source")




    exec_sql = f'''
 insert overwrite table {target_table}
 select a.device_id,    
            a.app_id,
            a.install_timestamp,
            a.install_datetime,
            a.os,
            a.screen_width,
            a.screen_height,
            a.device_model,
            a.device_type,
            a.bundle_id,
            a.ip,
            a.country,
            a.province,
            a.city,
            a.manufacturer,
            a.simulator,
            a.server_timestamp,
            a.server_datetime,
            a.model_type,
            a.uuid,
            a.distinct_id,
            a.account_id,
            a.event_name,
            a.event_timestamp,
            a.event_datetime,
            a.start_timestamp,
            a.start_time,
            a.zone_offset,
            a.network_type,
            a.carrier,
            a.app_version,
            a.os_version,
            a.lib_version,
            a.system_language,
            a.cpu,
            a.ram,
            a.disk,
            a.fps,
            a.ab_test_id,
            a.properties,
            a.duration,
            a.s_ad_type,
            a.revenue,
            a.ad_format,
            a.sdk_data_source,
            a.p_0,
            a.p_1,
            a.p_2,
            a.p_3,
            a.p_4,
            a.p_5,
            a.ab_status,
            a.combo_cnt,
            a.dead_block_list,
            a.five_clean,
            a.four_clean,
            a.game_id,
            a.game_time,
            a.game_type,
            a.his_max_score,
            a.item_collect_detail,
            a.matrix,
            a.max_combo,
            a.one_clean,
            a.oneclean,
            a.process,
            a.rec_strategy,
            a.rec_strategy_fact,
            a.result_type,
            a.revice_success_cnt,
            a.revive_show_cnt,
            a.reward_ad_click_cnt,
            a.round_id,
            a.score,
            a.six_clean,
            a.three_clean,
            a.time,
            a.travel_id,
            a.travel_lv,
            a.two_clean,

            b.block_step_cnt,
            b.round_cnt,
            b.round_012_cnt,
            b.round_021_cnt,
            b.round_102_cnt,
            b.round_120_cnt,
            b.round_201_cnt,
            b.round_210_cnt,

            b.game_combo_cnt,
            b.game_combo_step_cnt,
            b.game_combo_block_step_cnt,
            b.multiple_block_step_cnt,
            b.game_combo_block_step_cnt_rate,
            b.game_combo_step_cnt_rate,
            b.avg_game_combo_step_cnt,
            b.block_step_cnt_rate,
            b.multiple_block_step_cnt_rate,
            b.time_diff_in_seconds,
            b.time_action_in_seconds,
            b.time_think_in_seconds,
            c.inter_ad_pv,
            c.rewarded_ad_pv,
            c.banner_ad_pv,
            c.inter_ad_revenue,
            c.rewarded_ad_revenue,
            c.banner_ad_revenue,
            b.roundinfo,
            c.real_time,
            c.weight
            ,b.blast_cnt
            ,b.clean_cnt
            ,b.clear_screen_cnt
            ,coalesce(c.revive_round_id_list, array())          as revive_round_id_list
            ,coalesce(b.all_puzzle_round_id_list, array())      as all_puzzle_round_id_list
            ,case when b.dead_rec_strategy_fact = '["死亡难题","死亡难题","死亡难题"]'  then 1
                  when a.is_solvable = 1                                            then 2   
                  else  -1        end as dead_reason   
            ,cast(e.is_sdk_sample as Integer) as is_sdk_sample
            ,cast(f.game_way_type as int) as game_way_type
            ,f.user_waynum
            ,cast(f.active_ab_game_way_type as int) as active_ab_game_way_type
            ,f.nowwayarr
            ,f.latest_now_way     
            ,a.session_id
            ,cast(b.combo5_cnt as int) as combo5_cnt
            ,cast(b.combo10_cnt as int) as combo10_cnt
            ,cast(b.combo15_cnt as int) as combo15_cnt
            ,b.matrix_before_dead
            ,b.block_list_before_dead
            ,c.game_popup_revive_click_cnt
            ,c.inter_ad_request_pv
            ,c.rewarded_ad_request_pv 
            ,c.init_matrix
            ,a.dt
        from 
         game_source a
        left join 
        round_source b 
        on a.distinct_id=b.distinct_id and a.game_id=b.game_id and a.game_type=b.game_type
        left join 
        white_event_source c
        on a.distinct_id=c.distinct_id and a.game_id=c.game_id and a.game_type=c.game_type
        left join sdk_user_info_source e
        on a.distinct_id = e.user_distinct_id 
        left join user_label_source f
        on a.distinct_id = f.user_way_distinct_id
     '''.format(DT=DT)
    print(exec_sql)
    spark.sql(exec_sql)

    # 关闭 Spark 会话
    spark.stop()
