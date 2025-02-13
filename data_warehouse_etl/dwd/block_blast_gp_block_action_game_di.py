from pyspark.sql import SparkSession

import sys
"""

CREATE EXTERNAL TABLE hungry_studio.dwd_block_blast_gp_block_action_game_di(
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
  two_clean string comment '2消次数')
COMMENT 'block blast ios 游戏-局粒度明细数据'
PARTITIONED BY ( 
  dt string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://hungry-studio-data-warehouse/dwd/block_blast/gp/block_action_game_di'
TBLPROPERTIES (
  'classification'='parquet', 
  'transient_lastDdlTime'='1701767686')
"""

if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("dws_block_blast_gp_block_action_round_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]

    spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict;''')
    exec_sql = '''
insert overwrite table hungry_studio.dwd_block_blast_gp_block_action_game_di
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
            session_id,
            dt
        from (
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
                    country_cn country,
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
                    cast(event_timestamp - game_time * 1000 as bigint) start_timestamp,
                    from_unixtime(
                        (event_timestamp - game_time * 1000)/1000 ,
                        'yyyy-MM-dd HH:mm:ss.SSS'
                    ) as start_time,
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
                    -1 as duration,
                    -1 as s_ad_type,
                    -1 as revenue,
                    '' ad_format,
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
                    1 as rn,
                    session_id,
                    dt
                from hungry_studio.dwd_block_blast_gp_white_event_unique_data_hi 
                lateral view json_tuple(
                        properties,
                        '0',
                        '1',
                        '2',
                        '3',
                        '4',
                        '5',
                        'ABStatus',
                        'combo_cnt',
                        'dead_block_list',
                        'five_clean',
                        'four_clean',
                        'game_id',
                        'game_time',
                        'game_type',
                        'his_max_score',
                        'item_collect_detail',
                        'matrix',
                        'max_combo',
                        'one_clean',
                        'oneclean',
                        'process',
                        'rec_strategy',
                        'rec_strategy_fact',
                        'result_type',
                        'revice_success_cnt',
                        'revive_show_cnt',
                        'reward_ad_click_cnt',
                        'round_id',
                        'score',
                        'six_clean',
                        'three_clean',
                        'time',
                        'travel_id',
                        'travel_lv',
                        'two_clean',
                        'session_id'
                    ) x as p_0,
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
                    session_id
                where dt = '{DT}'
                    and event_name = 'game_end'
            ) a 
        where rn = 1;
     '''.format(DT=DT)
    print(exec_sql)
    spark.sql(exec_sql)

    # 关闭 Spark 会话
    spark.stop()
