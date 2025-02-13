from pyspark.sql import SparkSession
import sys
import math
from collections import Counter

import copy
import json
import traceback

import sys


import sys
from datetime import datetime

if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("dws_block_blast_gp_block_action_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    HOUR = sys.argv[2]
    F_DT = datetime.strptime(DT, "%Y-%m-%d").strftime("%Y%m%d")
    now = datetime.now()
    current_time_str = now.strftime("%H%M%S")
    print(current_time_str)
    tmp_tbl_name = f"""temp_dws_block_blast_gp_block_action_di_pre_0_{F_DT}_{HOUR}"""
    tbl_path = f"""temp_dws_block_blast_gp_block_action_di_pre_0_{F_DT}_{current_time_str}"""
    print(tmp_tbl_name)
    # 删除存在的临时表
    sql_delete_tmp_tbl = '''
    drop table if exists default.{tmp_tbl_name}
    '''.format(tmp_tbl_name=tmp_tbl_name)
    print(sql_delete_tmp_tbl)
    spark.sql(sql_delete_tmp_tbl).show()


    create_sql= f""" CREATE EXTERNAL TABLE  default.{tmp_tbl_name} (
              `device_id` string COMMENT '设备唯一标识', 
              `app_id` string COMMENT 'app标识', 
              `install_datetime` timestamp COMMENT 'app安装时间,UTC日期格式', 
              `os` string COMMENT '系统', 
              `ip` string COMMENT 'ip', 
              `country_cn` string COMMENT '国家中文全称', 
              `country` string COMMENT '国家简称', 
              `city_cn` string COMMENT '城市中文', 
              `city` string COMMENT '城市英文', 
              `uuid` string COMMENT '事件唯一标识', 
              `distinct_id` string COMMENT '本次安装唯一标识', 
              `event_name` string COMMENT '事件名称', 
              `event_timestamp` bigint COMMENT '事件时间戳', 
              `event_datetime` timestamp COMMENT '事件时间,UTC日期', 
              `app_version` string COMMENT 'app版本', 
              `ram` string COMMENT '剩余内存/内存', 
              `disk` string COMMENT '剩余磁盘大小/磁盘大小', 
              `network_type` string COMMENT '网络类型', 
              `dt` string, 
              `game_id` string, 
              `game_type` string, 
              `round_id` string, 
              `travel_id` string, 
              `travel_lv` string, 
              `matrix` string, 
              `position` string, 
              `clean` array<string>, 
              `block_id` string, 
              `index_id` string, 
              `rec_strategy` string, 
              `rec_strategy_fact` string, 
              `combo_cnt` string, 
              `gain_score` string, 
              `gain_item` string, 
              `block_list` string, 
              `last_click_time` string, 
              `click_rank_per_round` string, 
              `gain_score_per_done` string, 
              `is_clean_screen` string, 
              `weight` string, 
              `put_rate` string, 
              `userwaynum` string, 
              `block_down_color` string, 
              `session_id` string, 
		      `block_shape_list` string,
		      `block_shape` string, 
		      `design_postion_upleft` string, 
		      `fps` double,
		      `design_position` string,
              `group_id` int)
            PARTITIONED BY ( 
            `hour` int)  
            ROW FORMAT SERDE 
              'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
            WITH SERDEPROPERTIES ( 
              'path'='s3://hungry-studio-tmp/{tbl_path}/') 
            STORED AS INPUTFORMAT 
              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
            OUTPUTFORMAT 
              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION
              's3://hungry-studio-tmp/{tbl_path}' """
    print(create_sql)
    spark.sql(create_sql)
    print("""建表完成""")
    # 关闭 Spark 会话
    spark.stop()
