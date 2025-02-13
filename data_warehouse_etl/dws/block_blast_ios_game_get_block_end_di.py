from pyspark.sql import SparkSession

import sys

"""
sunlongjiang
CREATE EXTERNAL TABLE hungry_studio.dws_block_blast_gp_block_revive_round_di(
  `distinct_id` string COMMENT '用户id', 
  `game_type` string COMMENT '游戏模式（0无尽 2关卡 6土耳其方块 10谜题 99新手引导）', 
  `round_id` string COMMENT '局内累计轮数',
  `game_id` string COMMENT '游戏-局id' , 
  `revive_cnt` bigint comment '该轮的复活次数'
)
COMMENT 'block blast ios 游戏-局-轮粒度复活明细表'
  PARTITIONED by(dt string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://hungry-studio-data-warehouse/dws/block_blast/gp/block_revive_round_di'
  TBLPROPERTIES (
  'classification'='parquet')


"""


import sys

from pyspark.sql import SparkSession


if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("block_blast_ios_game_get_block_end_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict;''')
    exec_sql = '''
insert overwrite table hungry_studio.dws_block_blast_ios_game_get_block_end_di
SELECT distinct_id
,get_json_object(properties,'$.game_type') AS game_type
,get_json_object(properties,'$.round_id') AS round_id
,get_json_object(properties,'$.game_id') AS game_id
,get_json_object(properties,'$.cost_time') AS cost_time
,get_json_object(properties,'$.downReason') AS down_reason 
,dt 
FROM hungry_studio.dwd_block_blast_ios_event_data_hi 
WHERE dt = '{DT}'
AND event_name = 'game_get_block_end'
     '''.format(DT=DT)
    print(exec_sql)
    spark.sql(exec_sql)
    # 关闭 Spark 会话
    spark.stop()
