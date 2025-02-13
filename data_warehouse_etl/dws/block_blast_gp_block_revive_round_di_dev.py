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
    spark = SparkSession.builder.appName("dws_block_blast_gp_revive_round_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict;''')
    exec_sql = '''
insert overwrite table hungry_studio_dev.dws_block_blast_gp_block_revive_round_di
select distinct_id
       ,game_type
       ,round_id
       ,game_id
       ,revive_cnt
       ,cast((ABS(HASH(distinct_id)) % 10) + 1 as string) AS group_id
       ,dt 
        from (
  select  
     distinct_id as distinct_id
    ,game_type as game_type
    ,case when cast(game_type as integer) = 0 then cast(round_id as integer) -1 else cast(round_id as integer) end as round_id
    ,game_id as game_id
    ,dt
    ,cast(count(round_id) as bigint) as revive_cnt
   
  from 
    (
      SELECT 
       dt
       ,distinct_id
       ,coalesce(get_json_object(properties, '$.game_type'), get_json_object(properties, '$.GameType'))  as game_type
       ,coalesce(get_json_object(properties, '$.game_id'), get_json_object(properties, '$.gameId'))   as game_id
       ,coalesce(get_json_object(properties, '$.round_id'), get_json_object(properties, '$.roundId'))   as round_id
      FROM 
        hungry_studio.dwd_block_blast_gp_white_event_unique_data_hi
      WHERE 
        dt = '{DT}' 
        AND event_name IN ('game_revive')
    ) a group by 1,2,3,4,5 
    ) a 
     '''.format(DT=DT)
    print(exec_sql)
    spark.sql(exec_sql)
    # 关闭 Spark 会话
    spark.stop()
