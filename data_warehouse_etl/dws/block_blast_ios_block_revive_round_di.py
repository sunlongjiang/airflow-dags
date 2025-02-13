from pyspark.sql import SparkSession

import sys

"""
sunlongjiang
CREATE EXTERNAL TABLE hungry_studio.dws_block_blast_ios_block_revive_round_di(
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
  's3://hungry-studio-data-warehouse/dws/block_blast/ios/block_revive_round_di'
  TBLPROPERTIES (
  'classification'='parquet')


"""


import sys

from pyspark.sql import SparkSession


if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("dws_block_blast_ios_revive_round_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict;''')
    exec_sql = '''
insert overwrite table hungry_studio.dws_block_blast_ios_block_revive_round_di
select distinct_id
       ,game_type
       ,round_id
       ,game_id
       ,cast(revive_cnt as int) as revive_cnt
       ,cast(revive_show_cnt as int) as revive_show_cnt
       ,cast(revive_click_cnt as int) as revive_click_cnt
       ,game_revive_color_list
       ,game_end_color_list
       ,1 group_id -- cast((ABS(HASH(distinct_id)) % 10) + 1 as string) AS group_id
       ,dt  
        from (
  select  
     distinct_id as distinct_id
    ,game_type as game_type
    ,case when cast(game_type as integer) = 0 then cast(round_id as integer) -1 else cast(round_id as integer) end as round_id
    ,game_id as game_id
    ,dt
    ,sum(case when event_name in ('game_revive') then 1 else 0 end) as revive_cnt
    ,sum(case when event_name in ('game_revive_ui_show') then 1 else 0 end) as revive_show_cnt
    ,sum(case when event_name in ('game_popop_revive_click') then 1 else 0 end) as revive_click_cnt
    ,max(case when event_name in ('game_revive_ui_show') then color_list end) as game_revive_color_list
    ,max(case when event_name in ('usr_data_game_end') then color_list end) as game_end_color_list 
  from 
    (
      SELECT 
       dt
       ,distinct_id
       ,coalesce(game_type,get_json_object(properties, '$.game_type')) as game_type
       ,coalesce(game_id,get_json_object(properties, '$.game_id')) as game_id
       ,coalesce(round_id,get_json_object(properties, '$.round_id')) as round_id
       ,from_json(properties, 'struct<color_list: array<string>>').color_list as color_list
       ,event_name  
      FROM 
       hs_lakehouse.dwd_block_blast_ios_white_event_unique_data_hi 
      WHERE 
        dt = '{DT}' 
        AND event_name IN ('game_revive','game_revive_ui_show','game_popop_revive_click','usr_data_game_end')
    ) a group by 1,2,3,4,5 
    ) a 
     '''.format(DT=DT)
    print(exec_sql)
    spark.sql(exec_sql)
    # 关闭 Spark 会话
    spark.stop()
