from pyspark.sql import SparkSession
import sys
import math
from collections import Counter
import copy
import json
import traceback

import sys

from datetime import datetime

if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("block_blast_gp_block_action_block_di_pre_0").enableHiveSupport().getOrCreate()

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
    print('''执行删除存在的临时表 default.{tmp_tbl_name}完成'''.format(tmp_tbl_name=tmp_tbl_name))

    spark.sql('''
    CREATE TABLE default.{tmp_tbl_name}
    USING PARQUET
     location 's3://hungry-studio-tmp/{tbl_path}/'
		select device_id
        ,app_id
        ,install_datetime
        ,os
        ,ip
        ,country_cn
        ,country 
        ,city_cn 
        ,city
        ,uuid
        ,distinct_id
        ,event_name
        ,event_timestamp
        ,event_datetime
        ,app_version
        ,ram
        ,disk
        ,network_type
        ,coalesce(game_id,usr_game_id) as game_id
		,coalesce(game_type,usr_game_type) as game_type
        ,round_id
        ,travel_id
        ,travel_lv
        ,matrix
        ,position
        ,clean
        ,block_id
        ,index_id
        ,rec_strategy
        ,rec_strategy_fact
        ,combo_cnt
        ,gain_score
        ,gain_item
        ,block_list
        ,coalesce(last_click_time,0) as last_click_time
        ,click_rank_per_round
        ,gain_score_per_done
        ,is_clean_screen
        ,weight
        ,put_rate
        ,userwaynum
        ,block_down_color
        ,session_id
		,block_shape_list
		,block_shape
		,design_postion_upleft
		,fps
		,design_position
        ,group_id
        ,dt
		 from 
		 (
		select 
		 device_id
		 ,get_json_object(properties,'$.gameId') as usr_game_id
         ,get_json_object(properties,'$.GameType') as usr_game_type
		 ,app_id
		 ,install_datetime
		 ,os
		 ,ip
		 ,country_cn
		 ,country
		 ,city_cn
		 ,city
		 ,uuid
		 ,distinct_id
		 ,event_name
		 ,event_timestamp
		 ,event_datetime
		 ,properties
		 ,app_version
		 ,ram
		 ,disk
		 ,network_type
		 ,fps
		 ,from_json(properties, 'struct<clean: array<string>>').clean AS clean
		 ,(ABS(HASH(distinct_id)) % 10) + 1 AS group_id 
		 ,dt
		 from hungry_studio.dwd_block_blast_gp_event_data_hi
		 where dt = '{DT}'
			and event_name in('game_touchend_block_done', 'usr_data_game_start')
		)
		 lateral view json_tuple(
				properties,
				'game_id',
				'game_type',
				'round_id',
				'travel_id',
				'travel_lv',
				'matrix',
				'position',
				'block_id',
				'index_id',
				'rec_strategy',
				'rec_strategy_fact',
				'combo_cnt',
				'gain_score',
				'gain_item',
				'block_list',
				'last_click_time',
				'click_rank_per_round',
				'gain_score_per_done',
				'is_clean_screen',
				'weight',
				'put_rate',
				'userwaynum',
				'block_down_color',
				'session_id',
				'block_shape_list',
		        'block_shape',
		        'design_postion_upleft',
		        'design_position'
			) t2 as game_id,
			game_type,
			round_id,
			travel_id,
			travel_lv,
			matrix,
			position,
			block_id,
			index_id,
			rec_strategy,
			rec_strategy_fact,
			combo_cnt,
			gain_score,
			gain_item,
			block_list,
			last_click_time,
			click_rank_per_round,
			gain_score_per_done,
			`is_clean_screen`,
		    `weight`,
		    `put_rate`,
		    `userwaynum`,
		    `block_down_color`,
		    `session_id`,
		    `block_shape_list`,
		    `block_shape`,
		    `design_postion_upleft`,
		    `design_position`
		'''.format(tmp_tbl_name=tmp_tbl_name, DT=DT,tbl_path=tbl_path))

    print('''执行创建并写入default.{tmp_tbl_name}完成,执行时间{DT}'''.format(tmp_tbl_name=tmp_tbl_name,DT=DT))

    # 关闭 Spark 会话
    spark.stop()
