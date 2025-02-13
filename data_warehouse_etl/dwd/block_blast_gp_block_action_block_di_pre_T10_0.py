from pyspark.sql import SparkSession
import sys


from datetime import datetime

if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("dws_block_blast_gp_block_action_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    HOUR = sys.argv[2]
    start_hour = sys.argv[3]
    end_hour = sys.argv[4]




    F_DT = datetime.strptime(DT, "%Y-%m-%d").strftime("%Y%m%d")
    now = datetime.now()
    current_time_str = now.strftime("%H%M%S")
    print(current_time_str)
    tmp_tbl_name = f"""temp_dws_block_blast_gp_block_action_di_pre_0_{F_DT}_{HOUR}"""
    # tmp_tbl_name = f"""temp_dws_block_blast_gp_block_action_di_pre_0_{F_DT}_{HOUR}"""
    print(tmp_tbl_name)
    spark.sql('''
    insert overwrite TABLE default.{tmp_tbl_name}
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
        ,dt
        ,game_id
		,game_type
        ,round_id
        ,travel_id
        ,travel_lv
        ,matrix
        ,position
        ,from_json(properties, 'struct<clean: array<string>>').clean AS clean
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
        --新增字段
        ,session_id
		,block_shape_list
		,block_shape
		,design_postion_upleft
		,fps
		,design_position
        ,group_id 
        ,case when hour>='00' and hour<='07' then 1 
          when hour>='08' and hour<='15' then 2
          when hour>='16' and hour<='23' then 3 end as hour 
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
		 ,(ABS(HASH(distinct_id)) % 10) + 1 AS group_id 
		 ,dt
		 ,hour
		 -- ,ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY event_timestamp) AS rn 效率考虑
		 ,1 as rn
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
				'clean',
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
			clean,
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
		    where rn = 1;
		'''.format(tmp_tbl_name=tmp_tbl_name, DT=DT,HOUR = HOUR,start_hour=start_hour,end_hour=end_hour))

    print("""mid table done!""")
    # 关闭 Spark 会话
    spark.stop()
