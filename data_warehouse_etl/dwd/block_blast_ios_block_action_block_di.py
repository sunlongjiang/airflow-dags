from datetime import datetime
from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("dws_block_blast_ios_block_action_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    HOUR = sys.argv[2]
    F_DT = datetime.strptime(DT, "%Y-%m-%d").strftime("%Y%m%d")
    tmp_tbl_name = f"""temp_dws_block_blast_ios_block_action_di_pre_0_{F_DT}_{HOUR}"""

    print('''注册java函数''')
    spark.udf.registerJavaFunction("get_area_complex_value", "com.hungrystudio.utf.block.AreaComplexValue")
    spark.udf.registerJavaFunction("most_common_element", "com.hungrystudio.utf.common.MosCommonElement")
    spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict;''')
    exec_sql = '''
  insert overwrite table hungry_studio.dwd_block_blast_ios_block_action_block_di
			select
			a.distinct_id device_id -- l临时
			,app_id
			,install_datetime
			,os
			,ip
			,country
			,city
			,uuid
			,a.distinct_id
			,event_name
			,event_timestamp
			,event_datetime
			,game_id
			,game_type
			,round_id
			,travel_id
			,travel_lv
			,matrix
			,position
			,concat('[', concat_ws(',', clean), ']') as clean
			,block_id
			,index_id
			,rec_strategy
			,rec_strategy_fact
			,combo_cnt
			,gain_score
			,gain_item
			,block_list
			,lag_event_timestamp
			,(event_timestamp - lag_event_timestamp) / 1000 AS time_diff_in_seconds
			,most_common_element( replace( replace(replace(rec_strategy_fact, '[', ''), ']', ''), '"', '' )) as rec_strategy_fact_most
			,lag_matrix
			,is_clear_screen
			,is_blast
			,blast_row_col_cnt
			,CASE 
                WHEN size(clean) > 0 THEN
                    CASE 
                        WHEN (IF(size(lag_clean_3) > 0, 1, 0) + IF(size(lag_clean_2) > 0, 1, 0) + IF(size(lag_clean_1) > 0, 1, 0)) > 0 THEN TRUE
                        WHEN (IF(size(lag_clean_3) > 0, 1, 0) + IF(size(lag_clean_2) > 0, 1, 0) + IF(size(lag_clean_1) > 0, 1, 0)) = 0
                        AND combo_cnt = '-1'
                        AND (IF(size(lead_clean_3) > 0, 1, 0) + IF(size(lead_clean_2) > 0, 1, 0) + IF(size(lead_clean_1) > 0, 1, 0)) > 0 THEN TRUE
                        ELSE FALSE
                    END
                ELSE
                    CASE 
                        WHEN (IF(size(lag_clean_2) > 0, 1, 0) + IF(size(lag_clean_1) > 0, 1, 0)) = 0 THEN FALSE
                        WHEN (IF(size(lag_clean_2) > 0, 1, 0) + IF(size(lag_clean_1) > 0, 1, 0)) = 2 THEN TRUE
                        WHEN size(lag_clean_1) > 0
                            AND lag_combo_cnt_1 = -1
                            AND (IF(size(lead_clean_1) > 0, 1, 0) + IF(size(lead_clean_2) > 0, 1, 0)) >= 1 THEN TRUE
                        WHEN size(lag_clean_1) > 0
                            AND lag_combo_cnt_1 > -1
                            AND (IF(size(lag_clean_3) > 0, 1, 0) + IF(size(lag_clean_4) > 0, 1, 0)) >= 1 THEN TRUE
                        WHEN size(lag_clean_2) > 0
                            AND lag_combo_cnt_2 = -1
                            AND size(lead_clean_1) > 0 THEN TRUE
                        WHEN size(lag_clean_2) > 0
                            AND lag_combo_cnt_2 > -1
                            AND (IF(size(lag_clean_3) > 0, 1, 0) + IF(size(lag_clean_4) > 0, 1, 0) + IF(size(lag_clean_5) > 0, 1, 0)) >= 1 THEN TRUE
                        ELSE FALSE
                    END
                END AS is_combo_status
			,common_block_cnt
			,step_score
			,block_index_id
			,cast(get_area_complex_value(lag_matrix) as int) matrix_complex_value
			,-0.1 as block_line_percent
			,-0.1 as corner_outside_percent
			,-0.1 as corner_inside_percent
			,sum((event_timestamp - lag_event_timestamp) / 1000) over(partition by game_id,game_type,distinct_id) time_accumulate_seconds --  double COMMENT '当前块距离本局游戏的开始的时长，秒级别的'
			,max(cast(round_id as Integer)) over(partition by game_id,game_type,distinct_id ) max_round_id -- 最大轮数2024-12-11数据开始准确
			,case when round_id=max(cast(round_id as Integer)) over(partition by game_id,game_type,distinct_id ) then true else false end  is_final_round -- boolean COMMENT '此轮是不是最后一轮' 最后一轮 2024-12-11数据开始准确
			,case when round_id=max(cast(round_id as Integer)) over(partition by game_id,game_type,distinct_id ) and block_index_id=max(block_index_id) over(partition by game_id,game_type,distinct_id,round_id )  then true 			else false end is_lethal_block -- boolean COMMENT '是不是致死块' 最后一轮里面的最后一个放块 2024-12-11数据开始准确
			,sum(step_score) over(partition by game_id,game_type,distinct_id) - step_score lag_accumulate_score -- double COMMENT '出此块前的累计分数'
			,sum(step_score) over(partition by game_id,game_type,distinct_id) accumulate_score -- double COMMENT '出此块后的累计分数'
			,cast((event_timestamp-last_click_time) / 1000 as int) as time_action_in_seconds -- 落块动作的时间
			,(event_timestamp - lag_event_timestamp) / 1000	- (event_timestamp-last_click_time) / 1000 as time_think_in_seconds --落块-思考时间
			,0 as last_click_time
			,cast(`gain_score_per_done` as Integer) as gain_score_per_done
			,cast(`is_clean_screen` as Integer) as is_clean_screen
			,cast(`weight` as float) as weight
			,cast(`put_rate` as float) as put_rate
			,0 as userwaynum
			,clean_times
			,clean_cnt
			,sum(clean_times) over(partition by game_id,game_type,distinct_id)   as accumulate_clean_times
			,sum(clean_cnt) over(partition by game_id,game_type,distinct_id)     as accumulate_clean_cnt
			,app_version
			,ram
			,disk
			,cast(get_area_complex_value(matrix) as int) cur_matrix_complex_value
			,block_down_color
			,design_position
			,1 as is_sdk_sample
			,network_type
			,session_id
			,block_shape_list
			,block_shape
			,design_postion_upleft
			,fps
			,-1 as fact_line
		    ,case when block_id in (1)  then 4  
                  when block_id in (2,3) then 6  
                  when block_id in (4 ,5 ,6 ,9 ,15 ,27 ,28 ,37 ,38 ) then 8 
                  when block_id in  (7 ,8 ,10 ,14 ,16 ,17 ,18 ,19 ,20 ,25 ,26 ,29 ,30 ,31 ,32 ,33 ,34 ,35 ,36 ,42)   then 10 
                  when block_id in (11, 12 ,13 ,21 ,22 ,23 ,24 ,39 ,40 ,41)   then 12 
             end as total_line
			,dt
			from hungry_studio.dwd_block_blast_ios_block_action_block_pre_di a
			where dt='{DT}' and event_name = 'game_touchend_block_done' and (event_timestamp - lag_event_timestamp)>0;
     '''.format(DT=DT, tmp_tbl_name=tmp_tbl_name)
    print(exec_sql)
    spark.sql(exec_sql)

    # 关闭 Spark 会话
    spark.stop()
