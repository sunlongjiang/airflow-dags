from pyspark.sql import SparkSession
import sys



from pyspark.sql.types import DoubleType, StructType, StructField
from datetime import datetime

if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("block_blast_gp_block_action_block_di_pre").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    HOUR = sys.argv[2]
    begin_id = sys.argv[3]
    end_id = sys.argv[4]
    print("输入参数：")
    print(f"DT: {DT}, HOUR: {HOUR}, begin_id: {begin_id},end_id: {end_id}")
    F_DT = datetime.strptime(DT, "%Y-%m-%d").strftime("%Y%m%d")
    now = datetime.now()
    current_time_str = now.strftime("%H%M%S")
    print(current_time_str)
    tmp_tbl_name = f"""temp_dws_block_blast_gp_block_action_di_{F_DT}_{HOUR}"""
    tbl_path = f"""temp_dws_block_blast_gp_block_action_di_{F_DT}_{current_time_str}"""
    print('''查询的表为：default.{tmp_tbl_name}'''.format(tmp_tbl_name=tmp_tbl_name))
    f_sql = '''
     insert overwrite table hungry_studio.dwd_block_blast_gp_block_action_block_pre_di
		select device_id,
			app_id,
			install_datetime,
			os,
			ip,
			country_cn country,
			city_cn city,
			uuid,
			distinct_id,
			event_name,
			event_timestamp,
			event_datetime,
			game_id,
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
			lag(event_timestamp) over(partition by distinct_id,game_id,game_type order by event_timestamp asc) as lag_event_timestamp,
            lag(matrix) over(partition by distinct_id,game_id,game_type order by event_timestamp asc) as lag_matrix, -- 上次盘面
            --为是否处于is_combo_status 准备数据
            lag(clean, 5) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_5,
            lag(clean, 4) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_4,
            lag(clean, 3) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_3,
            lag(clean, 2) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_2,
            lag(clean, 1) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_1,
            lead(clean, 3) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lead_clean_3,
            lead(clean, 2) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lead_clean_2,
            lead(clean, 1) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lead_clean_1,
            lag(cast(combo_cnt as int), 1) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_combo_cnt_1,
            lag(cast(combo_cnt as int), 2) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_combo_cnt_2,
			case
				when replace(
					matrix,
					'-1',
					''
				) rlike '[0-9]' then false
				when replace(
					matrix,
					'-1',
					''
				) is null then false else true
			end as is_clear_screen,
			IF(SIZE(clean) = 0, FALSE, TRUE) AS is_blast,
            IF(SIZE(clean) = 0, 0, SIZE(clean)) AS blast_row_col_cnt,
			CASE
              WHEN CAST(block_id AS INT) = 1 THEN 1
              WHEN CAST(block_id AS INT) = 2 THEN 2
              WHEN CAST(block_id AS INT) IN (3, 37, 38) THEN 2
              WHEN CAST(block_id AS INT) IN (4, 5, 6, 15, 27, 28, 39, 40, 41) THEN 3
              WHEN CAST(block_id AS INT) IN (7, 8, 9, 10, 14, 16, 17, 18, 19, 20, 25, 26, 29, 30, 31, 32, 33, 34, 42) THEN 4
              WHEN CAST(block_id AS INT) IN (11, 12, 21, 22, 23, 24) THEN 5
              WHEN CAST(block_id AS INT) IN (35, 36) THEN 6
              WHEN CAST(block_id AS INT) = 13 THEN 9
              END AS common_block_cnt,
			case
				when is_clear_screen = true then 300 -- 当处于清屏时，直接获得300分
				when is_blast = false then common_block_cnt -- 当不存在消除时，按块的个数计算得分
				when combo_cnt <> '-1'
				and blast_row_col_cnt = 1 then (combo_cnt + 2) * 10 + common_block_cnt -- 当处于combo且消除行列数为1时，单次得分为：（combo数+2）*10 + 块的个数
				when combo_cnt <> '-1'
				and blast_row_col_cnt > 1 then (combo_cnt + 2) * blast_row_col_cnt *(blast_row_col_cnt -1) * 10 + common_block_cnt -- 当处于combo且消除行列数大于1时，单次得分为：（combo数+2）*消除行列数*（消除行列数-1）*10 + 块的个数
				else blast_row_col_cnt * 10 + common_block_cnt --仅完成消除时，单次得分为：行列消除数 * 10 + 块的个数
			end step_score,
			row_number() over (partition by game_id,game_type,distinct_id,round_id order by event_timestamp asc) as block_index_id,
		    cast(coalesce(last_click_time,0) as bigint ) as last_click_time,
		    cast(`gain_score_per_done` as int ) as gain_score_per_done,
		    cast(`is_clean_screen` as int ) as is_clean_screen,
		    cast(`weight` as float) as weight,
		    cast(`put_rate` as float) as put_rate,
		    IF(clean IS NULL OR size(clean) = 0, 0, 1) AS clean_times,
            IF(clean IS NULL OR size(clean) = 0, 0, size(clean)) AS clean_cnt,
		    app_version,
		    ram,
            disk,
            cast(block_down_color as int) block_down_color,
			design_position,
			network_type,
			session_id,
		    split(block_shape_list,',') as block_shape_list,
		    block_shape,
		    split(design_postion_upleft,',') as design_postion_upleft,
		    fps,
			dt,
			group_id 
		from 
		default.temp_dws_block_blast_gp_block_action_di_pre_0_{F_DT}_{HOUR} a
		where group_id>={begin_id} and group_id<={end_id}
		'''.format(tmp_tbl_name=tmp_tbl_name, F_DT=F_DT,HOUR=HOUR,tbl_path=tbl_path,begin_id=begin_id,end_id=end_id)
    print(f_sql)
    spark.sql(f_sql)

    print("""mid table done!""")
    # 关闭 Spark 会话
    spark.stop()
