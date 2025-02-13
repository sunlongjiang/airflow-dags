from pyspark.sql import SparkSession
import sys
from datetime import datetime


if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("dws_block_blast_ios_block_action_round_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    F_DT = datetime.strptime(DT, "%Y-%m-%d").strftime("%Y%m%d")
    current_time_str = datetime.now().strftime("%H%M%S")
    spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict;''')
    spark.sql(f''' drop table  if exists  hungry_studio_dev.tmp_f_dws_block_blast_ios_block_action_round_di_pre_{F_DT}''')
    spark.sql(f"""create table if not exists  hungry_studio_dev.tmp_f_dws_block_blast_ios_block_action_round_di_pre_{F_DT}
         location 's3://hungry-studio-tmp/tmp_f_dws_block_blast_ios_block_action_round_di_pre/{DT}_{current_time_str}' as
            select 
            device_id,
                app_id,
                install_datetime,
                os,
                ip,
                country,
                city,
                distinct_id,
                event_name,
                min_event_timestamp, -- 轮开始时间戳 
                min_event_time, -- 轮开始时间
                max_event_timestamp,
                max_event_datetime, -- 轮最后落块时间
                game_id,
                game_type,
                round_id,
                travel_id,
                travel_lv,
                matrix, -- 当前排面
                clear_screen_cnt, -- 清屏次数
                blast_cnt,--消除次数
                blast_row_col_cnt, -- 消除行列数
                is_combo_status_cnt, -- 在combo状态的步数
                common_block_cnt, -- 块基础得分
                round_score, -- 单轮得分
                time_diff_in_seconds, -- 单轮耗时
                matrix_complex_value,  -- 轮初始排面复杂度
                rec_strategy,  -- 轮初始理论出块策略
                rec_strategy_fact , -- 轮初始实际出块策略
                ARRAY_JOIN(
                    TRANSFORM(
                        ARRAY_SORT(step_details, (x, y) -> CASE 
                            WHEN x.block_index_id < y.block_index_id THEN -1 
                            WHEN x.block_index_id > y.block_index_id THEN 1 
                            ELSE 0 
                        END),
                        x -> x.index_id
                        ),
                        ''
                )AS index_pos_list, -- 轮内出块位置信息

                block_step_cnt, -- 步数
    	        game_combo_cnt, -- 段数
    	        game_combo_step_cnt, -- combo 总长度
    	        game_combo_block_step_cnt, --combo内消除步数

                multiple_block_step_cnt, -- 多消步数
                game_combo_block_step_cnt_rate, -- combo消除比例 一局，在combo内消除的次数/combo总长度
                game_combo_step_cnt_rate, -- combo浓度 一局，combo总长度/摆放块次数
                avg_game_combo_step_cnt,-- combo平均段长度 一局，combo总长度/combo段数
                block_step_cnt_rate,-- 消除比例 一局，消除次数/摆放块次数
                multiple_block_step_cnt_rate,-- 多消比例
                null death_percent_dict,
                block_list , -- 轮初始 预备方块列表
                -- 阳角率
                -- 阴角率
                time_action_in_seconds,
                time_think_in_seconds
                ,last_click_time
                ,gain_score_per_done
                ,gain_score_per_done_avg
                ,clean_cnt
                ,clean_cnt_avg
                ,is_clean_screen
                ,weight_avg            
                ,put_rate_avg            
                ,userwaynum                     
                ,gain_score                     
                ,combo_cnt    
                ,accumulate_clean_times
                ,accumulate_clean_cnt
                ,app_version                 
                ,clean_screen_cnt                 
                ,is_final_round  
                ,revive_cnt
                ,CONCAT('[', ARRAY_JOIN(TRANSFORM(step_details, x -> x.block_down_color), ','), ']') AS color_list
                ,network_type
                ,cast(game_way_type as int) as game_way_type
                ,user_waynum
                ,cast(active_ab_game_way_type as int) as active_ab_game_way_type
                ,nowwayarr
                ,latest_now_way
                ,TRANSFORM(step_details, x -> x.weight) AS weight_list
                ,session_id
                ,combo5_cnt
                ,combo10_cnt
                ,combo15_cnt
                ,CONCAT('[', ARRAY_JOIN(TRANSFORM(step_details, x -> x.common_block_cnt), ','), ']') AS common_block_cnt_list
                ,CONCAT('[', ARRAY_JOIN(TRANSFORM(step_details, x -> x.fact_line), ','), ']') AS fact_line_list
                ,CONCAT('[', ARRAY_JOIN(TRANSFORM(step_details, x -> x.total_line), ','), ']') AS total_line_list
                ,max_blast_row_col_cnt
                ,CONCAT('[', ARRAY_JOIN(TRANSFORM(step_details, x -> x.block_index_id), ','), ']') AS block_index_id_list
                ,CONCAT('[', ARRAY_JOIN(TRANSFORM(step_details, x -> x.design_position), ','), ']') AS design_position_list
                ,CONCAT('[', ARRAY_JOIN(TRANSFORM(step_details, x -> x.position), ','), ']') AS position_list
                ,first_block_time
                ,revive_show_cnt 
                ,revive_click_cnt
                ,d.cost_time
                ,d.down_reason
                ,dt
      from           
     (
              select  
                device_id,
                app_id,
                install_datetime,
                max(os) os,
                max(ip) ip,
                max(country) country,
                max(city) city,
                distinct_id,
                event_name,
                min(lag_event_timestamp) min_event_timestamp, -- 轮开始时间戳 
                min(from_unixtime(lag_event_timestamp/1000, 'yyyy-MM-dd HH:mm:ss')) min_event_time, -- 轮开始时间
                max(event_timestamp) max_event_timestamp,
                max(event_datetime)  max_event_datetime, -- 轮最后落块时间
                game_id,
                game_type,
                round_id,
                travel_id,
                travel_lv,
                max(case when block_index_id=1 then lag_matrix end) as matrix, -- 当前排面
                sum(case when is_clear_screen=true then 1 end ) as  clear_screen_cnt, -- 清屏次数
                sum(case when is_blast then 1 else 0 end) blast_cnt,--消除次数
                sum(blast_row_col_cnt ) as  blast_row_col_cnt, -- 消除行列数
                sum(case when is_combo_status then 1 else 0 end ) as  is_combo_status_cnt, -- 在combo状态的步数
                sum(common_block_cnt) common_block_cnt, -- 块基础得分
                sum(step_score) round_score, -- 单轮得分
                sum(time_diff_in_seconds) time_diff_in_seconds, -- 单轮耗时
                max(case when block_index_id=1 then matrix_complex_value end) matrix_complex_value,  -- 轮初始排面复杂度
                max(case when block_index_id=1 then rec_strategy end) rec_strategy,  -- 轮初始理论出块策略
                max(case when block_index_id=1 then rec_strategy_fact end) rec_strategy_fact , -- 轮初始实际出块策略
                count(1) block_step_cnt, -- 步数
                --修复字段
    	        sum(case when is_blast and combo_cnt='1' then 1 else 0 end) game_combo_cnt, -- 段数
    	        sum(case when is_combo_status then 1 else 0 end) game_combo_step_cnt, -- combo 总长度
    	        sum(case when is_combo_status and is_blast then 1 else 0 end) game_combo_block_step_cnt, --combo内消除步数

                sum(case when is_blast and cardinality(split(clean,','))>1  then 1 else 0 end) multiple_block_step_cnt, -- 多消步数
                round(sum(case when is_combo_status and is_blast then 1 else 0 end)/sum(case when is_combo_status then 1 else 0 end),4) game_combo_block_step_cnt_rate, -- combo消除比例 一局，在combo内消除的次数/combo总长度
                round(sum(case when is_combo_status then 1 else 0 end)/count(1),4) game_combo_step_cnt_rate, -- combo浓度 一局，combo总长度/摆放块次数
                round(sum(case when is_combo_status then 1 else 0 end)/sum(case when is_blast and combo_cnt='0' then 1 else 0 end),4) avg_game_combo_step_cnt,-- combo平均段长度 一局，combo总长度/combo段数
                round(sum(case when is_blast  then 1 else 0 end)/count(1),4) block_step_cnt_rate,-- 消除比例 一局，消除次数/摆放块次数
                round(sum(case when is_blast and cardinality(split(clean,','))>1  then 1 else 0 end)/count(1),4)  multiple_block_step_cnt_rate,-- 多消比例

                max(case when block_index_id=1 then replace(block_list,'-1',block_id) end) block_list , -- 轮初始 预备方块列表
                -- 阳角率
                -- 阴角率
                sum(time_action_in_seconds) time_action_in_seconds, -- 单轮耗时
                sum(time_think_in_seconds) time_think_in_seconds -- 单轮耗时
                ,max(last_click_time) as last_click_time
                ,sum(gain_score_per_done) as gain_score_per_done
                ,avg(gain_score_per_done) as gain_score_per_done_avg
            	,sum(if(clean=='[]', 0, size(split(clean,',')))) as clean_cnt
            	,avg(if(clean=='[]', 0, size(split(clean,',')))) as clean_cnt_avg
                ,max(is_clean_screen) as is_clean_screen
                ,avg(weight) as weight_avg
                ,avg(put_rate) as put_rate_avg
                ,max(userwaynum) as userwaynum
                ,max(cast(gain_score as Integer)) as gain_score
                ,max(cast(combo_cnt as Integer)) as combo_cnt
                ,max(accumulate_clean_times)        as accumulate_clean_times
                ,max(accumulate_clean_cnt)          as accumulate_clean_cnt
                ,max(app_version)                   as app_version
                ,sum(if(is_clean_screen = True, 1, 0))   as clean_screen_cnt
                ,max(is_final_round)  as is_final_round
                ,max(network_type)  as network_type
                 --新增字段
                ,max(session_id) as session_id
              --新增字段
                ,sum(case when is_blast and combo_cnt='5' then 1 else 0 end) combo5_cnt -- 达到combo5的次数
                ,sum(case when is_blast and combo_cnt='10' then 1 else 0 end) combo10_cnt -- 达到combo10的次数
                ,sum(case when is_blast and combo_cnt='15' then 1 else 0 end) combo15_cnt -- 达到combo15的次数 
                  --新增字段
                ,max(blast_row_col_cnt)  as max_blast_row_col_cnt -- 单块最大消除行列数 
                ,max(case when round_id=1 then event_timestamp end) AS first_block_time -- 本局首次落块时间
                ,SORT_ARRAY(
                    COLLECT_LIST(
                        STRUCT(index_id, block_down_color, weight, common_block_cnt, fact_line, total_line, block_index_id, design_position, position)
                    ), 
                    TRUE --按照 index_id 升序排列
                ) AS step_details
                ,dt
                from hungry_studio.dwd_block_blast_ios_block_action_block_di  t where dt='{DT}' --  and distinct_id='B790632A-83FE-4AA9-8066-333409E2A1D7' 

                group by 
                device_id,
                app_id,
                install_datetime,
                distinct_id,
                event_name,
                game_id,
                game_type,
                round_id,
                travel_id,
                travel_lv,
                dt 
    ) a 
     left join 
            (
             select 
            distinct_id as revive_distinct_id,
            game_id as revive_game_id,
            game_type as revive_game_type,
            round_id as revive_round_id,
            cast(revive_cnt as int) as revive_cnt,
            cast(revive_show_cnt as int) as revive_show_cnt,
            cast(revive_click_cnt as int) as revive_click_cnt,
            game_revive_color_list as revive_color_list,
            game_end_color_list as end_color_list
             from hungry_studio.dws_block_blast_ios_block_revive_round_di where dt = '{DT}' 
            ) b 
            on a.distinct_id = b.revive_distinct_id 
            and a.game_id = b.revive_game_id 
            and a.round_id = b.revive_round_id
            and a.game_type = b.revive_game_type
     left join 
            (
            select 
            distinct_id as user_way_distinct_id,
            game_way_type as game_way_type,
            user_waynum as user_waynum,
            active_ab_game_way_type as active_ab_game_way_type,
            nowwayarr as nowwayarr,
            case when nowwayarr is not null then nowwayarr[size(nowwayarr) -1] end as latest_now_way
            from hungry_studio.dim_block_blast_ios_user_label_ha where dt = '{DT}' and hour = '23'
            ) c 
            on a.distinct_id = c.user_way_distinct_id 
     left join  
            (
            select 
            distinct_id as cost_distinct_id,
            game_id as cost_game_id,
            game_type as cost_game_type,
            round_id as cost_round_id,
            cost_time as cost_time,
            down_reason as down_reason 
             from hungry_studio.dws_block_blast_ios_game_get_block_end_di where dt = '{DT}' 
            ) d
            on a.distinct_id = d.cost_distinct_id 
            and a.game_id = d.cost_game_id 
            and a.round_id = d.cost_round_id
            and a.game_type = d.cost_game_type
      """)

    print(f"hungry_studio_dev.tmp_f_dws_block_blast_ios_block_action_round_di_pre_{F_DT} done!")
    # 关闭 Spark 会话
    spark.stop()
