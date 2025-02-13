import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 创建 Spark 会话
    date_str = sys.argv[1]
    hour_str = sys.argv[2]
    min_str = sys.argv[3]

    app_name = f"dwd_adx_server_event_data_hi_{date_str}_{hour_str}_{min_str}"
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    print(f"app_name = {app_name}")
    ##请求
    request_sql = f"""
                select t.pid,
                t.p_type,
                t.dsp_id,
                t.dsp_name,
                t.dsp_g_name,
                t.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                t.ad_type,
                t.adomain,
                t.devicetype,
                t.dt,
                t.hour,
                t.crid,
                count(t.rid) request_pv,
                sum(case when cast(t.fill as int) == 1 then 1 else 0 end) as fill_pv,
                sum(case when cast(t.ret_code as int) == 10000 then 1 else 0 end) as wim_pv
                from (
            SELECT 
                t.pid,
                t.p_type,
                bid.dsp_id,
                bid.dsp_name,
                bid.dsp_g_name,
                bid.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                bid.ad_type,
                bid.adomain,
                t.devicetype,
                t.dt,
                t.hour,
                t.rid,
                bid.fill,
                bid.ret_code,
                bid.index,
                bid.crid,
                ROW_NUMBER() OVER (PARTITION BY t.rid, bid.index ORDER BY t.dt DESC, t.hour DESC) AS rn
            FROM 
                hungry_studio.dwd_adx_server_event_data_request_hi t
            LATERAL VIEW explode(t.bid_info) bid_table AS bid
            LATERAL VIEW json_tuple(bid, 'dsp_g_name', 'dsp_id', 'ad_type', 'dsp_name', 'adomain', 'tagid','fill','ret_code','index','crid') bid 
            AS dsp_g_name, dsp_id, ad_type, dsp_name,adomain,tagid,fill,ret_code,index,crid
            where dt = '{date_str}' and hour = '{hour_str}'
            )t where t.rn = 1 
            group by t.pid,
                t.p_type,
                t.dsp_id,
                t.dsp_name,
                t.dsp_g_name,
                t.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                t.ad_type,
                t.adomain,
                t.devicetype,
                t.dt,
                t.hour,
                t.crid
    """
    print(request_sql)
    spark.sql(request_sql).createOrReplaceTempView("request_event_source")
    ##曝光
    show_sql = f"""
    select  t.pid,
                t.p_type,
                t.dsp_id,
                t.dsp_name,
                t.dsp_g_name,
                t.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                t.ad_type,
                t.adomain,
                t.devicetype,
                t.dt,
                t.hour,
                t.crid,
                count(t.rid) as imp_pv,
                sum(ecpm)/1000  as revenue
                from (   
 SELECT 
                t.pid,
                t.p_type,
                t.dsp_id,
                t.dsp_name,
                t.dsp_group_id as dsp_g_name,
                t.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                t.ad_type,
                t.adomain,
                t.devicetype,
                t.dt,
                t.hour,
                t.crid,
                t.rid,
                cast(t.ecpm as DECIMAL(18,9)) as ecpm,
                ROW_NUMBER() OVER (PARTITION BY t.rid ORDER BY t.dt DESC, t.hour DESC) AS rn
            FROM 
                hungry_studio.dwd_adx_server_event_data_show_hi t
            where dt = '{date_str}' and hour = '{hour_str}'
) t where rn = 1
group by  t.pid,
                t.p_type,
                t.dsp_id,
                t.dsp_name,
                t.dsp_g_name,
                t.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                t.ad_type,
                t.adomain,
                t.devicetype,
                t.dt,
                t.hour,
                t.crid
    """
    print(show_sql)
    spark.sql(show_sql).createOrReplaceTempView("show_event_source")
    ##点击
    click_sql = f"""
    select  t.pid,
                t.p_type,
                t.dsp_id,
                t.dsp_name,
                t.dsp_g_name,
                t.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                t.ad_type,
                t.adomain,
                t.devicetype,
                t.dt,
                t.hour,
                t.crid,
                count(t.rid) as click_pv
                from (
                
 SELECT 
                t.pid,
                t.p_type,
                t.dsp_id,
                t.dsp_name,
                t.dsp_group_id as dsp_g_name,
                t.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                t.ad_type,
                t.adomain,
                t.devicetype,
                t.dt,
                t.hour,
                t.rid,
                t.crid,
                ROW_NUMBER() OVER (PARTITION BY t.rid ORDER BY t.dt DESC, t.hour DESC) AS rn
            FROM 
                hungry_studio.dwd_adx_server_event_data_click_hi t
            where dt = '{date_str}' and hour = '{hour_str}'
) t  where rn = 1 
group by
                t.pid,
                t.p_type,
                t.dsp_id,
                t.dsp_name,
                t.dsp_g_name,
                t.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                t.ad_type,
                t.adomain,
                t.devicetype,
                t.dt,
                t.hour,
                t.crid
    """
    print(click_sql)
    spark.sql(click_sql).createOrReplaceTempView("click_event_source")

    ##聚合
    insert_sql = """
     insert overwrite table hungry_studio.dws_adx_server_event_data_hi
    SELECT 
                t.pid,
                t.p_type,
                t.dsp_id,
                t.dsp_name,
                t.dsp_g_name,
                t.tagid,
                t.country,
                t.os,
                t.bundle_id,
                t.ver_n,
                t.ad_type,
                t.adomain,
                t.devicetype, 
                t.request_pv,
                t.fill_pv,
                t.wim_pv,
                if(s.imp_pv is not null,s.imp_pv, 0) as imp_pv,
                if(c.click_pv is not null,c.click_pv, 0) as click_pv,
                cast(if(s.revenue is not null,s.revenue, 0.000000000) as DECIMAL(18,9)) as revenue,
                t.crid,
                t.dt,
                t.hour 
            FROM request_event_source t
            left join show_event_source s
            on  t.pid = s.pid and t.p_type = s.p_type and t.dsp_id = s.dsp_id and t.dsp_name = s.dsp_name and 
            t.dsp_g_name = s.dsp_g_name and t.tagid = s.tagid and t.country = s.country and t.os = s.os
            and t.bundle_id = s.bundle_id and t.ver_n = s.ver_n and t.ad_type = s.ad_type and t.adomain =s.adomain
            and t.devicetype = s.devicetype and t.dt = s.dt and t.hour = s.hour and t.crid = s.crid
             left join click_event_source c
             on t.pid = c.pid and t.p_type = c.p_type and t.dsp_id = c.dsp_id and t.dsp_name = c.dsp_name and 
            t.dsp_g_name = c.dsp_g_name and t.tagid = c.tagid and t.country = c.country and t.os = c.os
            and t.bundle_id = c.bundle_id and t.ver_n = c.ver_n and t.ad_type = c.ad_type and t.adomain =c.adomain
            and t.devicetype = c.devicetype and t.dt = c.dt and t.hour = c.hour and t.crid = c.crid
    """
    print(insert_sql)
    spark.sql(insert_sql)
    # 关闭 Spark 会话
    spark.stop()
