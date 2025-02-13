import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 创建 Spark 会话
    date_str = sys.argv[1]
    hour_str = sys.argv[2]
    min_str = sys.argv[3]

    app_name = f"dws_adx_server_event_data_geoedge_scan_hi_{date_str}_{hour_str}_{min_str}"
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    print(f"app_name = {app_name}")

    exec_sql = f'''
    insert overwrite table hungry_studio.dws_adx_server_event_data_geoedge_scan_hi 
      select 
        a.event_name, 
        a.ver, 
        a.ver_n, 
        a.sdk_ver, 
        a.lid, 
        a.sid, 
        a.rid, 
        a.pid, 
        a.p_type, 
        a.cst, 
        a.target_id, 
        a.dsp_group_id, 
        a.country, 
        a.crid, 
        a.project_id, 
        a.project_name, 
        a.timestamp, 
        a.ab, 
        a.ext, 
        a.adm, 
        b.scan_timestamp, 
        b.creative_screenshot_url, 
        b.landing_page_screenshot_url, 
        b.creative_url, 
        b.landing_page_url, 
        b.resp, 
        b.ext as scan_ext, 
        c.alert_count, 
        d.imp_total_count, 
        d.click_total_count, 
        a.bundle_id, 
        a.os, 
        a.dt, 
        a.hour 
      from 
        (
          select 
            event_name, 
            ver, 
            ver_n, 
            sdk_ver, 
            lid, 
            sid, 
            rid, 
            pid, 
            p_type, 
            cst, 
            target_id, 
            dsp_group_id, 
            country, 
            crid, 
            project_id, 
            project_name, 
            timestamp, 
            ab, 
            ext, 
            adm, 
            bundle_id, 
            os, 
            dt, 
            hour 
          from 
            hungry_studio.dwd_adx_server_event_data_geoedge_hi 
          where 
            dt = '{date_str}' 
            and hour = '{hour_str}'
        ) a 
        left join (
          select 
            project_id, 
            project_name, 
            crid, 
            timestamp, 
            scan_timestamp, 
            creative_screenshot_url, 
            landing_page_screenshot_url, 
            creative_url, 
            landing_page_url, 
            country, 
            resp, 
            ext, 
            dt, 
            hour 
          from 
            hungry_studio.dwd_adx_server_event_data_scan_hi 
          where 
            dt = '{date_str}' 
            and hour = '{hour_str}'
        ) b on a.project_id = b.project_id 
        left join (
          select 
            project_id, 
            count(1) as alert_count 
          from 
            hungry_studio.dwd_adx_server_event_data_alert_hi  
          where 
            dt <= '{date_str}' 
            and hour <= '{hour_str}' group by project_id
        ) c on a.project_id = c.project_id 
        left join (
          select 
            country, 
            os, 
            crid,
            dsp_g_name,
            sum(imp_pv) as imp_total_count, 
            sum(click_pv) as click_total_count 
          from 
            hungry_studio.dws_adx_server_event_data_hi 
          where 
            dt <= '{date_str}' 
            and hour <= '{hour_str}' 
          group by 
            country, 
            os, 
            crid,
            dsp_g_name
        ) d on a.country = d.country 
        and a.os = d.os 
        and a.crid = d.crid     
        and a.dsp_group_id = d.dsp_g_name     
    '''
    print(exec_sql)
    spark.sql(exec_sql)
    # 关闭 Spark 会话
    spark.stop()
