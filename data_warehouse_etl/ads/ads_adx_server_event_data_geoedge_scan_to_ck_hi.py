import functools
import pytz
from pyspark import TaskContext
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import sys
from clickhouse_driver import Client
import time
import random


def get_ck_client(ck_cluster):
    ck_config = {
        "CKClusterNo2": [
            {
                "host": "10.0.12.192",
                "port": 9000,
                "user": "adx_prod_w",
                "password": "8Dam91luHevoKuSs",
                "database": "adx_prod"
            },
            {
                "host": "10.0.4.168",
                "port": 9000,
                "user": "adx_prod_w",
                "password": "8Dam91luHevoKuSs",
                "database": "adx_prod"
            },
            {
                "host": "10.0.12.224",
                "port": 9000,
                "user": "adx_prod_w",
                "password": "8Dam91luHevoKuSs",
                "database": "adx_prod"
            },
            {
                "host": "10.0.9.111",
                "port": 9000,
                "user": "adx_prod_w",
                "password": "8Dam91luHevoKuSs",
                "database": "adx_prod"
            },
            {
                "host": "10.0.12.254",
                "port": 9000,
                "user": "adx_prod_w",
                "password": "8Dam91luHevoKuSs",
                "database": "adx_prod"
            },
            {
                "host": "10.0.4.78",
                "port": 9000,
                "user": "adx_prod_w",
                "password": "8Dam91luHevoKuSs",
                "database": "adx_prod"
            }
        ]

    }
    # 生成一个0到5之间的随机数（包含0和5）
    randend = len(ck_config[ck_cluster]) - 1
    x = random.randint(0, randend)
    ck_client = Client(database=ck_config[ck_cluster][x]["database"],
                       user=ck_config[ck_cluster][x]["user"],
                       password=ck_config[ck_cluster][x]["password"],
                       host=ck_config[ck_cluster][x]["host"],
                       port=ck_config[ck_cluster][x]["port"])
    return ck_client


def delete_data_from_ucloud_ck(delete_sql, optimize_sql, check_sql):
    res_code = 0

    client = get_ck_client('CKClusterNo2')
    try:
        # 执行删除操作
        client.execute(delete_sql)
        print("正在删除数据...")
        time.sleep(2)
        client.execute(optimize_sql)

        # 检查删除是否成功
        print("正在检查删除操作是否完成...")
        result = client.execute(check_sql)

        delete_failed_record_num = 0

        if result[0][0] == 0:
            print(f"{delete_sql} - 删除操作成功完成！")
            res_code = 1
        else:
            delete_failed_record_num = result[0][0]
            print(f"{delete_sql} - 初次检查发现还有 {delete_failed_record_num} 条记录未删除。")
            num = 1
            sleep_seconds = 2
            while num <= sleep_seconds * 30 * 10:  # 10分钟超时时间
                print(f"正在重试删除检查... 睡眠 {sleep_seconds} 秒。")
                client.execute(optimize_sql)
                time.sleep(sleep_seconds)
                result = client.execute(check_sql)
                if result[0][0] == 0:
                    print(f"{delete_sql} - 删除操作成功完成！")
                    res_code = 1
                    break
                else:
                    delete_failed_record_num = result[0][0]
                    print(f"第 {num} 次检查：仍有 {delete_failed_record_num} 条记录未删除。")
                    num += 1

        if res_code == 0:
            print(f"{delete_sql} - 超时！10分钟后仍有 {delete_failed_record_num} 条记录未删除。")

    except Exception as e:
        print(f"删除操作发生错误：{e}")
    finally:
        client.disconnect()

    return res_code


def load_data_to_ucloud_ck(insert_sql, data_dict):
    client = get_ck_client('CKClusterNo2')
    try:
        client.execute(insert_sql, data_dict)
    except Exception as e:
        print(f"插入数据发生错误：{e}")
        sys.exit("插入数据发生错误！")
    finally:
        client.disconnect()
    return 1


if __name__ == "__main__":
    # 初始化 Spark 会话
    spark = SparkSession \
        .builder \
        .appName("ads_adx_server_event_data_to_ck_hi") \
        .config("spark.sql.session.timeZone", "UTC") \
        .enableHiveSupport() \
        .getOrCreate()

    # 读取输入参数
    dt = sys.argv[1]
    hour = sys.argv[2]
    src_table = sys.argv[3]
    ck_table_replica = sys.argv[4]
    ck_table_dist = sys.argv[5]

    print("输入参数：")
    print(
        f"dt: {dt}, hour: {hour}, src_table: {src_table}, ck_table_replica: {ck_table_replica},ck_table_dist: {ck_table_dist}")
    cur_step = 0
    start_time = time.time()
    sync_sql = f"""
                SELECT 
                    COALESCE(event_name,"-") as event_name,
                    COALESCE(ver,"-") as ver,
                    COALESCE(ver_n,"-") as ver_n,
                    COALESCE(sdk_ver,"-") as sdk_ver,
                    COALESCE(lid,"-") as lid,
                    COALESCE(sid,"-") as sid,
                    COALESCE(rid,"-") as rid,
                    COALESCE(pid,"-") as pid,
                    COALESCE(p_type,"-") as p_type,
                    COALESCE(cst,0) as cst,
                    COALESCE(target_id,"-") as target_id,
                    COALESCE(dsp_group_id,"-") as dsp_group_id,
                    COALESCE(country,"-") as country,
                    COALESCE(crid,"-") AS crid, 
                    COALESCE(project_id,"-") AS project_id, 
                    COALESCE(project_name,"-") AS project_name, 
                    COALESCE(timestamp,0) AS timestamp,
                    COALESCE(ab,"-") AS ab,
                    COALESCE(ext,"-") AS ext,
                    COALESCE(adm,"-") AS adm,
                    COALESCE(scan_timestamp,0) AS scan_timestamp,
                    COALESCE(creative_screenshot_url,"-") AS creative_screenshot_url, 
                    COALESCE(landing_page_screenshot_url,"-") AS landing_page_screenshot_url, 
                    COALESCE(creative_url,"-") AS creative_url, 
                    COALESCE(landing_page_url,"-") AS landing_page_url, 
                    COALESCE(resp,"-") AS resp, 
                    COALESCE(scan_ext,"-") AS scan_ext,
                    COALESCE(alert_count,0) AS alert_count, 
                    COALESCE(imp_total_count,0) AS imp_total_count, 
                    COALESCE(click_total_count,0) AS click_total_count, 
                    COALESCE(bundle_id,"-") AS bundle_id, 
                    COALESCE(os,"-") AS os,
                    dt,
                    hour
               FROM {src_table}
               WHERE dt = '{dt}' AND hour = '{hour}'
               """
    sync_df = spark.sql(sync_sql)

    # 删除目标表中当前国家的数据
    delete_ck_current_date_sql = f"ALTER TABLE {ck_table_replica} ON CLUSTER CKClusterNo2 DELETE WHERE dt = '{dt}' AND hour = '{hour}'"
    optimize_cur_partition_sql = f"OPTIMIZE TABLE {ck_table_replica} ON CLUSTER CKClusterNo2 PARTITION ('{dt}','{hour}')"
    check_ck_current_date_sql = f"SELECT COUNT(1) AS num FROM {ck_table_dist} WHERE dt = '{dt}' AND hour = '{hour}'"

    if delete_data_from_ucloud_ck(delete_ck_current_date_sql, optimize_cur_partition_sql,
                                  check_ck_current_date_sql):
        print("数据删除成功！")
    else:
        sys.exit("数据删除失败！")

    # 分批插入数据到 ClickHouse
    insert_sql = f"INSERT INTO {ck_table_dist} VALUES"
    batch_size = 50000
    sync_data = sync_df.collect()

    for i in range(0, len(sync_data), batch_size):
        batch = sync_data[i:i + batch_size]
        load_data_to_ucloud_ck(insert_sql, batch)

    end_time = time.time()
    print(f"处理 {dt} 完成，用时 {end_time - start_time:.2f} 秒。步骤 {cur_step + 1}")
    cur_step += 1

spark.stop()

'''
 CREATE TABLE adx_prod.ads_adx_server_event_data_geoedge_scan_hi_replica on CLUSTER CKClusterNo2(
  `event_name` String COMMENT '事件名称', 
  `ver` String COMMENT '包的版本号 包的版本号(字符串版本号) 1.0.0', 
  `ver_n` String COMMENT '包的版本号（数字版本号）1', 
  `sdk_ver` String COMMENT '广告sdk的版本号', 
  `lid` String COMMENT '三方广告版位id', 
  `sid` String COMMENT '业务侧调用时会有一个id 业务侧只要促发adx sdk请求广告时，会生成一个id', 
  `rid` String COMMENT '请求id adx sdk 请求后端server时都会生成一个id', 
  `pid` String COMMENT 'placement id 广告位id', 
  `p_type` String COMMENT '广告位类型', 
  `cst` UInt32 COMMENT '客户端时间戳', 
  `target_id` String COMMENT '请求dsp的targeting id', 
  `dsp_group_id` String COMMENT 'dsp分组id', 
  `country` String COMMENT '用户所在的国家', 
  `crid` String COMMENT '素材id', 
  `project_id` String COMMENT 'GeoEdge返回的项目ID', 
  `project_name` String COMMENT 'GeoEdge返回的项目名', 
  `timestamp` UInt32 COMMENT '时间戳 13位时间戳，接收到ping上报的时候赋值', 
  `ab` String COMMENT 'ab信息', 
  `ext` String COMMENT '扩展字段', 
  `adm` String COMMENT 'rtb的ADM', 
  `scan_timestamp` UInt32 COMMENT '扫描时间戳', 
  `creative_screenshot_url` String COMMENT '素材URL', 
  `landing_page_screenshot_url` String COMMENT '落地页URL', 
  `creative_url` String COMMENT '原素材URL', 
  `landing_page_url` String COMMENT '原落地页URL', 
  `resp` String COMMENT '响应Json', 
  `scan_ext` String COMMENT 'scan扩展字段', 
  `alert_count` UInt32 COMMENT '累计失败次数', 
  `imp_total_count` UInt32 COMMENT '累计展示次数', 
  `click_total_count` UInt32 COMMENT '累计点击次数',
  `bundle_id` String COMMENT '包名 接入广告应用方包名', 
  `os` String COMMENT '操作系统iOS GP', 
  `dt` String, 
  `hour` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/adx_prod/ads_adx_server_event_data_geoedge_scan_hi', '{replica}')
PARTITION BY (dt,hour)
ORDER BY (project_id)
SETTINGS index_granularity = 8192;


CREATE TABLE adx_prod.ads_adx_server_event_data_geoedge_scan_hi_dist on CLUSTER CKClusterNo2(
   `event_name` String COMMENT '事件名称', 
  `ver` String COMMENT '包的版本号 包的版本号(字符串版本号) 1.0.0', 
  `ver_n` String COMMENT '包的版本号（数字版本号）1', 
  `sdk_ver` String COMMENT '广告sdk的版本号', 
  `lid` String COMMENT '三方广告版位id', 
  `sid` String COMMENT '业务侧调用时会有一个id 业务侧只要促发adx sdk请求广告时，会生成一个id', 
  `rid` String COMMENT '请求id adx sdk 请求后端server时都会生成一个id', 
  `pid` String COMMENT 'placement id 广告位id', 
  `p_type` String COMMENT '广告位类型', 
  `cst` UInt32 COMMENT '客户端时间戳', 
  `target_id` String COMMENT '请求dsp的targeting id', 
  `dsp_group_id` String COMMENT 'dsp分组id', 
  `country` String COMMENT '用户所在的国家', 
  `crid` String COMMENT '素材id', 
  `project_id` String COMMENT 'GeoEdge返回的项目ID', 
  `project_name` String COMMENT 'GeoEdge返回的项目名', 
  `timestamp` UInt32 COMMENT '时间戳 13位时间戳，接收到ping上报的时候赋值', 
  `ab` String COMMENT 'ab信息', 
  `ext` String COMMENT '扩展字段', 
  `adm` String COMMENT 'rtb的ADM', 
  `scan_timestamp` UInt32 COMMENT '扫描时间戳', 
  `creative_screenshot_url` String COMMENT '素材URL', 
  `landing_page_screenshot_url` String COMMENT '落地页URL', 
  `creative_url` String COMMENT '原素材URL', 
  `landing_page_url` String COMMENT '原落地页URL', 
  `resp` String COMMENT '响应Json', 
  `scan_ext` String COMMENT 'scan扩展字段', 
  `alert_count` UInt32 COMMENT '累计失败次数', 
  `imp_total_count` UInt32 COMMENT '累计展示次数', 
  `click_total_count` UInt32 COMMENT '累计点击次数',
  `bundle_id` String COMMENT '包名 接入广告应用方包名', 
  `os` String COMMENT '操作系统iOS GP', 
  `dt` String, 
  `hour` String
)ENGINE = Distributed('CKClusterNo2', 'adx_prod', 'ads_adx_server_event_data_geoedge_scan_hi_replica', rand())
'''
