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
    randend = len(ck_config[ck_cluster])-1
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
                    COALESCE(pid,"-") as pid,
                    COALESCE(p_type,"-") as p_type,
                    COALESCE(dsp_id,"-") as dsp_id,
                    COALESCE(dsp_name,"-") as dsp_name,
                    COALESCE(dsp_g_name,"-") as dsp_g_name,
                    COALESCE(tagid,"-") as tagid,
                    COALESCE(country,"-") as country,
                    COALESCE(os,"-") as os,
                    COALESCE(bundle_id,"-") as bundle_id,
                    COALESCE(ver_n,"-") as ver_n,
                    COALESCE(ad_type,"-") as ad_type,
                    COALESCE(adomain,"-") as adomain,
                    COALESCE(devicetype,"-") as devicetype,
                    COALESCE(request_pv,0) AS request_pv, 
                    COALESCE(fill_pv,0) AS fill_pv, 
                    COALESCE(wim_pv,0) AS wim_pv, 
                    COALESCE(imp_pv,0) AS imp_pv, 
                    COALESCE(click_pv,0) AS click_pv, 
                    COALESCE(revenue,0.000000) AS revenue,
                    dt,
                    hour,
                    COALESCE(crid,"-") as crid 
               FROM {src_table}
               WHERE dt = '{dt}' AND hour = '{hour}'
               """
    sync_df = spark.sql(sync_sql)

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
CREATE TABLE adx_prod.ads_adx_server_event_data_ck_hi_replica on CLUSTER CKClusterNo2(
    pid String COMMENT 'placement id 广告位id', 
    p_type String COMMENT '广告位类型', 
    dsp_id String COMMENT 'dsp id', 
    dsp_name String COMMENT 'dsp的名称', 
    dsp_g_name String COMMENT 'dsp分组', 
    tagid String COMMENT '请求dsp的tagid', 
    country String COMMENT '用户所在的国家', 
    os String COMMENT '操作系统iOS GP', 
    bundle_id String COMMENT '包名 接入广告应用方包名', 
    ver_n String COMMENT '包的版本号（数字版本号）1', 
    ad_type String COMMENT 'dsp广告类型', 
    adomain String COMMENT '广告主域名',
    devicetype String COMMENT '设备类型', 
    request_pv UInt32 COMMENT '请求次数', 
    fill_pv UInt32 COMMENT '填充次数', 
    wim_pv UInt32 COMMENT '竞价成功次数', 
    imp_pv UInt32 COMMENT '展示次数', 
    click_pv UInt32 COMMENT '点击次数',
    revenue Decimal(18,6) COMMENT '总收入',
    dt String COMMENT '日期',  -- Add the dt column
    hour UInt32 COMMENT '小时',  -- Add the hour column
   crid String COMMENT 's素材id'  -- Add the hour column
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/adx_prod/ads_adx_server_event_data_ck_hi', '{replica}')
PARTITION BY (dt,hour)
ORDER BY (pid,p_type,dsp_id,dsp_name,dsp_g_name,tagid,country,os,bundle_id,ver_n,ad_type,adomain,devicetype)
SETTINGS index_granularity = 8192;


CREATE TABLE adx_prod.ads_adx_server_event_data_ck_hi_dist on CLUSTER CKClusterNo2(
    pid String COMMENT 'placement id 广告位id', 
    p_type String COMMENT '广告位类型', 
    dsp_id String COMMENT 'dsp id', 
    dsp_name String COMMENT 'dsp的名称', 
    dsp_g_name String COMMENT 'dsp分组', 
    tagid String COMMENT '请求dsp的tagid', 
    country String COMMENT '用户所在的国家', 
    os String COMMENT '操作系统iOS GP', 
    bundle_id String COMMENT '包名 接入广告应用方包名', 
    ver_n String COMMENT '包的版本号（数字版本号）1', 
    ad_type String COMMENT 'dsp广告类型', 
    adomain String COMMENT '广告主域名',
    devicetype String COMMENT '设备类型', 
    request_pv UInt32 COMMENT '请求次数', 
    fill_pv UInt32 COMMENT '填充次数', 
    wim_pv UInt32 COMMENT '竞价成功次数', 
    imp_pv UInt32 COMMENT '展示次数', 
    click_pv UInt32 COMMENT '点击次数',
    revenue Decimal(18,6) COMMENT '总收入',
    dt String COMMENT '日期',  -- Add the dt column
    hour UInt32 COMMENT '小时',  -- Add the hour column
    crid String COMMENT 's素材id'  -- Add the hour column
)ENGINE = Distributed('CKClusterNo2', 'adx_prod', 'ads_adx_server_event_data_ck_hi_replica', rand())
'''
