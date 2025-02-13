import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 创建 Spark 会话
    date_str = sys.argv[1]
    hour_str = sys.argv[2]
    min_str = sys.argv[3]

    app_name = f"dwd_adx_server_event_data_alert_hi_{date_str}_{hour_str}_{min_str}"
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    print(f"app_name = {app_name}")
    # 声明表名
    table = 'hungry_studio.dwd_adx_server_event_data_alert_hi'
    # 定义分区字段的值
    partition = f"(dt='{date_str}', hour='{hour_str}')"
    # 尝试删除分区（如果存在，不会删除数据）
    drop_partition_sql = f"ALTER TABLE {table} DROP IF EXISTS PARTITION {partition};"
    # 添加分区
    add_partition_sql = f"ALTER TABLE {table} ADD PARTITION {partition};"
    try:
        print(add_partition_sql)
        spark.sql(add_partition_sql)
    except Exception as e:
        print(f"Failed to execute add partition: {add_partition_sql}, Error: {e}")
        print("重新执行修复分区操作")
        spark.sql(drop_partition_sql)
        spark.sql(add_partition_sql)

    # 停止SparkSession
    spark.stop()
