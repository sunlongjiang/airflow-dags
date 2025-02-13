import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 创建 Spark 会话
    date_str = sys.argv[1]
    hour_str = sys.argv[2]
    min_str = sys.argv[3]

    app_name = f"dwd_adx_server_event_data_request_hi_{date_str}_{hour_str}_{min_str}"
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    print(f"app_name = {app_name}")
    # 声明表名
    table = 'hungry_studio.dwd_adx_server_event_data_request_hi'
    # 定义分区字段的值
    bundle_ids = ['com.block.juggle', 'com.blockpuzzle.us.ios', 'com.mathbrain.sudoku']
    os_values = ['android', 'ios']
    # hours = [f'{i:02d}' for i in range(24)]  # 从 '00' 到 '23'
    group_ids = range(1, 11)  # 从 1 到 10

    # 为每个表的每个分区生成并执行SQL命令
    for bundle_id in bundle_ids:
        for os in os_values:
            for group_id in group_ids:
                partition = f"(bundle_id='{bundle_id}', os='{os}', dt='{date_str}', hour='{hour_str}', group_id='{group_id}')"
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
