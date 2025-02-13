# 表示处理block blast GP 老版国外服务器SDK 小时级文件级去重明细表

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from common.utils import get_dag_id_for_dir_dag, on_failure_callback, create_success_file, simple_print, on_sla_callback
from common.conf import EMR_EC2_JOB_FLOW_ID,EMR_EC2_JOB_FLOW_ID_DEFAULT_2,EMR_EC2_JOB_FLOW_ID_ADX
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from datetime import datetime,timedelta
from functools import partial

import os

DAG_ID = get_dag_id_for_dir_dag(os.path.abspath(__file__))

specified_receivers = ["sunlongjiang@hungrystudio.com"]
DEFAULT_ARGS = {
    'owner': 'sunlongjiang',
    'depends_on_past': False,
    'wait_for_downstream': False,
    # 'email': ['lianghongbin@hungrystudio.com'],  # 暂时无用
    # 'email_on_failure': True,    # 暂时无用
    # 'email_on_retry': False,      # 暂时无用
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 30, 0, 0),
    # 'on_failure_callback': lambda context: on_failure_callback(context, specified_receivers=specified_receivers)
    'on_failure_callback': lambda context: on_failure_callback(context, specified_receivers=specified_receivers, alarm_level=0, alarm_group="AD", is_call_phone=False)
}
sla_miss_partial = partial(on_sla_callback, specified_receivers=specified_receivers, alarm_level=0, alarm_group="AD", is_call_phone=False)

# sla_miss_partial = partial(on_sla_callback, specified_receivers=specified_receivers)

dt = '{{ (execution_date).strftime("%Y-%m-%d") }}'
hour = '{{ (execution_date).strftime("%H") }}'
min = '{{ (execution_date).strftime("%M") }}'

input_path = "s3://hungry-studio-data-ads/ods/data_type=adx/appid=hellad2dc7bf553b88367c/dt={0}/hour={1}".format(dt, hour)
check_path = "ods/data_type=adx/appid=hellad2dc7bf553b88367c/dt={0}/hour={1}/*.json".format(dt, hour)
check_bucket = "hungry-studio-data-ads"
# dest_table = "hungry_studio_dev.dwd_block_blast_gp_event_data_hi_dev"
dest_table = "hungry_studio.dwd_adx_client_event_data_hi"
ahead_reserve_date_range = "15"
after_reserve_date_range = "2"
mid_repartition_num = "40"
final_repartition_num = "5"
SPARK_CLIENT_STEPS = [
    {
        'Name': 'dwd_adx_client_event_data_hi_{0}_{1}_{2}'.format(dt, hour,min),
        'ActionOnFailure': 'CONTINUE', # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--class", "dwd.DWDADXEventHI",
                "--conf", "spark.app.name=dwd_adx_client_event_data_hi_{0}_{1}_{2}".format(dt, hour,min),
                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=10",
                "--conf", "spark.dynamicAllocation.minExecutors=10",
                "--conf", "spark.dynamicAllocation.maxExecutors=60",
                "--conf", "spark.driver.memory=5704M",
                "--conf", "spark.executor.memory=5704M", #2852M
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.default.parallelism=60",
                "--conf", "spark.sql.shuffle.partitions=100",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.driverEnv.JAVA_HOME=/usr/lib/jvm/jre-17",
                "--conf", "spark.executorEnv.JAVA_HOME=/usr/lib/jvm/jre-17",
                "--jars", "s3://hungry-studio-data-warehouse/user/lianghb/input/jar_libs/fastjson-1.2.29.jar,s3://hungry-studio-data-warehouse/user/lianghb/input/jar_libs/awdb-java-2.0.0.jar",
                "s3://hungry-studio-data-warehouse/user/sunlj/adx/jars/hungry-stdio-data-warehouse.jar",
                input_path,
                dest_table,
                ahead_reserve_date_range,
                after_reserve_date_range,
                mid_repartition_num,
                final_repartition_num,
                "overwrite",
                dt,
                hour
            ],
        }
    }
]


SCHEDULE_INTERVAL = "10 * * * *"
# SCHEDULE_INTERVAL = "*/10 * * * *"
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=2,
    dagrun_timeout=timedelta(hours=12),    #DAG运行超时时间
    tags=['emr', 'data_warehouse', 'dwd'],
    sla_miss_callback = sla_miss_partial,
    catchup=True
) as dag:
    begin_dag = EmptyOperator(
        task_id='begin_dag'
    )

    check_s3_adx_client_event_data_path_existence = S3KeySensor(
        task_id='check_s3_adx_client_event_data_path_existence',
        bucket_name=check_bucket,  # S3 桶名称
        bucket_key=check_path,
        # 3 ,7 , 23
        # 你要检查的S3中的路径
        wildcard_match= True,  # 是否使用通配符匹配
        aws_conn_id='aws_default',  # AWS连接ID
        timeout=10 * 60,  # 超时时间（秒）
        poke_interval=60 * 5  # 检查时间间隔（秒）
    )


    step_client_adder = EmrAddStepsOperator(
        task_id='step_client_adder',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_ADX,
        aws_conn_id='aws_default',
        steps=SPARK_CLIENT_STEPS,
        wait_for_completion=True,
        waiter_delay=300,   # 检测间隔时长
        waiter_max_attempts=43200,   # 检测次数
        sla=timedelta(seconds=5400)
    )

    success_path_bucket_name = "hungry-studio-data-warehouse"
    success_path_key = "dwd/adx/success/dwd_adx_client_event_data_hi"
    success_path_date = dt
    success_path_hour = hour
    create_success_flag = PythonOperator(
        task_id='create_success_flag',
        provide_context=True,
        python_callable=create_success_file,
        op_kwargs={'bucket_name': success_path_bucket_name,
                   'key': '{0}/dt={1}/hour={2}/_SUCCESS'.format(success_path_key, success_path_date,
                                                                           success_path_hour)}
    )

    begin_dag >> check_s3_adx_client_event_data_path_existence >> step_client_adder >> create_success_flag