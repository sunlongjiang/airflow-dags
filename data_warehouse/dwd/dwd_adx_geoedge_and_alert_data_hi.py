# 表示处理block blast GP 老版国外服务器SDK 小时级文件级去重明细表

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from common.utils import get_dag_id_for_dir_dag, on_failure_callback, create_success_file, simple_print, on_sla_callback
from common.conf import EMR_EC2_JOB_FLOW_ID, EMR_EC2_JOB_FLOW_ID_DEFAULT_2, EMR_EC2_JOB_FLOW_ID_ADX
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from datetime import datetime, timedelta
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
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 8, 3, 0),
    # 'on_failure_callback': lambda context: on_failure_callback(context, specified_receivers=specified_receivers)
    'on_failure_callback': lambda context: on_failure_callback(context, specified_receivers=specified_receivers,
                                                               alarm_level=0, alarm_group="AD", is_call_phone=False)
}
sla_miss_partial = partial(on_sla_callback, specified_receivers=specified_receivers, alarm_level=0, alarm_group="AD",
                           is_call_phone=False)

dt = '{{ (execution_date).strftime("%Y-%m-%d") }}'
hour = '{{ (execution_date).strftime("%H") }}'
min = '{{ (execution_date).strftime("%M") }}'

SPARK_DWD_SERVER_GEOEDGE_STEPS = [
    {
        'Name': 'dwd_adx_server_event_data_geoedge_hi_{0}_{1}_{2}'.format(dt, hour, min),
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.app.name=dwd_adx_server_event_data_geoedge_hi_{0}_{1}_{2}".format(dt, hour, min),
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.driver.memory=2852M",
                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=10",
                "--conf", "spark.dynamicAllocation.minExecutors=10",
                "--conf", "spark.dynamicAllocation.maxExecutors=10",
                "--conf", "spark.executor.memory=2852M",  # 2852M
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.default.parallelism=60",
                "--conf", "spark.sql.shuffle.partitions=100",
                "s3://hungry-studio-airflow/etl/data_warehouse/dwd/dwd_adx_server_event_data_geoedge_hi.py",
                dt,
                hour,
                min
            ],
        }
    }
]

SPARK_DWD_SERVER_SCAN_STEPS = [
    {
        'Name': 'dwd_adx_server_event_data_scan_hi_{0}_{1}_{2}'.format(dt, hour, min),
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.app.name=dwd_adx_server_event_data_scan_hi_{0}_{1}_{2}".format(dt, hour, min),
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.driver.memory=2852M",
                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=10",
                "--conf", "spark.dynamicAllocation.minExecutors=10",
                "--conf", "spark.dynamicAllocation.maxExecutors=10",
                "--conf", "spark.executor.memory=2852M",  # 2852M
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.default.parallelism=60",
                "--conf", "spark.sql.shuffle.partitions=100",
                "s3://hungry-studio-airflow/etl/data_warehouse/dwd/dwd_adx_server_event_data_scan_hi.py",
                dt,
                hour,
                min
            ],
        }
    }
]

SPARK_DWD_SERVER_ALERT_STEPS = [
    {
        'Name': 'dwd_adx_server_event_data_alert_hi_{0}_{1}_{2}'.format(dt, hour, min),
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.app.name=dwd_adx_server_event_data_alert_hi_{0}_{1}_{2}".format(dt, hour, min),
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.driver.memory=2852M",
                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=10",
                "--conf", "spark.dynamicAllocation.minExecutors=10",
                "--conf", "spark.dynamicAllocation.maxExecutors=10",
                "--conf", "spark.executor.memory=2852M",  # 2852M
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.default.parallelism=60",
                "--conf", "spark.sql.shuffle.partitions=100",
                "s3://hungry-studio-airflow/etl/data_warehouse/dwd/dwd_adx_server_event_data_alert_hi.py",
                dt,
                hour,
                min
            ],
        }
    }
]

SPARK_DWS_SERVER_GEOEDGE_SCAN_STEPS = [
    {
        'Name': 'dws_adx_server_event_data_geoedge_and_scan_hi_{0}_{1}_{2}'.format(dt, hour, min),
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.app.name=dws_adx_server_event_data_geoedge_and_scan_hi_{0}_{1}_{2}".format(dt, hour, min),
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.driver.memory=2852M",
                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=10",
                "--conf", "spark.dynamicAllocation.minExecutors=10",
                "--conf", "spark.dynamicAllocation.maxExecutors=10",
                "--conf", "spark.executor.memory=2852M",  # 2852M
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.default.parallelism=60",
                "--conf", "spark.sql.shuffle.partitions=100",
                "s3://hungry-studio-airflow/etl/data_warehouse/dws/dws_adx_server_event_data_geoedge_scan_hi.py",
                dt,
                hour,
                min
            ],
        }
    }
]

src_table = 'hungry_studio.dws_adx_server_event_data_geoedge_scan_hi'
ck_table_replica = 'adx_prod.ads_adx_server_event_data_geoedge_scan_hi_replica'
ck_table_dist = 'adx_prod.ads_adx_server_event_data_geoedge_scan_hi_dist'
SPARK_ADS_SERVER_GEOEDGE_SCAN_STEPS = [
    {
        'Name': 'ads_adx_server_event_data_geoedge_scan_hi_{0}_{1}_{2}'.format(dt, hour,min),
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--master", "yarn",
                "--conf", "spark.app.name=ads_adx_server_event_data_geoedge_scan_hi_{0}_{1}_{2}".format(dt, hour,min),
                "--conf", "spark.driver.memory=2G",
                "--conf", "spark.yarn.maxAppAttempts=1",
                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=15",
                "--conf", "spark.dynamicAllocation.minExecutors=30",
                "--conf", "spark.dynamicAllocation.maxExecutors=50",
                "--conf", "spark.executor.memory=6G",
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.default.parallelism=200",
                "--conf", "spark.sql.shuffle.partitions=200",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "s3://hungry-studio-airflow/etl/data_warehouse/ads/ads_adx_server_event_data_geoedge_scan_to_ck_hi.py",
                dt,
                hour,
                src_table,
                ck_table_replica,
                ck_table_dist
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
        max_active_runs=5,
        dagrun_timeout=timedelta(hours=12),  # DAG运行超时时间
        tags=['emr', 'data_warehouse', 'dwd'],
        sla_miss_callback=sla_miss_partial,
        catchup=True
) as dag:
    begin_dag = EmptyOperator(
        task_id='begin_dag'
    )

    dwd_adx_server_event_data_geoedge_hi = EmrAddStepsOperator(
        task_id='dwd_adx_server_event_data_geoedge_hi',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_ADX,
        aws_conn_id='aws_default',
        steps=SPARK_DWD_SERVER_GEOEDGE_STEPS,
        wait_for_completion=True,
        waiter_delay=300,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
        sla=timedelta(seconds=5400)
    )

    dwd_adx_server_event_data_scan_hi = EmrAddStepsOperator(
        task_id='dwd_adx_server_event_data_scan_hi',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_ADX,
        aws_conn_id='aws_default',
        steps=SPARK_DWD_SERVER_SCAN_STEPS,
        wait_for_completion=True,
        waiter_delay=300,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
        sla=timedelta(seconds=5400)
    )

    dwd_adx_server_event_data_alert_hi = EmrAddStepsOperator(
        task_id='dwd_adx_server_event_data_alert_hi',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_ADX,
        aws_conn_id='aws_default',
        steps=SPARK_DWD_SERVER_ALERT_STEPS,
        wait_for_completion=True,
        waiter_delay=300,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
        sla=timedelta(seconds=5400)
    )

    check_s3_dws_adx_server_event_data_hi_path = S3KeySensor(
        task_id='check_s3_dws_adx_server_event_data_hi_path',
        bucket_name='hungry-studio-data-warehouse',  # S3 桶名称
        bucket_key=f'dws/adx/success/dws_adx_server_event_data_hi/dt={dt}/hour={hour}/_SUCCESS',
        # 3 ,7 , 23
        # 你要检查的S3中的路径
        wildcard_match=False,  # 是否使用通配符匹配
        aws_conn_id='aws_default',  # AWS连接ID
        timeout=6 * 60 * 60,  # 超时时间（秒）
        poke_interval=60 * 5  # 检查时间间隔（秒）
    )


    dws_adx_server_event_data_geoedge_and_scan_hi = EmrAddStepsOperator(
        task_id='dws_adx_server_event_data_geoedge_and_scan_hi',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_ADX,
        aws_conn_id='aws_default',
        steps=SPARK_DWS_SERVER_GEOEDGE_SCAN_STEPS,
        wait_for_completion=True,
        waiter_delay=300,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
        sla=timedelta(seconds=5400)
    )

    ads_adx_server_event_data_geoedge_and_scan_hi = EmrAddStepsOperator(
        task_id='ads_adx_server_event_data_geoedge_and_scan_hi',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_ADX,
        aws_conn_id='aws_default',
        steps=SPARK_ADS_SERVER_GEOEDGE_SCAN_STEPS,
        wait_for_completion=True,
        waiter_delay=300,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
        sla=timedelta(seconds=5400)
    )



    create_success_geoedge_flag = PythonOperator(
        task_id='create_success_geoedge_flag',
        provide_context=True,
        python_callable=create_success_file,
        op_kwargs={'bucket_name': "hungry-studio-data-warehouse",
                   'key': '{0}/dt={1}/hour={2}/_SUCCESS'.format("dwd/adx/success/dwd_adx_server_event_data_geoedge_hi",
                                                                dt,
                                                                hour)}
    )

    create_success_scan_flag = PythonOperator(
        task_id='create_success_scan_flag',
        provide_context=True,
        python_callable=create_success_file,
        op_kwargs={'bucket_name': "hungry-studio-data-warehouse",
                   'key': '{0}/dt={1}/hour={2}/_SUCCESS'.format("dwd/adx/success/dwd_adx_server_event_data_scan_hi",
                                                                dt,
                                                                hour)}
    )

    create_success_alert_flag = PythonOperator(
        task_id='create_success_alert_flag',
        provide_context=True,
        python_callable=create_success_file,
        op_kwargs={'bucket_name': "hungry-studio-data-warehouse",
                   'key': '{0}/dt={1}/hour={2}/_SUCCESS'.format("dwd/adx/success/dwd_adx_server_event_data_alert_hi",
                                                                dt,
                                                                hour)}
    )

    create_success_geoedge_scan_flag = PythonOperator(
        task_id='create_success_geoedge_scan_flag',
        provide_context=True,
        python_callable=create_success_file,
        op_kwargs={'bucket_name': "hungry-studio-data-warehouse",
                   'key': '{0}/dt={1}/hour={2}/_SUCCESS'.format("dwd/adx/success/dws_adx_server_event_data_geoedge_scan_hi",
                                                                dt,
                                                                hour)}
    )

    begin_dag >> [dwd_adx_server_event_data_geoedge_hi >> create_success_geoedge_flag,
                  dwd_adx_server_event_data_scan_hi >> create_success_scan_flag,
                  dwd_adx_server_event_data_alert_hi >> create_success_alert_flag,
                  check_s3_dws_adx_server_event_data_hi_path] >> dws_adx_server_event_data_geoedge_and_scan_hi >> create_success_geoedge_scan_flag >> ads_adx_server_event_data_geoedge_and_scan_hi
