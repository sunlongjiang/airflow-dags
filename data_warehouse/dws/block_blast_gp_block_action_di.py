# ROI - 收入聚合数据
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator,EmrTerminateJobFlowOperator
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from common.utils import get_dag_id_for_dir_dag, on_failure_callback, create_success_file,simple_print
from common.conf import EMR_EC2_JOB_FLOW_ID_DEFAULT,EMR_EC2_JOB_FLOW_ID_DEFAULT_2
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor

from datetime import datetime, timedelta

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
    'start_date': datetime(2023, 12, 1, 0, 0),
    'on_failure_callback': lambda context: on_failure_callback(context, specified_receivers=specified_receivers)
}
dt= '{{ (execution_date - macros.timedelta(days=0)).strftime("%Y-%m-%d") }}'
src_table = "hungry_studio.dws_block_blast_gp_block_action_di"
# execution_date = '{{ (execution_date - macros.timedelta(days=0)).strftime("%Y-%m-%d") }}'
check_date = '{{ (execution_date).strftime("%Y-%m-%d") }}'
execution_date = '{{ (execution_date - macros.timedelta(days=0)).strftime("%Y-%m-%d") }}'
hour = '{{ (data_interval_end - macros.timedelta(hours=1)).strftime("%H") }}'
success_path_bucket_name = "hungry-studio-data-warehouse"
success_path_key = "dws/block_blast/gp/block_action_di"
success_path_date = execution_date
# hour_flag = "00"
# black_event_names = "game_click_block,game_touchend_block_done,game_move_block,game_get_block_end"
# dest_table = "hungry_studio.dwd_block_blast_gp_white_event_unique_data_hi"
# final_repartition_num = "100"

#
# tags = [
#     {'Key': 'for-use-with-amazon-emr-managed-policies', 'Value': 'true'},
#     {'Key': 'shucang', 'Value': 'block_action_data_etl'}, {'Key': 'map-migrated',
#                                                            'Value': 'migMXNW6DOH0G'}
# ]
#
# JOB_FLOW_OVERRIDES = {
#     'Name': 'data_warehouse_dws_block_blast_gp_block_action_di',# 与airflow 的dag名称保持一致
#     'ReleaseLabel': 'emr-6.15.0',
#     'Applications': [
#         {'Name': 'Spark'}, {'Name': 'Hadoop'}, {'Name': 'Hive'}, {'Name': 'Livy'}
#     ],
#     'StepConcurrencyLevel': 10,  # 并行步骤个数
#     'LogUri': 's3://aws-logs-459528473147-us-east-2/elasticmapreduce/',
#     'AutoTerminationPolicy': {
#         'IdleTimeout': 3600  # 设置空闲超时，超过此时间集群自动终止
#     },
#     'Instances': {
#         'KeepJobFlowAliveWhenNoSteps': True,
#         'InstanceGroups': [
#             {
#                 'Name': "Master nodes",
#                 'Market': 'ON_DEMAND',
#                 'InstanceRole': 'MASTER',
#                 'InstanceType': 'r6g.2xlarge',
#                 'InstanceCount': 1,
#                 'EbsConfiguration': {
#                     'EbsOptimized': True,
#                     'EbsBlockDeviceConfigs': [
#                         {
#                             'VolumeSpecification': {
#                                 'VolumeType': 'gp3',
#                                 'SizeInGB': 300,  # 每卷大小GB
#                                 'Iops': 10000,  # 设置 IOPS，最高为 16,000
#                                 'Throughput': 600  # 设置吞吐量，最高为 1,000 MiB/s
#                             },
#                             'VolumesPerInstance': 4  # 每个实例等卷数
#                         }
#                     ]
#                 }
#
#             },
#             {
#                 'Name': "Slave nodes",
#                 'Market': 'ON_DEMAND',
#                 'InstanceRole': 'CORE',
#                 'InstanceType': 'r6g.2xlarge',
#                 'InstanceCount': 2,
#                 'EbsConfiguration': {
#                     'EbsOptimized': True,
#                     'EbsBlockDeviceConfigs': [
#                         {
#                             'VolumeSpecification': {
#                                 'VolumeType': 'gp3',
#                                 'SizeInGB': 300,  # 每卷大小GB
#                                 'Iops': 10000,  # 设置 IOPS，最高为 16,000
#                                 'Throughput': 600  # 设置吞吐量，最高为 1,000 MiB/s
#                             },
#                             'VolumesPerInstance': 4  # 每个实例等卷数
#                         }
#                     ]
#                 }
#
#             },
#             {
#                 'Name': "Task nodes",
#                 'Market': 'SPOT',
#                 'InstanceRole': 'TASK',
#                 'InstanceType': 'r6g.2xlarge',
#                 'InstanceCount': 105,  # 最大核心数为200
#                 # 'BidPrice': '0.5',  # 设置Spot价格，您可以根据实际情况调整
#                 'EbsConfiguration': {
#                     'EbsOptimized': True,
#                     'EbsBlockDeviceConfigs': [
#                         {
#                             'VolumeSpecification': {
#                                 'VolumeType': 'gp3',
#                                 'SizeInGB': 300,  # 每卷大小GB
#                                 'Iops': 10000,  # 设置 IOPS，最高为 16,000
#                                 'Throughput': 600  # 设置吞吐量，最高为 1,000 MiB/s
#                             },
#                             'VolumesPerInstance': 4  # 每个实例等卷数
#                         }
#                     ]
#                 }
#
#             }
#         ],
#
#         'TerminationProtected': False,
#         'Ec2KeyName': 'cluster_aws',
#         'Ec2SubnetId': 'subnet-0dcad77695a5fdb8a',  # 设置子网
#         'EmrManagedMasterSecurityGroup': 'sg-006207ada91c0a696',  # 主节点安全组
#         'EmrManagedSlaveSecurityGroup': 'sg-0331985778a9e930e'  # 从节点安全组
#
#     },
#     'VisibleToAllUsers': True,
#     'JobFlowRole': 'AmazonEMR-InstanceProfile-20230830T151933',
#     'ServiceRole': 'service-role/AmazonEMR-ServiceRole-20230830T151952',
#     'Tags': tags,
#     # 配置 Glue Data Catalog 作为外部元存储
#     "Configurations": [
#         {
#             "Classification": "hive-site",
#             "Properties": {
#                 "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
#             },
#         },
#         {
#             "Classification": "spark-hive-site",
#             "Properties": {
#                 "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
#             },
#         },
#         {
#             "Classification": "spark-defaults",
#             "Properties": {
#                 "spark.dynamicAllocation.enabled": "false"
#             }
#         },
#         {
#             "Classification": "capacity-scheduler",
#             "Properties": {
#                 "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
#             }
#         },
#         {
#             "Classification": "flink-conf",
#             "Properties": {
#                 "classloader.check-leaked-classloader": "false",
#                 "classloader.resolve-order": "parent-first",
#                 "containerized.master.env.JAVA_HOME": "/usr/lib/jvm/jre-11",
#                 "containerized.taskmanager.env.JAVA_HOME": "/usr/lib/jvm/jre-11",
#                 "env.java.home": "/usr/lib/jvm/jre-11",
#                 "taskmanager.numberOfTaskSlots": "2"
#             }
#         },
#         {
#             "Classification": "yarn-env",
#             "Configurations": [
#                 {
#                     "Classification": "export",
#                     "Properties": {
#                         "AWS_METADATA_SERVICE_NUM_ATTEMPTS": "10",
#                         "AWS_METADATA_SERVICE_TIMEOUT": "40"
#                     }
#                 }
#             ],
#             "Properties": {}
#         },
#         {
#             "Classification": "hdfs-site",
#             "Properties": {
#                 "dfs.replication": "3"
#             }
#         }
#     ]
# }

BLOCK_PRE_0_SPARK_STEPS = [
    {
        'Name': 'block_blast_gp_block_action_block_di_pre_0',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                #"--conf", "spark.executor.instances=125",
                "--conf", "spark.driver.memory=9852M",
                "--conf", "spark.executor.memory=9852M",  #约等于每core分配2463M
                "--conf", "spark.executor.cores=4",

                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=250",
                "--conf", "spark.dynamicAllocation.minExecutors=250",
                "--conf", "spark.dynamicAllocation.maxExecutors=250",

                "--conf", "spark.default.parallelism=2000",
                "--conf", "spark.sql.shuffle.partitions=3000",
                "--conf", "spark.network.timeout=600s",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",

                "s3://hungry-studio-airflow/etl/data_warehouse/dwd/block_blast_gp_block_action_block_di_pre_0.py",
                execution_date,
                hour
            ],
        }
    }
]
BLOCK_PRE_1_SPARK_STEPS = [
    {
        'Name': 'dwd_block_blast_gp_block_action_block_di_pre_1',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", f"spark.app.name=dwd_block_blast_gp_block_action_block_di_pre_1_{execution_date}",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.driver.memory=49088M",
                "--conf", "spark.executor.memory=49088M",  # 6136M
                "--conf", "spark.executor.cores=8",

                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=55",
                "--conf", "spark.dynamicAllocation.minExecutors=55",
                "--conf", "spark.dynamicAllocation.maxExecutors=55",

                 "--conf", "spark.default.parallelism=880",
                "--conf", "spark.sql.shuffle.partitions=1320",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",

                "s3://hungry-studio-airflow/etl/data_warehouse/dwd/block_blast_gp_block_action_block_di_pre.py",
                execution_date,
                hour,
                "1",
                "4"
            ],
        }
    }
]

BLOCK_PRE_2_SPARK_STEPS = [
    {
        'Name': 'dwd_block_blast_gp_block_action_block_di_pre_2',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", f"spark.app.name=dwd_block_blast_gp_block_action_block_di_pre_2_{execution_date}",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                #"--conf", "spark.executor.instances=125",
                "--conf", "spark.driver.memory=9852M",
                "--conf", "spark.executor.memory=9852M", # 6136M
                "--conf", "spark.executor.cores=4",

                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=100",
                "--conf", "spark.dynamicAllocation.minExecutors=100",
                "--conf", "spark.dynamicAllocation.maxExecutors=100",

                "--conf", "spark.default.parallelism=800",
                "--conf", "spark.sql.shuffle.partitions=1200",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",

                "s3://hungry-studio-airflow/etl/data_warehouse/dwd/block_blast_gp_block_action_block_di_pre.py",
                execution_date,
                hour,
                "5",
                "7"
            ],
        }
    }
]

BLOCK_PRE_3_SPARK_STEPS = [
    {
        'Name': 'dwd_block_blast_gp_block_action_block_di_pre_3',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", f"spark.app.name=dwd_block_blast_gp_block_action_block_di_pre_3_{execution_date}",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                #"--conf", "spark.executor.instances=125",
                "--conf", "spark.driver.memory=9852M",
                "--conf", "spark.executor.memory=9852M", # 6136M
                "--conf", "spark.executor.cores=4",

                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=100",
                "--conf", "spark.dynamicAllocation.minExecutors=100",
                "--conf", "spark.dynamicAllocation.maxExecutors=100",

                "--conf", "spark.default.parallelism=800",
                "--conf", "spark.sql.shuffle.partitions=1200",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",

                "s3://hungry-studio-airflow/etl/data_warehouse/dwd/block_blast_gp_block_action_block_di_pre.py",
                execution_date,
                hour,
                "8",
                "10"
            ],
        }
    }
]

BLOCK_SPARK_STEPS = [
    {
        'Name': 'dwd_block_blast_gp_block_action_block_di',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", f"spark.app.name=dwd_block_blast_gp_block_action_block_di_{execution_date}",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.executor.instances=250",
                "--conf", "spark.driver.memory=9852M",
                "--conf", "spark.executor.memory=9852M", # 6136M
                "--conf", "spark.executor.cores=4",
                "--conf", "spark.default.parallelism=2000",
                "--conf", "spark.sql.shuffle.partitions=3000",
                "--conf", "spark.network.timeout=600s",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",
                "--jars", "s3://hungry-studio-data-warehouse/user/sunlj/java_udf/adx_platform_common-4.0.0.jar",
                "s3://hungry-studio-airflow/etl/data_warehouse/dwd/block_blast_gp_block_action_block_di.py",
                execution_date,
                hour
            ],
        }
    }
]

GAME_SPARK_STEPS = [
    {
        'Name': 'dwd_block_blast_gp_block_action_game_di',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.driver.memory=7389M",
                "--conf", "spark.executor.memory=7389M",
                "--conf", "spark.executor.cores=3",
                "--conf", "spark.executor.instances=100",
                "--conf", "spark.default.parallelism=600",
                "--conf", "spark.sql.shuffle.partitions=600",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",

                "s3://hungry-studio-airflow/etl/data_warehouse/dwd/block_blast_gp_block_action_game_di.py",
                execution_date
            ],
        }
    }
]

DWS_GAME_SPARK_STEPS = [
    {
        'Name': 'dws_block_blast_gp_block_action_game_di',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", f"spark.app.name=dws_block_blast_gp_block_action_game_di_{execution_date}",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.driver.memory=4926M",
                "--conf", "spark.executor.memory=4926M",# 6136M
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.executor.instances=200",
                "--conf", "spark.default.parallelism=600",
                "--conf", "spark.sql.shuffle.partitions=600",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",

                "s3://hungry-studio-airflow/etl/data_warehouse/dws/block_blast_gp_block_action_game_di.py",
                execution_date
            ],
        }
    }
]


ROUND_PRE_SPARK_STEPS = [
    {
        'Name': 'dws_block_blast_gp_block_action_round_di_pre',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                 "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", f"spark.app.name=block_blast_gp_block_action_round_di_pre_{execution_date}",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                 "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.driver.memory=7389M",
                "--conf", "spark.executor.memory=7389M",
                "--conf", "spark.executor.cores=3",
                "--conf", "spark.executor.instances=200",
                "--conf", "spark.default.parallelism=2000",
                "--conf", "spark.sql.shuffle.partitions=3000",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",
                "--jars", "s3://hungry-studio-data-warehouse/user/sunlj/java_udf/adx_platform_common-4.0.0.jar",
                "s3://hungry-studio-airflow/etl/data_warehouse/dws/block_blast_gp_block_action_round_di_pre.py",
                execution_date
            ],
        }
    }
]

GAME_GET_BLOCK_END_STEPS = [
    {
        'Name': 'block_blast_gp_game_get_block_end_di',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", f"spark.app.name=block_blast_gp_game_get_block_end_di_{execution_date}",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.driver.memory=24544M",
                "--conf", "spark.executor.memory=24544M",  # 6136M
                "--conf", "spark.executor.cores=4",

                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=100",
                "--conf", "spark.dynamicAllocation.minExecutors=100",
                "--conf", "spark.dynamicAllocation.maxExecutors=100",

                "--conf", "spark.default.parallelism=880",
                "--conf", "spark.sql.shuffle.partitions=1320",
                "--conf", "spark.network.timeout=600s",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",
                "s3://hungry-studio-airflow/etl/data_warehouse/dws/block_blast_gp_game_get_block_end_di.py",
                execution_date
            ],
        }
    }
]

ROUND_REVIVE_SPARK_STEPS = [
    {
        'Name': 'dws_block_blast_gp_block_revive_round_di',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf",
                f"spark.app.name=block_blast_gp_block_revive_round_di_{execution_date}",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.dynamicAllocation.enabled=true",
                "--conf", "spark.dynamicAllocation.initialExecutors=10",
                "--conf", "spark.dynamicAllocation.minExecutors=10",
                "--conf", "spark.dynamicAllocation.maxExecutors=20",
                "--conf", "spark.executor.instances=20",
                "--conf", "park.driver.cores=2",
                "--conf", "spark.driver.memory=4926M",
                "--conf", "spark.executor.memory=4926M",
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.default.parallelism=200",
                "--conf", "spark.sql.shuffle.partitions=500",
                "--conf", "spark.network.timeout=600s",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",

                "s3://hungry-studio-airflow/etl/data_warehouse/dws/block_blast_gp_block_revive_round_di.py",
                execution_date
            ],
        }
    }
]


ROUND_SPARK_STEPS = [
    {
        'Name': 'dws_block_blast_gp_block_action_round_di',  # 天级别计算app收入聚合数据
        'ActionOnFailure': 'CONTINUE',
        # 定义了步骤失败时的行为，"TERMINATE_CLUSTER":当步骤失败时，整个EMR集群会终止，"CANCEL_AND_WAIT":失败时取消任何后续步骤并等待，"CONTINUE":即使步骤失败也继续执行后续步骤
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf",
                f"spark.app.name=block_blast_gp_block_action_round_di_{execution_date}",
                "-c", "spark.pyspark.python=./environment/bin/python",
                "--archives", "s3://hungry-studio-airflow/venv_arm/pyspark-airflow-venv.tar.gz#environment",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition=true",
                "--conf", "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict",
                "--conf", "spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
                "--conf", "spark.driver.memory=7389M",
                "--conf", "spark.executor.memory=7389M",
                "--conf", "spark.executor.cores=3",
                "--conf", "spark.executor.instances=100",
                "--conf", "spark.default.parallelism=1600",
                "--conf", "spark.sql.shuffle.partitions=2000",

                "--conf", "spark.shuffle.file.buffer=64k",
                "--conf", "spark.shuffle.spill.batchSize=50000",
                "--conf", "spark.shuffle.io.maxRetries=10",
                "--conf", "spark.shuffle.io.retryWait=20s",
                "--conf", "spark.reducer.maxSizeInFlight=96m",
                "--conf", "spark.sql.parquet.enableVectorizedReader=true",
                # 添加垃圾回收相关的JVM参数
                "--conf", "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps",

                "--jars", "s3://hungry-studio-data-warehouse/user/sunlj/java_udf/adx_platform_common-4.0.0.jar",
                "s3://hungry-studio-airflow/etl/data_warehouse/dws/block_blast_gp_block_action_round_di.py",
                execution_date
            ],
        }
    }
]


SCHEDULE_INTERVAL = "03 5 * * *"
with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        schedule_interval=None,
        max_active_runs=2,
        dagrun_timeout=timedelta(hours=20),  # DAG运行超时时间
        tags=['emr', 'data_warehouse', 'dws'],
        concurrency=10,  # 定义并行执行的最大任务数
        catchup=False
) as dag:
    begin_dag = DummyOperator(
        task_id='begin_dag'
    )
    # cluster_creator = EmrCreateJobFlowOperator(
    #     task_id='create_job_flow',
    #     job_flow_overrides=JOB_FLOW_OVERRIDES,
    #     aws_conn_id="aws_default",
    #     emr_conn_id='emr_default',
    # )
    # cluster_terminator = EmrTerminateJobFlowOperator(
    #     task_id='terminate_job_flow',
    #     job_flow_id=cluster_creator.output,  # 使用创建集群时返回的 job_flow_id
    #     aws_conn_id='aws_default',
    #
    # )
    dwd_block_blast_gp_block_action_block_di_pre_0 = EmrAddStepsOperator(
        task_id='dwd_block_blast_gp_block_action_block_di_pre_0',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT_2,
        aws_conn_id='aws_default',
        steps=BLOCK_PRE_0_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )
    dwd_block_blast_gp_block_action_block_di_pre_1 = EmrAddStepsOperator(
        task_id='dwd_block_blast_gp_block_action_block_di_pre_1',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT,
        aws_conn_id='aws_default',
        steps=BLOCK_PRE_1_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )
    dwd_block_blast_gp_block_action_block_di_pre_2 = EmrAddStepsOperator(
        task_id='dwd_block_blast_gp_block_action_block_di_pre_2',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT_2,
        aws_conn_id='aws_default',
        steps=BLOCK_PRE_2_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )
    dwd_block_blast_gp_block_action_block_di_pre_3 = EmrAddStepsOperator(
        task_id='dwd_block_blast_gp_block_action_block_di_pre_3',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT_2,
        aws_conn_id='aws_default',
        steps=BLOCK_PRE_3_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )

    dwd_block_blast_gp_block_action_block_di = EmrAddStepsOperator(
        task_id='dwd_block_blast_gp_block_action_block_di',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT_2,
        aws_conn_id='aws_default',
        steps=BLOCK_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )


    dws_block_blast_gp_block_action_round_di_pre = EmrAddStepsOperator(
        task_id='dws_block_blast_gp_block_action_round_di_pre',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT_2,
        aws_conn_id='aws_default',
        steps=ROUND_PRE_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )


    dws_block_blast_gp_block_action_round_di_revive = EmrAddStepsOperator(
        task_id='dws_block_blast_gp_block_action_round_di_revive',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT_2,
        aws_conn_id='aws_default',
        steps=ROUND_REVIVE_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )
    check_s3_dim_block_blast_gp_user_label_ha_path = S3KeySensor(
        task_id='check_s3_dim_block_blast_gp_user_label_ha_path',
        bucket_name='hungry-studio-data-warehouse',  # S3 桶名称
        bucket_key=f'dim/block_blast/gp/all_user_label_ha/dt={dt}/hour=23/area=foreign/_SUCCESS',
        # 3 ,7 , 23
        # 你要检查的S3中的路径
        wildcard_match=False,  # 是否使用通配符匹配
        aws_conn_id='aws_default',  # AWS连接ID
        timeout=6 * 60 * 60,  # 超时时间（秒）
        poke_interval=60 * 5  # 检查时间间隔（秒）
    )

    block_blast_gp_game_get_block_end_di = EmrAddStepsOperator(
        task_id='block_blast_gp_game_get_block_end_di',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT,
        aws_conn_id='aws_default',
        steps=GAME_GET_BLOCK_END_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )

    dws_block_blast_gp_block_action_round_di = EmrAddStepsOperator(
        task_id='dws_block_blast_gp_block_action_round_di',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT_2,
        aws_conn_id='aws_default',
        steps=ROUND_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )

    dwd_block_blast_gp_block_action_game_di = EmrAddStepsOperator(
        task_id='dwd_block_blast_gp_block_action_game_di',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT_2,
        aws_conn_id='aws_default',
        steps=GAME_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )


    dws_block_blast_gp_block_action_game_di = EmrAddStepsOperator(
        task_id='dws_block_blast_gp_block_action_game_di',
        job_flow_id=EMR_EC2_JOB_FLOW_ID_DEFAULT_2,
        aws_conn_id='aws_default',
        steps=DWS_GAME_SPARK_STEPS,
        wait_for_completion=True,
        waiter_delay=120,  # 检测间隔时长
        waiter_max_attempts=43200,  # 检测次数
    )


    create_success_flag = PythonOperator(
        task_id='create_success_flag',
        provide_context=True,
        python_callable=create_success_file,
        op_kwargs={
            'bucket_name': "hungry-studio-data-warehouse",
            'key': f'dws/block_blast/gp/block_action_round_di/dt={execution_date}/_SUCCESS'
        }
    )
    create_game_success_flag = PythonOperator(
        task_id='create_game_success_flag',
        provide_context=True,
        python_callable=create_success_file,
        op_kwargs={
            'bucket_name': "hungry-studio-data-warehouse",
            'key': f'dws/block_blast/gp/block_action_game_di/dt={execution_date}/_SUCCESS'
        }
    )
    print_run_date = PythonOperator(
        task_id="print_run_date",
        python_callable=simple_print,
        provide_context=True,
        op_kwargs={'date_info': f"date:{dt}"}
    )
    begin_dag >> dwd_block_blast_gp_block_action_game_di >> dws_block_blast_gp_block_action_game_di >> create_game_success_flag
    begin_dag >> [dwd_block_blast_gp_block_action_block_di_pre_0 >> print_run_date >> [dwd_block_blast_gp_block_action_block_di_pre_1,dwd_block_blast_gp_block_action_block_di_pre_2,dwd_block_blast_gp_block_action_block_di_pre_3] >> dwd_block_blast_gp_block_action_block_di,
                  dws_block_blast_gp_block_action_round_di_revive,
                  check_s3_dim_block_blast_gp_user_label_ha_path,
                  block_blast_gp_game_get_block_end_di] >> dws_block_blast_gp_block_action_round_di_pre >> dws_block_blast_gp_block_action_round_di >> create_success_flag >> dws_block_blast_gp_block_action_game_di
