from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_kafka_pipeline',
    default_args=default_args,
    description='Обработка данных из Kafka с помощью Spark',
    schedule_interval=timedelta(days=1),
)

check_hdfs_input = HdfsSensor(
    task_id='check_hdfs_input',
    filepath='/data/input/raw_data.csv',
    hdfs_conn_id='hdfs_default',
    dag=dag,
)

load_from_kafka = BashOperator(
    task_id='load_from_kafka',
    bash_command='python /opt/airflow/scripts/kafka_loader.py',
    dag=dag,
)

process_with_spark = SparkSubmitOperator(
    task_id='process_with_spark',
    application='/opt/airflow/scripts/spark_processor.py',
    conn_id='spark_default',
    verbose=False,
    dag=dag,
)

check_hdfs_output = HdfsSensor(
    task_id='check_hdfs_output',
    filepath='/data/output/processed_data.parquet',
    hdfs_conn_id='hdfs_default',
    dag=dag,
)

# send_notification = BashOperator(
#     task_id='send_notification',
#     bash_command='echo "Pipeline completed successfully" | mail -s "Airflow Notification" admin@example.com',
#     dag=dag,
# )

check_hdfs_input >> load_from_kafka >> process_with_spark >> check_hdfs_output >> send_notification
