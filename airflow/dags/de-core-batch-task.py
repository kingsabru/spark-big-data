from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 12, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30)
}

BASE_DIR = os.path.dirname(__file__)

SPARK_MASTER = "spark://LAP-336.pegghana.local:7077"
SPARK_APP_NAME = "Spark Big Data DE Core Batch"

with DAG(
        dag_id="de-core-batch-task",
        description="This DAG batch loads data using Spark submit.",
        default_args=default_args,
        schedule_interval=timedelta(minutes=2)
) as dag:
    start = DummyOperator(task_id="start")

    batch_task_file_path = os.path.join(BASE_DIR, '../../spark/app/de-core-batch-postgres.py')

    spark_submit_task = SparkSubmitOperator(
        task_id="spark_submit_task",
        conn_id="spark_default",
        application=batch_task_file_path,
        name=SPARK_APP_NAME,
        total_executor_cores=2,
        executor_cores=1,
        executor_memory="2g",
        driver_memory="2g",
        num_executors=2
        )

    end = DummyOperator(task_id="end")

    start >> spark_submit_task >> end