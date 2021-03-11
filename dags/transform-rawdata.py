from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
now = datetime.now()

#SPARK_MASTER = "spark://spark:7077"
SPARK_PATH = "/usr/local/spark"
POSTGRES_DRIVER_PATH = f"{SPARK_PATH}/resources/drivers/postgresql-42.2.19.jar"
POSTGRES_DB_URL = "jdbc:postgresql://postgres/service"
POSTGRES_USER = "test"
POSTGRES_PW = "test"

default_args = {
    "owner": "grab-data-world",
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["tansfil@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark-app-test",
        description="This DAG runs a simple Pyspark app.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = BashOperator(
    task_id="transform_rawdata",
    bash_command=f"spark-submit\
     --master local\
     --driver-class-path {POSTGRES_DRIVER_PATH} \
     {SPARK_PATH}/app/upload_rdb.py \
      {POSTGRES_DB_URL} {POSTGRES_USER} {POSTGRES_PW}",
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end