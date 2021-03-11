from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from library.log_generator import *
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook

now = datetime.now()
log_generator = LogGenerator()
SPARK_PATH = "/usr/local/spark"
POSTGRES_DRIVER_PATH = f"{SPARK_PATH}/resources/drivers/postgresql-42.2.19.jar"
POSTGRES_DB_URL = "jdbc:postgresql://postgres/service"
POSTGRES_USER = "test"
POSTGRES_PW = "test"


def upload_s3(**context):
    hook = S3Hook(aws_conn_id="aws_s3_default")
    csv_arr = context["ti"].xcom_pull(task_ids=["generate_log"], key="return_value")[0]
    for key,path in csv_arr.items():
        hook.load_file(filename=path, key=key, bucket_name="grab-world", replace=True)
    print("UPLOAD SUCCESS")

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
    dag_id="etl_sample",
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False
)

start = DummyOperator(task_id="start", dag=dag)

generate_log = PythonOperator(
    task_id="generate_log",
    python_callable=log_generator.execute,
    dag=dag
)

s3_upload = PythonOperator(
    task_id="s3_upload",
    python_callable=upload_s3,
    provide_context=True,
    dag=dag
)

spark_transform = BashOperator(
    task_id="spark_transform",
    bash_command=f"spark-submit\
     --master local\
     --driver-class-path {POSTGRES_DRIVER_PATH} \
     {SPARK_PATH}/app/upload_rdb.py \
      {POSTGRES_DB_URL} {POSTGRES_USER} {POSTGRES_PW}",
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)


start >> generate_log >> s3_upload >> spark_transform >> end
