from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from library.log_generator import *
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from library.load_s3 import upload_s3

now = datetime.now()
log_generator = LogGenerator()

default_args = {
    "owner": "grab",
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["tansfil@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="generate_log",
    default_args=default_args,
    schedule_interval="@once",
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
end = DummyOperator(task_id="end", dag=dag)


start >> generate_log >> s3_upload >> end
