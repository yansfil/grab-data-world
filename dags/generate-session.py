from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from libraries.log_generator import *
from airflow.hooks.S3_hook import S3Hook
now = datetime.now()
log_generator = LogGenerator()

def upload_s3():
    # content = "String content to write to a new S3 file"
    # v= s3.Object('grab-world', 'newfile.txt').put(Body=content)
    hook = S3Hook()
    hook.get_key("channels.csv",bucket_name="grab-world")
    # hook.check_for_bucket("grab-world")
    # hook.load_file(filename="libraries/generated/channels.csv", key="test.txt", bucket_name="grab-world", replace=True)
    print("SUCCESS")

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
    dag_id="generate_logs",
    default_args=default_args,
    schedule_interval=timedelta(1),
    catchup=False
)


generate_log = PythonOperator(
    task_id="generate_log",
    python_callable=log_generator.execute,
    dag=dag
)

s3_upload = PythonOperator(
    task_id="s3_upload",
    python_callable=upload_s3,
    dag=dag
)

generate_log >> s3_upload
