from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

SPARK_PATH = "/usr/local/spark"
SPARK_MASTER = "spark://spark:7070"
POSTGRES_DRIVER_PATH = f"{SPARK_PATH}/resources/drivers/postgresql-42.2.19.jar"
POSTGRES_DB_URL = "jdbc:postgresql://postgres/service"
POSTGRES_USER = "test"
POSTGRES_PW = "test"

now = datetime.now()

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
    dag_id="transform_load_csv",
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False
)

start = DummyOperator(task_id="start", dag=dag)

#별도의 Spark Cluster Container로 job을 넘김
spark_job_load_postgres = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    application="/usr/local/spark/app/upload_rdb.py", # Spark application path created in airflow and spark cluster
    name="load-postgres",
    conn_id="spark_default",
    conf={"spark.master":SPARK_MASTER},
    application_args=[POSTGRES_DB_URL,POSTGRES_USER,POSTGRES_PW],
    jars=POSTGRES_DRIVER_PATH,
    driver_class_path=POSTGRES_DRIVER_PATH,
    dag=dag)


#SparkSubmitOperator는 Local mode가 제대로 실행되지 않는 이슈가 있어 BashOperator 사용
spark_transform_load_local = BashOperator(
    task_id="spark_transform",
    bash_command=f"spark-submit\
     --master local\
     --driver-class-path {POSTGRES_DRIVER_PATH} \
     {SPARK_PATH}/app/upload_rdb.py \
      {POSTGRES_DB_URL} {POSTGRES_USER} {POSTGRES_PW}",
    dag=dag)


end = DummyOperator(task_id="end", dag=dag)

#Spark Master로 작업하는 DAG
# start >> spark_transform_load_master >> end

#Spark Local로 작업하는 DAG
start >> spark_transform_load_local >> end