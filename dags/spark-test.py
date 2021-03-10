from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Spark Hello World"
postgres_driver_path = "/usr/local/spark/resources/drivers/postgresql-42.2.19.jar"
###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "docker-airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["docker-airflow@docker-airflow.com"],
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

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/spark/app/upload_rdb.py", # Spark application path created in docker-airflow and spark-app cluster
    name=spark_app_name,
    conn_id="spark_default",
    conf={"spark.master": 'local'},
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end