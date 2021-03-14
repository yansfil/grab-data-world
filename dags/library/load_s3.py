from airflow.hooks.S3_hook import S3Hook

def upload_s3(**context):
    hook = S3Hook(aws_conn_id="aws_s3_default")
    csv_arr = context["ti"].xcom_pull(task_ids=["generate_log"], key="return_value")[0]
    for key,path in csv_arr.items():
        hook.load_file(filename=path, key=key, bucket_name="grab-world", replace=True)
    print("UPLOAD SUCCESS")
