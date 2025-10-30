from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta

from functions.covid_api import check_api_status, is_bucket_empty, full_load_ts, incremental_load_ts, api_to_s3

default_args = {
    'owner':'vividang',
    'retries':1,
    'retry_interval':timedelta(minutes=1)
}

with DAG(
    dag_id='covid_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 10, 26, 0, 0),
    schedule='0 0 * * *'
):
    task1 = PythonOperator(
        task_id = 'api_status',
        python_callable=check_api_status
    )
    task2 = BranchPythonOperator(
        task_id = 'full_or_incremental_load',
        python_callable=is_bucket_empty,
        op_kwargs={'logical_date': '{{ logical_date }}'}
    )
    task3 = PythonOperator(
        task_id = 'full_load_ts',
        python_callable=full_load_ts

    )
    task4 = PythonOperator(
        task_id = 'incremental_load_ts',
        python_callable=incremental_load_ts
    )
    task5 = PythonOperator(
        task_id = 'write_to_bucket',
        python_callable=api_to_s3,
        trigger_rule='none_failed_min_one_success'
    )
    task1 >> task2 >> [task3, task4] >> task5