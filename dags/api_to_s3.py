from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime, timedelta

from functions.covid_api import check_api_status, is_bucket_empty, get_full_load_ts, get_incremental_load_ts, api_to_s3, get_list_of_dates

default_args = {
    'owner':'vividang',
    'retries':0,
    'retry_interval':timedelta(minutes=1)
}

with DAG(
    dag_id='covid_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 10, 26, 0, 0),
    schedule='0 0 * * *'
):
    api_status = PythonOperator(
        task_id = 'api_status',
        python_callable=check_api_status
    )
    full_or_incremental_load = BranchPythonOperator(
        task_id = 'full_or_incremental_load',
        python_callable=is_bucket_empty,
        op_kwargs={'logical_date': '{{ logical_date }}'}
    )
    full_load_ts = PythonOperator(
        task_id = 'full_load_ts',
        python_callable=get_full_load_ts

    )
    incremental_load_ts = PythonOperator(
        task_id = 'incremental_load_ts',
        python_callable=get_incremental_load_ts
    )
    write_to_bucket = PythonOperator(
        task_id = 'write_to_bucket',
        python_callable=api_to_s3,
        trigger_rule='none_failed_min_one_success'
        # provide_context=True
    )
    api_status >> full_or_incremental_load >> [full_load_ts, incremental_load_ts] >> write_to_bucket