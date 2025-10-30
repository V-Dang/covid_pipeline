from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from covid_api import check_api_status, get_filename, fetch_covid_data, json_to_s3, api_to_s3

default_args = {
    'owner':'vividang',
    'retries':1,
    'retry_interval':timedelta(minutes=1)
}

with DAG(
    dag_id='api_to_s3',
    default_args=default_args,
    start_date=datetime(2025, 10, 26, 0, 0),
    schedule='0 0 * * *'
):
    task1 = PythonOperator(
        task_id = 'api_status',
        python_callable=check_api_status
    )
    task2 = PythonOperator(
        task_id = 'create_filename',
        python_callable=get_filename,
        # op_kwargs={'ds':'{{ ds }}'}
        op_kwargs={'current_date': '2020-10-27'}        # Hard-code this for now
    )
    # task3 = PythonOperator(
    #     task_id = 'get_covid_data_incremental',
    #     python_callable=fetch_covid_data,
    #     # op_kwargs={'current_date': '{{ ds }}'}
    #     op_kwargs={'current_date': '2020-10-27'}        # Hard-code this for now
    # )
    # task4 = PythonOperator(
    #     task_id = 'write_to_s3',
    #     python_callable=json_to_s3,

    # )
    task3 = PythonOperator(
        task_id = 'write_api_to_s3',
        python_callable = api_to_s3,
        op_kwargs={'current_date': '2020-10-27'}        # Hard-code this for now
    )
    task1 >> task2 >> task3