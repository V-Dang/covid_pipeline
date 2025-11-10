from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Param, get_current_context

from datetime import datetime, timedelta

from src.api_to_s3 import api_to_s3
from src.s3_to_postgres import s3_to_postgres
from src.secrets import s3_bucket_name, aws_conn_id, postgres_conn_id

covid_extract = api_to_s3(
    bucket_name=s3_bucket_name, 
    aws_conn_id=aws_conn_id
)
covid_load = s3_to_postgres(
    bucket_name=s3_bucket_name, 
    aws_conn_id=aws_conn_id,
    postgres_conn_id=postgres_conn_id
)

default_args = {
    'owner':'vividang',
    'retries':0,
    'retry_interval':timedelta(minutes=1)
}

with DAG(
    dag_id='covid_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 10, 26, 0, 0),
    schedule='0 0 * * *',
    params={
    'Country': Param(
        'Canada',
        type='string',
        enum=['Canada', 'US', 'China']),
        'Start Date': Param(
            None,
            type=['null', 'string'],
            format='date-time',
            description='Optional datetime parameter to specify date-specific data loads.'
        ),
        'End Date': Param(
            None,
            type=['null', 'string'],
            format='date-time',
            description='Optional datetime parameter to specify date-specific data loads.'
        )
    }
):
    api_status = PythonOperator(
        task_id = 'api_status',
        python_callable=covid_extract.check_api_status
    )
    full_or_incremental_load = BranchPythonOperator(
        task_id = 'full_or_incremental_load',
        python_callable=covid_extract.is_bucket_empty,
        op_kwargs={'logical_date': '{{ logical_date }}'}
    )
    full_load_ts = PythonOperator(
        task_id = 'full_load_ts',
        python_callable=covid_extract.get_full_load_ts

    )
    incremental_load_ts = PythonOperator(
        task_id = 'incremental_load_ts',
        python_callable=covid_extract.get_incremental_load_ts
    )
    write_to_bucket = PythonOperator(
        task_id = 'write_to_bucket',
        python_callable=covid_extract.write_to_s3,
        trigger_rule='none_failed_min_one_success'
        # provide_context=True
    )
    # get_latest_postgres_row = SQLExecuteQueryOperator(
    #     task_id = 'get_latest_postgres_row',
    #     conn_id='postgres_db',
    #     sql="""
    #     SELECT MAX(date) FROM covid_test
    #     """
    # )
    # insert_into_postgres_table = PythonOperator(
    #     task_id = 'insert_into_postgres_table',
    #     python_callable=
    # )
    create_postgres_table = SQLExecuteQueryOperator(
        task_id='create_postgres_table',
        conn_id=postgres_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS covid_test (
                date DATE,
                confirmed INT,
                deaths INT,
                recovered INT,
                confirmed_diff INT,
                deaths_diff INT,
                recovered_diff INT,
                last_update TIMESTAMP,
                active INT, 
                active_diff INT, 
                fatality_rate INT, 
                region VARCHAR(255)
            )
        """    
    )
    full_load_into_postgres_table = PythonOperator(
        task_id='full_load_into_postgres_table',
        python_callable=covid_load.full_load_into_postgres_table
    )
    api_status >> full_or_incremental_load >> [full_load_ts, incremental_load_ts] 
    full_load_ts >> write_to_bucket >> full_load_into_postgres_table
    incremental_load_ts >> write_to_bucket 
    # >> insert_into_postgres_table