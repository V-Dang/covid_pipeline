from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Param, get_current_context

from datetime import datetime, timedelta

from dags.src.S3Client.s3_loader import api_to_s3
from dags.src.postgres.postgres_loader import s3_to_postgres
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
        'start_date': Param(
            None,
            type=['null', 'string'],
            format='date-time',
            description='Optional datetime parameter to specify date-specific data loads.'
        ),
        'end_date': Param(
            None,
            type=['null', 'string'],
            format='date-time',
            description='Optional datetime parameter to specify date-specific data loads.'
        )
    }
):
    api_status = PythonOperator(
        task_id = 'api_status',
        python_callable=covid_extract.check_api_status,
        op_kwargs={
            'country': '{{ params.Country }}'
        }
    )
    full_or_incremental_load = BranchPythonOperator(
        task_id = 'full_or_incremental_load',
        python_callable=covid_extract.is_bucket_empty,
        op_kwargs={
            'country': '{{ params.Country }}'
        }
    )
    full_load_ts = PythonOperator(
        task_id = 'full_load_ts',
        python_callable=covid_extract.get_full_load_ts,
        op_kwargs={
            'manual_start_date': '{{ params.start_date }}'
        }
    )
    incremental_load_ts = PythonOperator(
        task_id = 'incremental_load_ts',
        python_callable=covid_extract.get_incremental_load_ts,
        op_kwargs={
            'manual_start_date': '{{ params.start_date }}',
            'country': '{{ params.Country }}'
        }
    )
    write_to_bucket = PythonOperator(
        task_id = 'write_to_bucket',
        python_callable=covid_extract.write_to_s3,
        op_kwargs={
            'country': '{{ params.Country }}',
            'start_date': '{{ ti.xcom_pull(task_ids="full_load_ts", key="return_value") if ti.xcom_pull(task_ids="full_or_incremental_load") == "full_load_ts" else ti.xcom_pull(task_ids="incremental_load_ts", key="return_value") }}',
            'manual_end_date': '{{ params.end_date }}'
        },
        trigger_rule='none_failed_min_one_success',
        # provide_context=True
    )
    create_postgres_table = SQLExecuteQueryOperator(
        task_id='create_postgres_table',
        conn_id=postgres_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS covid_raw (
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
                region VARCHAR(255),
                source_file_index VARCHAR(225), 
                created_ts TIMESTAMP
            )
        """,
        trigger_rule='none_skipped'
    )
    full_load_into_postgres_table = PythonOperator(
        task_id='full_load_into_postgres_table',
        python_callable=covid_load.full_load_into_postgres_table,
        trigger_rule='none_skipped',
        # provide_context=True
    )
    incremental_load_into_postgres_table = PythonOperator(
        task_id='incremental_load_into_postgres_table',
        python_callable=covid_load.incremental_load_into_postgres_table,
        op_kwargs={
            'country':'{{ params.Country }}',
            'execution_ts': '{{ ts}}'
        },
        trigger_rule='none_skipped',
        # provide_context=True
    )

    api_status >> full_or_incremental_load >> [full_load_ts, incremental_load_ts] >> write_to_bucket
    [full_load_ts, write_to_bucket] >> create_postgres_table >> full_load_into_postgres_table
    [incremental_load_ts, write_to_bucket] >> incremental_load_into_postgres_table