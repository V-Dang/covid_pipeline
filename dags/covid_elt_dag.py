from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Param, get_current_context

from datetime import datetime, timedelta

from src.models.schemas import CovidDataSchema
from src.pipeline.api.api_reader import ApiReader
from src.pipeline.pipeline import Pipeline
from src.pipeline.pipeline_config import PipelineConfig
from src.pipeline.postgres.postgres_loader import PostgresLoader
from src.pipeline.S3.s3_reader import S3Reader
from src.pipeline.S3.s3_writer import S3Writer
from src.secrets import s3_bucket_name, aws_conn_id, postgres_conn_id

# Define pipeline config obj here
config=PipelineConfig(
        api_url='https://covid-api.com/api/reports',
        s3_prefix='covid/Canada',
        postgres_table='covid_raw'
    )

covid_pipeline = Pipeline(
    # NOTE: should be named as CovidApiReader for specificity
    config=config,
    api_reader=ApiReader(
        url=config.api_url
        ),
    s3_writer=S3Writer(
        bucket_name=s3_bucket_name,
        aws_conn_id=aws_conn_id,
        prefix=config.s3_prefix
        ),
    s3_reader=S3Reader(
        bucket_name=s3_bucket_name,
        aws_conn_id=aws_conn_id,
        prefix=config.s3_prefix
    ),
    postgres_loader=PostgresLoader(
        bucket_name=s3_bucket_name,
        aws_conn_id=aws_conn_id,
        prefix=config.s3_prefix,

        postgres_conn_id=postgres_conn_id,
        table_name='covid_raw',
        schema=CovidDataSchema
    )
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
        python_callable=covid_pipeline.api_reader.check_api_status,
        op_kwargs={
            'region_name': '{{ params.Country }}'
        }
    )
    full_or_incremental_load = BranchPythonOperator(
        task_id = 'full_or_incremental_load',
        python_callable=covid_pipeline.s3_writer.evaluate_bucket_load_mode,
        op_kwargs={
            'region_name': '{{ params.Country }}'
        }
    )
    full_load_ts = PythonOperator(
        task_id = 'full_load_ts',
        python_callable=covid_pipeline.s3_writer.get_full_load_ts,
        op_kwargs={
            'manual_start_date': '{{ params.start_date }}'
        }
    )
    incremental_load_ts = PythonOperator(
        task_id = 'incremental_load_ts',
        python_callable=covid_pipeline.s3_writer.get_incremental_load_ts,
        op_kwargs={
            'manual_start_date': '{{ params.start_date }}',
            'region_name': '{{ params.Country }}'
        }
    )
    write_to_bucket = PythonOperator(
        task_id = 'write_to_bucket',
        python_callable=covid_pipeline.s3_writer.write_to_s3,
        op_kwargs={
            'region_name': '{{ params.Country }}',
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
        python_callable=covid_pipeline.postgres_loader.full_load_into_postgres_table,           # Postgres Loader class requires inheritance from Parent Class S3Client
        trigger_rule='none_skipped',
        # provide_context=True
    )
    incremental_load_into_postgres_table = PythonOperator(
        task_id='incremental_load_into_postgres_table',
        python_callable=covid_pipeline.postgres_loader.incremental_load_into_postgres_table,     # Postgres Loader class requires inheritance from Parent Class S3Client
        op_kwargs={
            'region_name':'{{ params.Country }}',
            'execution_ts': '{{ ts }}'
        },
        trigger_rule='none_failed',
        # provide_context=True
    )

    api_status >> full_or_incremental_load >> [full_load_ts, incremental_load_ts] >> write_to_bucket
    [full_load_ts, write_to_bucket] >> create_postgres_table >> full_load_into_postgres_table
    [incremental_load_ts, write_to_bucket] >> incremental_load_into_postgres_table