from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.sdk import Param, get_current_context

from datetime import datetime, timedelta
import logging
import requests
import json
import io

from src.pipeline.api.api_extractor import ApiExtractor
from src.pipeline.S3.s3_loader import S3Loader
from src.pipeline.postgres.postgres_loader import PostgresLoader
from src.secrets import s3_bucket_name, aws_conn_id, postgres_conn_id
from src.utils.date_utils import get_list_of_dates
from src.pipeline.pipeline_config import PipelineConfig
from src.models.schemas import CovidDataSchema

# Define pipeline config obj here
covid_pipeline = PipelineConfig(
    ApiExtractor=ApiExtractor(
        url='https://covid-api.com/api/reports',
        # params={
        #         'region_name':'Canada',
        #         'date': None
        #         },
        ),
    S3Loader=S3Loader(
        bucket_name=s3_bucket_name,
        aws_conn_id=aws_conn_id,
        prefix='covid/Canada'
        ),
    PostgresLoader=PostgresLoader(
        bucket_name=s3_bucket_name,
        aws_conn_id=aws_conn_id,
        prefix='covid/Canada',

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
    dag_id='covid_pipeline_v2',
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
    run_pipeline = PythonOperator(
        task_id='run_pipeline',
        python_callable=covid_pipeline.run,
        op_kwargs={
                'manual_start_date': '{{ params.start_date }}',
                'manual_end_date': '{{ params.end_date }}',
                'region_name': '{{ params.Country }}'
            }
    )

    run_postgres = PythonOperator(
        task_id='run_postgres',
        python_callable=covid_pipeline.run_postgres_load,
        op_kwargs={
            'region_name': '{{ params.Country }}',
            'execution_ts': '{{ ts }}'
        }
    )
run_pipeline >> run_postgres