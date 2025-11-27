from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Param

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
        python_callable=covid_pipeline.run_s3_load,
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