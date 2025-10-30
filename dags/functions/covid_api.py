from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import json
import logging
import requests
# import pandas as pd
from tempfile import NamedTemporaryFile
import pendulum

from secrets import s3_bucket_name

def check_api_status():
    url = "https://covid-api.com/api/reports"
    response = requests.get(url)
    return response.status_code

def is_bucket_empty(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_bucket')

    file_list = s3_hook.list_keys(
        bucket_name=s3_bucket_name,
        prefix='covid/'
        )
    if not file_list:
        print('Starting full load...')
        kwargs['ti'].xcom_push(
            key='is_bucket_empty',
            value=0
        )
        return 'full_load_ts'
    else:
        print('Starting incremental load...')
        kwargs['ti'].xcom_push(
        key='is_bucket_empty',
        value=1
        )
        return 'incremental_load_ts'

def full_load_ts(**kwargs):
    start_date = pendulum.datetime(2020, 1, 1)
    kwargs['ti'].xcom_push(
        key='start_date',
        value=start_date
    )

def incremental_load_ts(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_bucket')

    # Get the list of files (keys) in s3
    file_list = s3_hook.list_keys(
        bucket_name=s3_bucket_name,
        prefix='covid/'
        )

    file_dict = {}

    # Get the last modified timestamp for each file/key
    for filename in file_list:
        last_modified_ts = s3_hook.get_key(
            bucket_name=s3_bucket_name,
            key=filename
        ).last_modified
        file_dict[filename] = last_modified_ts

    # Get latest modified date
    file_dict = sorted(file_dict.items(), key=lambda x:x[1], reverse=True)
    latest_modified_name = file_dict[0][0]
    latest_modified_ts = pendulum.instance(file_dict[0][1]).subtract(years=5)

    print(f'Last Modified: {latest_modified_ts} | File Name: {latest_modified_name}')

    kwargs['ti'].xcom_push(
        key='start_date',
        value=latest_modified_ts
    )

def api_to_s3(**kwargs) -> None:

    filename = kwargs['ti'].xcom_pull(
        task_ids='create_filename',
        key='filename'
    )

    incremental_or_full = kwargs['ti'].xcom_pull(
        task_ids='full_or_incremental_load',
        key='is_bucket_empty'
    )

    # Create list of dates to pull and write to.
    start_date = kwargs['ti'].xcom_pull(
        task_ids=['full_load_ts', 'incremental_load_ts'],
        key='start_date'
    )[incremental_or_full]
    


    end_date = pendulum.now().subtract(years=5)

    print(f'start date: {start_date} end date:{end_date}')

    interval = pendulum.interval(start_date, end_date)
    dates_list = [d.format('YYYY-MM-DD') for d in (interval.range('days'))]
    print(dates_list)

    for d in dates_list:
        url = f"https://covid-api.com/api/reports?date={d}&region_name=Canada"
        response = requests.get(url)
        data = response.json()['data']

        if len(data) > 0:
            s3_hook = S3Hook(aws_conn_id='aws_bucket')
            s3_hook.load_string(
                string_data=json.dumps(data),
                key=f'covid/report_data_{d}.json',
                bucket_name=s3_bucket_name,
                replace=True
                )    
            
            logging.info(f'File ({len(data)} records) has been saved to AWS bucket: {filename}')
        else:
            print(f'There are no instances of covid for date: {d}')