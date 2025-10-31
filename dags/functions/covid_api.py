from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import logging
import requests
import pendulum

from functions.secrets import s3_bucket_name

def check_api_status() -> int:
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

def get_full_load_ts(**kwargs):
    start_date = pendulum.datetime(2020, 1, 1)
    kwargs['ti'].xcom_push(
        key='start_date',
        value=start_date
    )

def get_incremental_load_ts(**kwargs):
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
    latest_modified_ts = pendulum.instance(file_dict[0][1])

    print(f'Last Modified: {latest_modified_ts} | File Name: {latest_modified_name}')

    kwargs['ti'].xcom_push(
        key='start_date',
        value=latest_modified_ts
    )

def get_list_of_dates(**kwargs):
    start_date = (kwargs['ti'].xcom_pull(task_ids="full_load_ts", key="start_date") or kwargs['ti'].xcom_pull(task_ids="incremental_load_ts", key="start_date"))

    start_date = (pendulum.instance(start_date).subtract(years=5)).format('YYYY-MM-DD')
    end_date = (pendulum.now().subtract(years=5)).format('YYYY-MM-DD')
    
    if end_date == start_date:
        dates_list = [end_date]
    else:
        interval = pendulum.interval(start_date, end_date)
        dates_list = [d.format('YYYY-MM-DD') for d in (interval.range('days'))]

    print(f'Fetching data between {start_date} and {end_date}')
    print(dates_list)
    return dates_list

def api_to_s3(**kwargs) -> None:
    print('Getting dates list.......')

    dates = get_list_of_dates(**kwargs)

    print('Loading data......')

    for d in dates:
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
            
            logging.info(f'File ({len(data)} records) has been saved to AWS bucket: covid/report_data_{d}.json')
        else:
            print(f'There are no instances of covid for date: {d}')