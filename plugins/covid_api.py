from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import logging
import requests
import pandas as pd
from tempfile import NamedTemporaryFile

from secrets import s3_bucket_name

def check_api_status():
    url = "https://covid-api.com/api/reports"
    response = requests.get(url)
    return response.status_code

# Created string will be used as an xcom
def get_filename(current_date, **kwargs)-> None:
    filename = f'covid/report_data_{current_date}.json'
    kwargs['ti'].xcom_push(
        key='filename',
        value=filename
    )
    print(f'Filename: {filename}')

def api_to_s3(current_date: str, **kwargs) -> None:

    filename = kwargs['ti'].xcom_pull(
        task_ids='create_filename',
        key='filename'
    )

    url = f"https://covid-api.com/api/reports?date={current_date}"
    response = requests.get(url)
    data = response.json()['data']

    s3_hook = S3Hook(aws_conn_id='aws_bucket')
    s3_hook.load_string(
        # filename=temp_path,
        string_data=json.dumps(data),
        key=filename,
        bucket_name=s3_bucket_name,
        replace=True
        )    
    
    logging.info(f'File ({len(data)} records) has been saved to AWS bucket: {filename}')

def fetch_covid_data(current_date: str):

    # filename = kwargs['ti'].xcom_pull

    url = f"https://covid-api.com/api/reports?date={current_date}"
    response = requests.get(url)
    data = response.json()['data']
    # data = json.dumps(response.json(), indent=2)

    # with NamedTemporaryFile(mode='w', suffix=filename) as f:
    #     temp_data = json.dump(data)
    #     temp_data.flush()
    #     # content = json.loads(temp_data)

    logging.info(f"Fetched {len(data)} records for {current_date}")
        
    return data

    # print(data)

def json_to_s3(filename: str, data: str, current_date: str) -> None:
    with NamedTemporaryFile(mode='w', suffix=filename) as f:
        json.dumps(data)

        logging.info(f"Fetched {len(data)} records for {current_date}")

        s3_hook = S3Hook(aws_conn_id='aws_bucket')
        s3_hook.load_file(
            filename=f,                  # Path to json file (source)
            key=filename,                       # Path in container (target)
            bucket_name=s3_bucket_name,
            replace=True
            )    
        logging.info(f'Order data file has been saved to AWS bucket: {filename}')