from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import json
import logging
import requests
import pendulum

from src.secrets import s3_bucket_name

def check_api_status(**context) -> int:
    """
    Check the status of the api and return the status code.
    
    Args:
        **context: Airflow context dictionary containing:
            - params['Country']: Country enum in Canada, USA, China used to select which country

    Returns:
        int: The status code of the api (ex. 200, 404, etc.)
    """
    country = context['params']['Country']
    url = f"https://covid-api.com/api/reports?region_name={country}"
    response = requests.get(url)
    return response.status_code

def get_s3_filenames(**kwargs):
    country = kwargs['params']['Country']

    s3_hook = S3Hook(aws_conn_id='aws_bucket')

    file_list = s3_hook.list_keys(
        bucket_name=s3_bucket_name,
        prefix=f'covid/{country}'
        )
    return file_list

def evaluate_bucket_load_mode(**kwargs) -> str:
    """
    Checks if there are files in the S3 bucket. 
    
    If the bucket is empty, then push 0 to XCOM and return string "full_load_ts". 
    If there are files in the bucket, then push 1 to XCOM and return string "incremental_laod_ts".
    This will also be used as a decision branch operator to choose the type of load.
    Args:
        **kwargs: Airflow context dictionary containing:
                - ti (TaskInstance): Used to push data to XCom.
                - params['Country']: Country enum in Canada, USA, China

    Returns:
        string: 
    """

    file_list = get_s3_filenames(**kwargs)

    if not file_list:
        logging.info('Starting full load...')
        kwargs['ti'].xcom_push(
            key='evaluate_bucket_load_mode',
            value=0
        )
        return 'full_load_ts'
    else:
        logging.info('Starting incremental load...')
        kwargs['ti'].xcom_push(
        key='evaluate_bucket_load_mode',
        value=1
        )
        return 'incremental_load_ts'

def get_full_load_ts(**kwargs) -> None:
    """
    Gets the timestamp for a full load using the specified param (start_date) or manually set to Jan 1 2020 for a full load. Start timestamp is pushed to XCOM.
    
    Args:
        **kwargs: Airflow context dictionary containing:
                - ti (TaskInstance): Used to push data (start_date) to XCom.

    Returns:
        None
    """
    if kwargs['params']['Start Date']:
        start_date = pendulum.parse(kwargs['params']['Start Date'])
    else:
        start_date = pendulum.datetime(2020, 1, 1)

    kwargs['ti'].xcom_push(
        key='start_date',
        value=start_date
    )

def get_incremental_load_ts(**kwargs) -> None:
    """
    Gets the timestamp for an incremental load using the specified param (start_date) or the last modified metadata column in S3. Start timestamp is pushed to XCOM.

    **kwargs: Airflow context dictionary containing:
            - ti (TaskInstance): Used to push data (start_date) to XCom.
            - params['Country']: Country enum in Canada, USA, China

    Returns:
        None
    """
    if kwargs['params']['Start Date']:
        start_date = pendulum.parse(kwargs['params']['Start Date'])
    else:
        s3_hook = S3Hook(aws_conn_id='aws_bucket')
        
        # Get the list of files (keys) in s3
        file_list = get_s3_filenames(kwargs)

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
        latest_modified_ts = (pendulum.instance(file_dict[0][1])).subtract(years=5)

        start_date = last_modified_ts

        logging.info(f'Last Modified: {latest_modified_ts} | File Name: {latest_modified_name}')

    kwargs['ti'].xcom_push(
        key='start_date',
        value=start_date
    )

def get_list_of_dates(**kwargs):
    """
    Gets the list of dates from start date (pulled from XCOM) to end date (current date).

    Args:
        **kwargs: XCOM start date for full or incremental load

    Returns:
        list[str]: List of dates in format YYYY-MM-DD
    """

    start_date = (kwargs['ti'].xcom_pull(task_ids="full_load_ts", key="start_date") or kwargs['ti'].xcom_pull(task_ids="incremental_load_ts", key="start_date"))
    start_date = pendulum.instance(start_date)

    if kwargs['params']['End Date']:
        end_date = pendulum.parse(kwargs['params']['End Date'])
    else:
        end_date = pendulum.now().subtract(years=5)
    
    if end_date.format('YYYY-MM-DD') == start_date.format('YYYY-MM-DD'):
        dates_list = [end_date.format('YYYY-MM-DD')]
    else:
        interval = pendulum.interval(start_date, end_date)
        dates_list = [d.format('YYYY-MM-DD') for d in (interval.range('days'))]

    logging.info(f'Fetching data between {start_date} and {end_date}')
    logging.info(dates_list)
    return dates_list

def api_to_s3(**kwargs) -> None:
    """
    Read API and write to S3 bucket for each date.

    Calls the function get_list_of_dates and loop through the dates list. Each loop writes a new json file to S3 bucket.

    Args:
    **context: Airflow context dictionary containing:
        - params['Country']: Country enum in Canada, USA, China used to select which country

    Returns:
        None
    """
    logging.info('Getting dates list.......')

    dates = get_list_of_dates(**kwargs)

    logging.info('Loading data......')

    country = kwargs['params']['Country']

    for d in dates:
        url = f"https://covid-api.com/api/reports?date={d}&region_name={country}"
        response = requests.get(url)
        data = response.json()['data']

        if len(data) > 0:
            s3_hook = S3Hook(aws_conn_id='aws_bucket')
            s3_hook.load_string(
                string_data=json.dumps(data),
                key=f'covid/{country}/report_data_{d}.json',
                bucket_name=s3_bucket_name,
                replace=True
                )    
            
            logging.info(f'File ({len(data)} records) has been saved to AWS bucket: covid/{country}/report_data_{d}.json')
        else:
            logging.info(f'There are no instances of covid for date: {d}')

# Write to postgres

def full_load_into_postgres_table(**kwargs):

    s3_hook = S3Hook(aws_conn_id='aws_bucket')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_db')

    country = kwargs['params']['Country']

    file_list = get_s3_filenames(**kwargs)
    
    insert_files = []

    for file in file_list:
        obj = s3_hook.get_key(
            bucket_name='vd-airflow-docker-bucket',
            key=file
        )

        # Decode s3 json file to json
        content=obj.get()['Body'].read().decode('utf-8')
        json_file=json.loads(content)
        
        for row in json_file:
            row_values = (                  # row_values is a tuple. insert_files is a list of tuples that can be inserted into postgres
            row.get("date"),
            row.get("confirmed"),
            row.get("deaths"),
            row.get("recovered"),
            row.get("confirmed_diff"),
            row.get("deaths_diff"),
            row.get("recovered_diff"),
            row.get("last_update"),
            row.get("active"),
            row.get("active_diff"),
            row.get("fatality_rate"),
            json.dumps(row.get("region"))
            # Add created_ts, updated_ts
        )
            insert_files.append(row_values)

    postgres_hook.insert_rows(
        table='covid_raw',
        rows=insert_files,
        target_fields=["date","confirmed","deaths","recovered","confirmed_diff","deaths_diff","recovered_diff","last_update","active","active_diff","fatality_rate","region"],
        commit_every=1000,
        replace=False,
        executemany=False,
        fast_executemany=False,
        autocommit=False
    )
    print(f'Row inserted into Postgres: {file}')