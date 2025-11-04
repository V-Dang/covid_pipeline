from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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

def is_bucket_empty(**kwargs) -> str:
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

    country = kwargs['params']['Country']

    s3_hook = S3Hook(aws_conn_id='aws_bucket')

    file_list = s3_hook.list_keys(
        bucket_name=s3_bucket_name,
        prefix=f'covid/{country}'
        )
    if not file_list:
        logging.info('Starting full load...')
        kwargs['ti'].xcom_push(
            key='is_bucket_empty',
            value=0
        )
        return 'full_load_ts'
    else:
        logging.info('Starting incremental load...')
        kwargs['ti'].xcom_push(
        key='is_bucket_empty',
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
        country = kwargs['params']['Country']
        # Get the list of files (keys) in s3
        file_list = s3_hook.list_keys(
            bucket_name=s3_bucket_name,
            prefix=f'covid/{country}/'
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
    Read APi and write to S3 bucket for each date.

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