from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import logging
import requests
import pendulum

from src.s3 import s3

class api_to_s3(s3):
    def __init__(self, bucket_name, aws_conn_id):
        super().__init__(bucket_name=bucket_name, aws_conn_id=aws_conn_id)

    def check_api_status(self, **context) -> int:
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

    def get_list_of_dates(self, **kwargs) -> list:
        """
        Gets the list of dates from start date (pulled from XCOM) to end date (current date).

        Args:
            **kwargs: XCOM start date for full or incremental load

        Returns:
            list[str]: List of dates in format YYYY-MM-DD
        """

        start_date_xcom = (kwargs['ti'].xcom_pull(task_ids="full_load_ts", key="start_date") or kwargs['ti'].xcom_pull(task_ids="incremental_load_ts", key="start_date"))
        start_date = pendulum.instance(start_date_xcom)

        print('xcom:', start_date_xcom)
        print('sd:', start_date)

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
    
    def write_to_s3(self, **kwargs) -> None:
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

        dates = self.get_list_of_dates(**kwargs)

        logging.info('Loading data......')

        country = kwargs['params']['Country']

        for d in dates:
            url = f"https://covid-api.com/api/reports?date={d}&region_name={country}"
            response = requests.get(url)
            data = response.json()['data']

            if len(data) > 0:
                s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
                s3_hook.load_string(
                    string_data=json.dumps(data),
                    key=f'covid/{country}/report_data_{d}.json',
                    bucket_name=self.bucket_name,
                    replace=True
                    )    
                
                logging.info(f'File ({len(data)} records) has been saved to AWS bucket: covid/{country}/report_data_{d}.json')
            else:
                logging.info(f'There are no instances of covid for date: {d}')