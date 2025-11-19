from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import json
from typing import List

import logging
import requests
import pendulum


from dags.src.S3Client.s3_client import S3Client

class S3Loader(S3Client):
    def __init__(self, S3Client):
        self.S3Client = S3Client
    
    def load(self, country, start_date, manual_end_date=None) -> None:
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
        dates = self.get_list_of_dates(start_date, manual_end_date)

        logging.info('Loading data......')
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