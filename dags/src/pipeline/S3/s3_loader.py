from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import json
from typing import List

import logging
import requests
import pendulum

from .s3_client import S3Client
from src.pipeline.api.api_extractor import ApiExtractor
from src.utils.date_utils import get_list_of_dates

class S3Loader(S3Client):
    def __init__(self, bucket_name, aws_conn_id, prefix):
        S3Client.__init__(self, bucket_name, aws_conn_id, prefix)
    
    # for dag verion 2
    def load(self, json_data: List[dict], d:str, file_key) -> None:
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_string(
            string_data=json.dumps(json_data),
            key=file_key,
            bucket_name=self.bucket_name,
            replace=True
            )

    # for dag version 1
    def write_to_s3(self, region_name, start_date, manual_end_date=None) -> None:
        """
        Read API and write to S3 bucket for each date.

        Calls the function get_list_of_dates and loop through the dates list. Each loop writes a new json file to S3 bucket.

        Args:
        **context: Airflow context dictionary containing:
            - params['Country']: Country enum in Canada, USA, China used to select which country

        Returns:
            None
        """

        dates = get_list_of_dates(start_date, manual_end_date)

        for d in dates:
            params = {
                'region_name': region_name,
                'date': d
            }
            json_file = ApiExtractor.extract_api(url='https://covid-api.com/api/reports', params=params)

            print(json_file)

            file_name = f'{self.prefix}/report_data_{d}.json'

            print(file_name)

            # self.S3Loader.load(
            #     json_data=json_file,
            #     d=d,
            #     file_key=file_name
            # )

            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_hook.load_string(
            string_data=json.dumps(json_file),
            key=file_name,
            bucket_name=self.bucket_name,
            replace=True
            )

            print(f'Loaded data for date: {d} into S3 bucket.')

        # logging.info('Getting dates list.......')
        # dates = self.get_list_of_dates(start_date, manual_end_date)

        # logging.info('Loading data......')
        # for d in dates:
        #     url = f"https://covid-api.com/api/reports?date={d}&region_name={region_name}"
        #     response = requests.get(url)
        #     data = response.json()['data']

        #     if len(data) > 0:
        #         s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        #         s3_hook.load_string(
        #             string_data=json.dumps(data),
        #             key=f'covid/{region_name}/report_data_{d}.json',
        #             bucket_name=self.bucket_name,
        #             replace=True
        #             )    
                
        #         logging.info(f'File ({len(data)} records) has been saved to AWS bucket: covid/{country}/report_data_{d}.json')
        #     else:
        #         logging.info(f'There are no instances of covid for date: {d}')