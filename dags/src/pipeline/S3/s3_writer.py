from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import json
from typing import List

import logging
import requests
import pendulum

from src.pipeline.S3.s3_client import S3Client
from src.pipeline.api.api_reader import ApiReader
from src.utils.date_utils import get_list_of_dates
from src.utils.metadata_col_utils import inject_metadata_ts_file_key

class S3Writer(S3Client):
    def __init__(self, bucket_name:str, aws_conn_id:str, prefix:str):
        """Writer for S3 Bucket. Inherits from S3Client parent class.

        Args:
            bucket_name (str): _description_
            aws_conn_id (str): _description_
            prefix (str): _description_
        """
        S3Client.__init__(self, bucket_name, aws_conn_id, prefix)
    
    # for dag verion 2
    def write(self, json_data:List[dict], file_key:str) -> None:
        """Writes to S3 bucket

        Args:
            json_data (List[dict]): List of json data
            file_key (str): Name of the file in S3 bucket
        """
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_string(
            string_data=json.dumps(json_data),
            key=file_key,
            bucket_name=self.bucket_name,
            replace=True
            )

    # for dag version 1
    def write_to_s3(self, ts, region_name, start_date, manual_end_date=None) -> None:
        """
        Read API and write to S3 bucket for each date.
        Calls the function get_list_of_dates and loop through the dates list. Each loop writes a new json file to S3 bucket.

        Args:
            region_name (_type_): _description_
            start_date (_type_): _description_
            manual_end_date (_type_, optional): _description_. Defaults to None.
        """

        dates = get_list_of_dates(start_date, manual_end_date)

        for d in dates:
            params = {
                'region_name': region_name,
                'date': d
            }
            json_file = ApiReader.extract_api(url='https://covid-api.com/api/reports', params=params)

            print(json_file)

            file_name = f'{self.prefix}/report_data_{d}.json'

            print(file_name)

            json_file_with_metadata = inject_metadata_ts_file_key(json_file, file_name, ts)

            # self.S3Writer.load(
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