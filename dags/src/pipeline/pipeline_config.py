# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from datetime import datetime, timedelta
import json
# import logging
import requests
# import pandas as pd
# from tempfile import NamedTemporaryFile
# import pendulum

from src.pipeline.api.api_extractor import ApiExtractor
from src.pipeline.S3.s3_loader import S3Loader
from src.pipeline.postgres.postgres_loader import PostgresLoader
from src.secrets import s3_bucket_name, aws_conn_id, postgres_conn_id
from src.utils.date_utils import get_list_of_dates

class PipelineConfig():
    def __init__(self, ApiExtractor, S3Loader, PostgresLoader):
        self.ApiExtractor = ApiExtractor
        self.S3Loader = S3Loader
        self.PostgresLoader = PostgresLoader

    def run(self, params=None, manual_start_date=None, manual_end_date=None, region_name=None):
        # Check API status
        api_status = self.ApiExtractor.check_api_status()
        if api_status != 200:
            raise Exception(f'API is not reachable. Status code: {api_status}')
        else:
            print('API is reachable. Proceeding with data extraction and loading.')
        
            # Check S3 bucket status and get full or incremental load timestamp
            if self.S3Loader.is_bucket_empty() == 'full_load_ts':
                print('S3 bucket is empty. Proceeding with full load.')
                start_date = self.S3Loader.get_full_load_ts(manual_start_date)

            else:
                print('S3 bucket has existing data. Proceeding with incremental load.')
                start_date = self.S3Loader.get_incremental_load_ts(region_name, manual_start_date)

            # Parse list of dates between start and end date
            dates = get_list_of_dates(start_date, manual_end_date)

            for d in dates:
                params = {
                    'region_name': region_name,
                    'date': d
                }
                json_file = self.ApiExtractor.extract(params=params)

                print(json_file)

                file_name = f'{self.S3Loader.prefix}/report_data_{d}.json'

                print(file_name)

                self.S3Loader.load(
                    json_data=json_file,
                    d=d,
                    file_key=file_name
                )

                print(f'Loaded data for date: {d} into S3 bucket.')

    def run_postgres_load(self, execution_ts, region_name=None):
        if self.PostgresLoader.latest_postgres_row_date():
            print('Postgres table has existing data. Proceeding with incremental load.')

            self.PostgresLoader.incremental_load_into_postgres_table(
                region_name=region_name,
                execution_ts=execution_ts
            )
        else:
            print('Postgres table is empty. Proceeding with full load.')

            self.PostgresLoader.full_load_into_postgres_table(
                execution_ts=execution_ts
            )