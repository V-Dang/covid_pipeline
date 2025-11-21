# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from datetime import datetime, timedelta
import json
# import logging
import requests
# import pandas as pd
# from tempfile import NamedTemporaryFile
# import pendulum


from src.pipeline.S3.s3_client import S3Client
from src.pipeline.S3.s3_loader import S3Loader
from src.pipeline.api.api_client import ApiClient
from src.pipeline.api.api_extractor import ApiExtractor
from src.secrets import s3_bucket_name, aws_conn_id, postgres_conn_id
from src.utils.date_utils import get_list_of_dates

dates = get_list_of_dates('2021-01-01', '2021-01-03')

for d in dates:
    covid_extract = ApiExtractor(
        url='https://covid-api.com/api/reports',
        params={
            'date': f'{d}',
            'region_name': 'Canada'
            }
        )

    json_file = covid_extract.extract()

    covid_load = S3Loader(
        bucket_name=s3_bucket_name,
        aws_conn_id=aws_conn_id,
        prefix='covid/Canada' 
    )

    file_name = f'covid/Canada/report_data_{d}.json'

    covid_load.load(
        json_data=json_file,
        d=d,
        file_key=file_name
    )

