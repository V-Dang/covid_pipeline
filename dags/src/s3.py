from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
import pendulum

class s3():
    def __init__(self, bucket_name, aws_conn_id, **kwargs):
        super().__init__(**kwargs)
        # self.url = url
        self.bucket_name = bucket_name
        self.aws_conn_id = aws_conn_id
    
    def get_s3_filenames(self, country):
        # country = kwargs['params']['Country']

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        file_list = s3_hook.list_keys(
            bucket_name=self.bucket_name,
            prefix=f'covid/{country}'
            )
        return file_list

    def is_bucket_empty(self, country) -> str:
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
        file_list = self.get_s3_filenames(country)

        if not file_list:
            logging.info('Starting full load...')
            return 'full_load_ts'
        else:
            logging.info('Starting incremental load...')
            return 'incremental_load_ts'

    def get_full_load_ts(manual_start_date=None) -> str:
        """
        Gets the timestamp for a full load using the specified param (start_date) or manually set to Jan 1 2020 for a full load. Start timestamp is pushed to XCOM.
        
        Args:
            **kwargs: Airflow context dictionary containing:
                    - ti (TaskInstance): Used to push data (start_date) to XCom.

        Returns:
            None
        """
        if manual_start_date:
            start_date = pendulum.parse(manual_start_date)
        else:
            start_date = pendulum.datetime(2020, 1, 1)

        return start_date

    def get_incremental_load_ts(self, country, manual_start_date=None) -> str:
        """
        Gets the timestamp for an incremental load using the specified param (start_date) or the last modified metadata column in S3. Start timestamp is pushed to XCOM.

        **kwargs: Airflow context dictionary containing:
                - ti (TaskInstance): Used to push data (start_date) to XCom.
                - params['Country']: Country enum in Canada, USA, China

        Returns:
            None
        """
        if manual_start_date != 'None':
            start_date = pendulum.parse(manual_start_date)
        else:
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            
            # Get the list of files (keys) in s3
            file_list = self.get_s3_filenames(country)

            file_dict = {}

            # Get the last modified timestamp for each file/key
            for filename in file_list:
                last_modified_ts = s3_hook.get_key(
                    bucket_name=self.bucket_name,
                    key=filename
                ).last_modified
                file_dict[filename] = last_modified_ts

            # Get latest modified date
            file_dict = sorted(file_dict.items(), key=lambda x:x[1], reverse=True)
            latest_modified_name = file_dict[0][0]
            latest_modified_ts = (pendulum.instance(file_dict[0][1])).subtract(years=5)

            start_date = latest_modified_ts

        logging.info(f'Last Modified: {latest_modified_ts} | File Name: {latest_modified_name}')
        logging.info(f'Start Date: {start_date}')

        return start_date