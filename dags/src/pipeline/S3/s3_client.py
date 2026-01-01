from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
import logging
import pendulum

class S3Client():
    def __init__(self, bucket_name:str, aws_conn_id:str, prefix:str):
        """AWS S3 Client to connect, read from the S3 bucket, and extrapolate metadata

        Args:
            bucket_name (str): name of S3 bucket
            aws_conn_id (str): name of Airflow AWS connection
            prefix (str): subfolder in bucket
        """
        self.bucket_name = bucket_name
        self.aws_conn_id = aws_conn_id
        self.prefix = prefix
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
    
    def get_s3_filenames(self) -> list[str]:
        """List keys (file names) in S3 bucket under specified prefix/folder

        Returns:
            list: list of all file names (keys) in S3 bucket folder
        """

        file_list = self.s3_hook.list_keys(
            bucket_name=self.bucket_name,
            prefix=self.prefix
            )
        return file_list

    def evaluate_bucket_load_mode(self) -> str:
        """Checks if there are files in the S3 bucket. 
        
        If the bucket is empty, then BranchPythonOperator proceeds to do a full load. 
        If there are files in the bucket, then BranchPythonOperator proceeds to do an incremental load.

        Returns:
            string: (ex. full_load_ts, incremental_load_ts)
        """
        file_list = self.get_s3_filenames()

        if not file_list:
            logging.info('Starting full load...')
            return 'full_load_ts'
        else:
            logging.info('Starting incremental load...')
            return 'incremental_load_ts'

    def get_full_load_ts(self, manual_start_date:datetime = None) -> str:
        """Gets the timestamp for a full load using the specified param (start_date) or manually set to Jan 1 2020 for a full load. Start timestamp is pushed to XCOM.

        Args:
            manual_start_date (_type_, optional): User-specified start date chosen upon triggering dag. Defaults to None.

        Returns:
            str: Pendulum datetime for full load start date
        """
        if manual_start_date not in ['None', None]:
            start_date = pendulum.parse(manual_start_date)
        else:
            start_date = pendulum.datetime(2020, 1, 1)

        return start_date

    def get_incremental_load_ts(self, manual_start_date:datetime = None) -> str:
        """
        Gets the timestamp for an incremental load using the specified param (start_date) or the last modified metadata column in S3. Start timestamp is pushed to XCOM.

        Args:
            manual_start_date (datetime, optional): User-specified start date chosen upon triggering dag. Defaults to None.

        Returns:
            str: Pendulum datetime for incremental load start date
        """
        if manual_start_date not in ['None', None]:
            start_date = pendulum.parse(manual_start_date)
        else:
            # Get the list of files (keys) in s3
            file_list = self.get_s3_filenames()

            file_dict = {}

            # Get the last modified timestamp for each file/key
            for filename in file_list:
                last_modified_ts = self.s3_hook.get_key(
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