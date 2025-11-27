from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import json

from .s3_client import S3Client

class S3Reader(S3Client):
    def __init__(self, bucket_name:str, aws_conn_id:str, prefix:str):
        """Reader class for S3 bucket. Inherits from S3 Client class

        Args:
            bucket_name (str): _description_
            aws_conn_id (str): _description_
            prefix (str): _description_
        """
        S3Client.__init__(self, bucket_name, aws_conn_id, prefix)
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
    
    def read(self, file_name:str) -> list[dict] | dict:
        """Read and decode the S3 object as a JSON.

        Args:
            file_name (str): File name fo S3 object to read.

        Returns:
            list[dict] | dict: JSON content of S3 object as list of dictionaries or dictionary.
        """
        obj = self.s3_hook.get_key(
            bucket_name=self.bucket_name,
            key=file_name
        )

        # Decode s3 json file to json
        content=obj.get()['Body'].read().decode('utf-8')
        json_file=json.loads(content)

        return json_file