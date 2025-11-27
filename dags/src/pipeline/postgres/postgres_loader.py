from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import json
import pendulum
from pydantic import BaseModel

from src.secrets import aws_conn_id
from src.pipeline.S3.s3_writer import S3Client

class PostgresLoader(S3Client):
    def __init__(self, bucket_name: str, aws_conn_id: str, prefix: str, postgres_conn_id:str, table_name:str, schema:BaseModel):
        """Writer to Postgres. Inherits from S3 Client class required for version 1 dag.

        Args:
            bucket_name (str): _description_
            aws_conn_id (str): _description_
            prefix (str): _description_
            postgres_conn_id (str): _description_
            table_name (str): _description_
            schema (BaseModel): _description_
        """
        S3Client.__init__(self, bucket_name, aws_conn_id, prefix)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.schema = schema
        self.postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

    def latest_postgres_row_date(self) -> str:
        """
        Connects to postgres to get the max date (latest row) from the table.

        Returns:
            str: The latest date in the postgres table in "YYYY-MM-DD" format.
        """
        postgres_hook = PostgresHook(postgres_conn_id='postgres_db')
        latest_date_query = f'SELECT MAX(date) FROM {self.table_name};'

        max_date = (postgres_hook.get_first(latest_date_query)[0]).strftime("%Y-%m-%d")

        return max_date

    # Version 2 
    def load_into_postgres_table(self, insert_files:tuple) -> None:
        """
        Write to postgres table. 

        Args:
            insert_files (tuple): Row values to insert.
        """
        self.postgres_hook.insert_rows(
        table='covid_raw',
        rows=insert_files,
        target_fields=self.schema.model_json_schema()['required'],
        commit_every=1000,
        replace=False,
        executemany=False,
        fast_executemany=False,
        autocommit=False
        )


    # Version 1
    def incremental_load_into_postgres_table(self, execution_ts:datetime) -> None:
        """
        Inserts only new rows into Postgres table from S3 bucket based on the latest date in Postgres table and S3 filenames with dates > latest postgres row.

        Args:
            execution_ts (datetime): Timestamp of executed DAG run. Defined by airflow macros.
        """

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        max_date = self.latest_postgres_row_date()
        print(f'SQL Date: {max_date}')
        
        # Maybe pass the file list into xcom and grab from there? But what if the file list is too long?
        file_list = self.get_s3_filenames()

        insert_files = []

        # If it is not feasible to list all of the filenames (size constraints...) use:
        # 1. Stored metadata table to track the last uploaded json file
        # 2. Last modified ts and get list of all files where last modified ts > max_date's last modified ts
        # 3. Partition s3 bucket by date prefix (paritioned by year, month, day, etc.)
        for file in file_list:
            file_date = pendulum.parse((file.split('_')[-1]).removesuffix('.json'))

            if file_date > pendulum.parse(max_date):                # This IF statement is basically the only difference between full load into postgres
                obj = s3_hook.get_key(
                bucket_name=self.bucket_name,
                key=file
            )

                # Decode s3 json file to json
                content=obj.get()['Body'].read().decode('utf-8')
                json_file=json.loads(content)
                row_index= 0

                for row in json_file:
                    row_values = (                  # row_values is a tuple. insert_files is a list of tuples that can be inserted into postgres
                    row.get("date"),
                    row.get("confirmed"),
                    row.get("deaths"),
                    row.get("recovered"),
                    row.get("confirmed_diff"),
                    row.get("deaths_diff"),
                    row.get("recovered_diff"),
                    row.get("last_update"),
                    row.get("active"),
                    row.get("active_diff"),
                    row.get("fatality_rate"),
                    json.dumps(row.get("region")),
                    f'{file.split('/')[-1]}-{row_index}',                  # set up "Primary Key" to reference the file and row index
                    execution_ts
                )
                    insert_files.append(row_values)
                    row_index += 1
            else:
                continue
        
        postgres_hook.insert_rows(
        table='covid_raw',
        rows=insert_files,
        target_fields=self.schema.model_json_schema()['required'],
        commit_every=1000,
        replace=False,
        executemany=False,
        fast_executemany=False,
        autocommit=False
        )
        print(f'Row inserted into Postgres: {file}')

    def full_load_into_postgres_table(self, execution_ts:datetime) -> None:
        """
        Inserts new rows (full load) into Postgres table from S3 bucket.

        Args:
            execution_ts (datetime): Timestamp of executed DAG run. Defined by airflow macros.
        """
        s3_hook = S3Hook(aws_conn_id=self.bucket_name)
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        file_list = self.get_s3_filenames()
        
        # Getting list of data tuples can probably be separated into a different function
        insert_files = []

        for file in file_list:
            obj = s3_hook.get_key(
                bucket_name=self.bucket_name,
                key=file
            )

            # Decode s3 json file to json
            content=obj.get()['Body'].read().decode('utf-8')
            json_file=json.loads(content)
            row_index= 0

            for row in json_file:
                row_values = (                  # row_values is a tuple. insert_files is a list of tuples that can be inserted into postgres
                row.get("date"),
                row.get("confirmed"),
                row.get("deaths"),
                row.get("recovered"),
                row.get("confirmed_diff"),
                row.get("deaths_diff"),
                row.get("recovered_diff"),
                row.get("last_update"),
                row.get("active"),
                row.get("active_diff"),
                row.get("fatality_rate"),
                json.dumps(row.get("region")),
                f'{file}-{row_index}',                  # set up "Primary Key" to reference the file and row index
                execution_ts
            )
                insert_files.append(row_values)
                row_index += 1

        postgres_hook.insert_rows(
            table='covid_raw',
            rows=insert_files,
            target_fields=self.schema.model_json_schema()['required'],
            commit_every=1000,
            replace=False,
            executemany=False,
            fast_executemany=False,
            autocommit=False
        )
        print(f'Row inserted into Postgres: {file}')