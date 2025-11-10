from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import pendulum

from src.s3 import s3

class s3_to_postgres(s3):
    def __init__(self, bucket_name, aws_conn_id, postgres_conn_id):
        super().__init__(bucket_name=bucket_name, aws_conn_id=aws_conn_id)
        self.postgres_conn_id = postgres_conn_id

    def latest_postgres_row_date(self):
        postgres_hook = PostgresHook(postgres_conn_id='postgres_db')
        latest_date_query = 'SELECT MAX(date) FROM covid_test;'

        max_date = (postgres_hook.get_first(latest_date_query)[0]).strftime("%Y-%m-%d")

        return max_date

    def incremental_load_into_postgres_table(self, **kwargs):

        s3_hook = S3Hook(aws_conn_id='aws_bucket')
        postgres_hook = PostgresHook(postgres_conn_id='postgres_db')

        max_date = self.latest_postgres_row_date()
        print(f'SQL Date: {max_date}')
        
        # Maybe pass the file list into xcom and grab from there? But what if the file list is too long
        file_list = self.get_s3_filenames(**kwargs)

        insert_files = []

        # Maybe use max_date to get last modified date and get list of all files where last modified ts > max_date's last modified ts
        for file in file_list:
            file_date = pendulum.parse((file.split('_')[-1]).removesuffix('.json'))

            if file_date > pendulum.parse(max_date):
                obj = s3_hook.get_key(
                bucket_name=self.bucket_name,
                key=file
            )

                # Decode s3 json file to json
                content=obj.get()['Body'].read().decode('utf-8')
                json_file=json.loads(content)
                
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
                    json.dumps(row.get("region"))
                    # Add created_ts, updated_ts
                )
                    insert_files.append(row_values)
            else:
                continue
        
        postgres_hook.insert_rows(
        table='covid_test',
        rows=insert_files,
        target_fields=["date","confirmed","deaths","recovered","confirmed_diff","deaths_diff","recovered_diff","last_update","active","active_diff","fatality_rate","region"],
        commit_every=1000,
        replace=False,
        executemany=False,
        fast_executemany=False,
        autocommit=False
        )
        print(f'Row inserted into Postgres: {file}')

    def full_load_into_postgres_table(self, **kwargs):

        s3_hook = S3Hook(aws_conn_id=self.bucket_name)
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        file_list = self.get_s3_filenames(**kwargs)
        
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
                json.dumps(row.get("region"))
                # Add created_ts, updated_ts
            )
                insert_files.append(row_values)

        postgres_hook.insert_rows(
            table='covid_test',
            rows=insert_files,
            target_fields=["date","confirmed","deaths","recovered","confirmed_diff","deaths_diff","recovered_diff","last_update","active","active_diff","fatality_rate","region"],
            commit_every=1000,
            replace=False,
            executemany=False,
            fast_executemany=False,
            autocommit=False
        )
        print(f'Row inserted into Postgres: {file}')