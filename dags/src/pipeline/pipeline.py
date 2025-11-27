from datetime import datetime
import json
import pendulum

from src.pipeline.pipeline_config import PipelineConfig
from src.pipeline.api.api_reader import ApiReader
from src.pipeline.S3.s3_reader import S3Reader
from src.pipeline.S3.s3_writer import S3Writer
from src.pipeline.postgres.postgres_loader import PostgresLoader
from src.secrets import s3_bucket_name, aws_conn_id, postgres_conn_id
from src.utils.date_utils import get_list_of_dates

class Pipeline():
    def __init__(self, config: PipelineConfig, api_reader: ApiReader, s3_reader: S3Reader, s3_writer: S3Writer, postgres_loader: PostgresLoader):
        """EL pipeline to S3 and Postgres.

        Args:
            config (PipelineConfig): _description_
            api_reader (ApiReader): _description_
            s3_reader (S3Reader): _description_
            s3_writer (S3Writer): _description_
            postgres_loader (PostgresLoader): _description_
        """
        self.config = config
        self.api_reader = api_reader
        self.s3_reader = s3_reader
        self.s3_writer = s3_writer
        self.postgres_loader = postgres_loader

    def run_s3_load(self, manual_start_date:datetime = None, manual_end_date:datetime = None, region_name:str = None) -> None:
        """
        Extract data from API then write to S3 bucket as a JSON file.
        
        Args:
            manual_start_date (datetime, optional): User-specified start date chosen upon triggering dag. Defaults to None.
            manual_end_date (datetime, optional): User-specified end date chosen upon triggering dag. Defaults to None.
            region_name (str, optional): User-specified country chosen upon triggering dag. Defaults to None.

        Raises:
            Exception: If the API is not reachable, return status code of the API call.
        """
        # Check API status
        api_status = self.api_reader.check_api_status()
        if api_status != 200:
            raise Exception(f'API is not reachable. Status code: {api_status}')
        else:
            print('API is reachable. Proceeding with data extraction and loading.')
        
            # Check S3 bucket status and get full or incremental load timestamp
            if self.s3_writer.evaluate_bucket_load_mode() == 'full_load_ts':
                print('S3 bucket is empty. Proceeding with full load.')
                start_date = self.s3_writer.get_full_load_ts(manual_start_date)

            else:
                print('S3 bucket has existing data. Proceeding with incremental load.')
                start_date = self.s3_writer.get_incremental_load_ts(manual_start_date)

            # Parse list of dates between start and end date
            dates = get_list_of_dates(start_date, manual_end_date)

            for d in dates:
                json_data = self.api_reader.read(region_name=region_name, date=d)
                # print(json_data)

                file_name = f'{self.s3_writer.prefix}/report_data_{d}.json'
                # print(file_name)
                
                self.s3_writer.write(
                    json_data=json_data,
                    file_key=file_name
                )

                print(f'Loaded data for date: {d} into S3 bucket as {file_name}')

    def run_postgres_load(self, execution_ts: datetime) -> None:
        """
        Extract data from S3 bucket and insert into Postgres table.

        Args:
            execution_ts (datetime): Timestamp of executed DAG run. Defined by airflow macros.
            region_name (str, optional): User-specified country chosen upon triggering dag. Defaults to None.
        """
        max_date = self.postgres_loader.latest_postgres_row_date()
        print(f'SQL Date: {max_date}')
        
        # Maybe pass the file list into xcom and grab from there? But what if the file list is too long?
        file_list = self.s3_reader.get_s3_filenames()

        insert_files = []

        # If it is not feasible to list all of the filenames (size constraints...) use:
        # 1. Stored metadata table to track the last uploaded json file
        # 2. Last modified ts and get list of all files where last modified ts > max_date's last modified ts
        # 3. Partition s3 bucket by date prefix (paritioned by year, month, day, etc.)
        for file in file_list:
            file_date = pendulum.parse((file.split('_')[-1]).removesuffix('.json'))

            if max_date in [None, 'None'] or file_date > pendulum.parse(max_date):          # If it's a full load (max date is none) or s3 file date greater than max date
                json_data = self.s3_reader.read(file_name=file)
                row_index= 0

                for row in json_data:
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
            
        self.postgres_loader.load_into_postgres_table(insert_files=insert_files)