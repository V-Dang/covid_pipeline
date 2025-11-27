
# COVID ELT Project

Background:
- This project shows an ELT process using a dockerized airflow orchestrator to grab COVID report data from an API and write to an AWS S3 bucket as a full or incremental load. This data is then written into raw (bronze) tables in a database and transformed using DBT.
- As this API is no longer tracking recent cases of COVID, the dates are artificially set back 5 years to simulate incremental loading. These dates include:
    - last modified date (start date)
    - current date (end date)

Schedule:
- This DAG is set to run once a day.

Architecture:
- API --> S3 --> Data Warehouse (Postgres) --> Data Mart

There are 2 DAG versions of this pipeline.

Version 1: Multiple Dag Tasks (better visual representation)
1. api_status
    - Checks the status of the api. If the message is successful (Response Code: 200), then proceed
2. full_or_incremental_load
    - Checks the status of the S3 bucket (evaluate_bucket_load_mode()) using an S3 Hook.
    - Branch operator that decides which next task to run
    - If the bucket is empty (list_keys returns None), then do a full load
    - If the bucket is not empty (list_keys returns values), then do an incremental load
3. (A) full_load_ts
    - Set the implicit XCOM (return_value) as January 1, 2020
    - If there is a param value for start_date, then use the param value instead
3. (B) incremental_load_ts
    - Get the timestamp from the lastest modified file metadata
    - Set the implicit XCOM (return_value) as the last_modified 
        - *** 5 years will be subtracted from this to simulate incremental loading
    - If there is a param value for start_date, then use the param value instead
5. write_to_bucket
    - This is the final task that writes json files from the API to the S3 bucket.
    - Gets the list of dates between the start date and end date. These dates must be pendulum.datetime type and formatted as YYYY-MM-DD 
        - *** The API can only accept 1 specified date at a time (no date between queries)
    - Loop through the list of dates to get from the API
        - If the returned JSON file contains data, then write the file to the S3 Bucket as covid/report_data_YYYY-MM-DD.json
        - Else (if the returned JSON output does not have any values), then print a statement that indicates no data found

Version 2: Single Dag Task (majority of code is in pipelineconfig.py run functions)
1. run
    - Checks API status
    - Checks bucket status (empty or not) to get incremental or full load timestamps
    - Extract list of dates to pass into API
    - Load extracted API data into S3 as a JSON (1 file per day)
2. run_postgres_load
    - Checks if postgres table has a max watermark date value.
        - If yes, then incremental load
        - If no, then create table and then full load

Params:
- There are 3 different params.
1. Country - Will be used to read the api and write to a subfolder in the bucket
2. Start Date (Optional) - If this is not set upon triggering the dag, then the start_date used will be grabbed from downstream tasks
3. End Date (Optional) - If this is not set upon triggering the dag, then the end_date used will be grabbed from downstream tasks

Other Considerations:
- Different methods to get start_date for incremental loading
    - Getting start_date by parsing latest filename in the S3 bucket
    - Getting start_date by getting the last successful execution ts (airflow macros)
- Add different countries 
    - Each country should have a DAG?
- File format optimization
    - Converting JSON files to parquet for optimizing storage, read operations downstream
- Use DBT/PySpark for data normalization, validation, and clean-up in curated (silver) layer
- Use DBT to handle best practices for data transformations and business (gold) layer
- Airflow params

TO-DO:
- Save S3/airflow dag runs into a Postgres table to track metadata
- Pydantic model validation
- IaC
- Add transformation orchestration task (DBT)

- {...} -> {..., "_retrieved_at": "2025-02-01"}
- {...} -> {"column": str(column)}

Docker:
- Running locally (using container dependencies). Must enter into container:
    docker compose exec airflow-worker bash
    docker compose exec airflow-scheduler bash

Links and References:
- API: https://covid-api.com/api/
- Airflow: 
    - Macros - https://airflow.apache.org/docs/apache-airflow/3.1.1/templates-ref.html
    - S3Hooks - https://airflow.apache.org/docs/apache-airflow/1.10.6/_api/airflow/hooks/S3_hook/index.html
    - Dynamic Task Mapping - https://airflow.apache.org/docs/apache-airflow/3.1.1/authoring-and-scheduling/dynamic-task-mapping.html
    - Params - https://www.astronomer.io/docs/learn/airflow-params
    - Transfer S3 to Postgres 
        - https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/transfers/s3_to_sql/index.html#airflow.providers.amazon.aws.transfers.s3_to_sql.S3ToSqlOperator
        - https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/transfer/s3_to_sql.html
    - Insert into Postgres from S3
        - https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html#airflow.providers.postgres.hooks.postgres.PostgresHook.insert_rows