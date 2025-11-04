
# COVID ELT Project

Background:
- This project shows an ELT process using a dockerized airflow orchestrator to grab COVID report data from an API and write to an AWS S3 bucket as a full or incremental load. Eventually, this data will be written into raw (bronze) tables in a database and transformed using DBT/PySpark.
- As this API is no longer tracking recent cases of COVID, the dates are artificially set back 5 years to simulate incremental loading. These dates include:
    - last modified date (start date)
    - current date (end date)

Schedule:
- This DAG is set to run once a day.

Architecture:
- API --> S3 --> Database (TBD)

DAG:
1. api_status
    - Checks the status of the api. If the message is successful (Response Code: 200), then proceed
2. full_or_incremental_load
    - Checks the status of the S3 bucket using an S3 Hook.
    - Branch operator that decides which next task to run
    - If the bucket is empty (list_keys returns None), then do a full load
    - If the bucket is not empty (list_keys returns values), then do an incremental load
    - Stores output (bool) as an XCOM
3. (A) full_load_ts
    - Set the XCOM (start_date) as January 1, 2020
    - If there is a param value for start_date, then use the param value instead
3. (B) incremental_load_ts
    - Get the timestamp from the lastest modified file metadata
    - Set the XCOM (start_date) as the last_modified 
        - *** 5 years will be subtracted from this to simulate incremental loading
    - If there is a param value for start_date, then use the param value instead
5. write_to_bucket
    - This is the final task that writes json files from the API to the S3 bucket.
    - Gets the list of dates between the start date and end date. These dates must be pendulum.datetime type and formatted as YYYY-MM-DD 
        - *** The API can only accept 1 specified date at a time (no date between queries)
    - Loop through the list of dates to get from the API
        - If the returned JSON file contains data, then write the file to the S3 Bucket as covid/report_data_YYYY-MM-DD.json
        - Else (if the returned JSON output does not have any values), then print a statement that indicates no data found

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
    - Each country should have a DAG
- File format optimization
    - Converting JSON files to parquet for optimizing storage, read operations downstream
- Use PySpark for data normalization, validation, and clean-up in curated (silver) layer
- Use DBT to handle best practices for data transformations and business (gold) layer
- Airflow params?

- {...} -> {..., "_retrieved_at": "2025-02-01"}
- {...} -> {"column": str(column)}

Links and References:
- API: https://covid-api.com/api/
- Airflow: 
    - Macros - https://airflow.apache.org/docs/apache-airflow/3.1.1/templates-ref.html
    - S3Hooks - https://airflow.apache.org/docs/apache-airflow/1.10.6/_api/airflow/hooks/S3_hook/index.html
    - Dynamic Task Mapping - https://airflow.apache.org/docs/apache-airflow/3.1.1/authoring-and-scheduling/dynamic-task-mapping.html
    - Params - https://www.astronomer.io/docs/learn/airflow-params
