# This is where I'm putting my dead functions. Used for testing, thought processes, etc.

def get_last_execution_date(prev_start_date_success, **kwargs):
    if prev_start_date_success in [None, "None", "", "null"]:               # Or if no json file with extracted date in name in s3 bucket
        last_run_date = pendulum.now().subtract(years=5).format('YYYY-MM-DD')       # Artifically set last execution date to 5 years ago to ingest covid data
        print(f'First Run: {last_run_date}')
    else:
        last_run_date = (pendulum.parse(prev_start_date_success)).subtract(years=5).format('YYYY-MM-DD')
        print(f'Last Run: {last_run_date}')
    
    # Save the last execution date as an xcom to be used to pull data from API
    kwargs['ti'].xcom_push(
        key='last_execution',
        value=last_run_date
    )


def fetch_covid_data(current_date: str):

    # filename = kwargs['ti'].xcom_pull

    url = f"https://covid-api.com/api/reports?date={current_date}"
    response = requests.get(url)
    data = response.json()['data']
    # data = json.dumps(response.json(), indent=2)

    # with NamedTemporaryFile(mode='w', suffix=filename) as f:
    #     temp_data = json.dump(data)
    #     temp_data.flush()
    #     # content = json.loads(temp_data)

    logging.info(f"Fetched {len(data)} records for {current_date}")
        
    return data

    # print(data)

def json_to_s3(filename: str, data: str, current_date: str) -> None:
    with NamedTemporaryFile(mode='w', suffix=filename) as f:
        json.dumps(data)

        logging.info(f"Fetched {len(data)} records for {current_date}")

        s3_hook = S3Hook(aws_conn_id='aws_bucket')
        s3_hook.load_file(
            filename=f,                  # Path to json file (source)
            key=filename,                       # Path in container (target)
            bucket_name=s3_bucket_name,
            replace=True
            )    
        logging.info(f'Order data file has been saved to AWS bucket: {filename}')


        # def fetch_covid_api(region: str, date: date) -> dict
            # check status code
            # check ..
            # validate
            # return data
        # fetch_covid_api(date=d, region="Canada") 

    # def is_good_covid_data() -> True / False
        # if true: > 'covid/report_data'
        # if false > 'errors/covid/report_data'