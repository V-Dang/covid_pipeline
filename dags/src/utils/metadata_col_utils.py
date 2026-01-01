from datetime import datetime
import json
import pandas as pd


def inject_metadata_ts_file_key(execution_ts: datetime, json_file:list[dict], file_name:str) -> list[dict]:
    # We're not actually ingesting a json file. We are ingesting a list of dicts
    df = pd.DataFrame.from_records(json_file)

    # Add metadata columns - ingestion_ts, file_key
    df['ingested_ts'] = execution_ts
    df['row_num'] = df.index.astype(str)
    df['source_file_index'] = f'{file_name}-' + df['row_num']
    df = df.drop(columns=['row_num'])

    df_metadata = df.to_dict(orient='records')

    return df_metadata
