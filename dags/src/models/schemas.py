import datetime
from pydantic import BaseModel, Field

class CovidDataSchema(BaseModel):
    date: datetime.date
    confirmed: int
    deaths: int
    recovered: int
    confirmed_diff: int
    deaths_diff: int
    recovered_diff: int
    last_update: datetime.date
    active: int 
    active_diff: int 
    fatality_rate: float 
    region: str
    source_file_index: str
    created_ts: datetime.datetime