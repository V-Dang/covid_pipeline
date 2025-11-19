import json
from typing import List

import logging
import requests


from dags.src.pipeline.api.api_client import ApiClient

class ApiExtractor(ApiClient):
    def __init__(self, url, params=None):
        self.url = url
        self.params = params

    def extract(self) -> List[dict]:
        response = requests.get(f'{self.url}', params=self.params)
        return response.json()['data']