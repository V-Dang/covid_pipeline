import json
from typing import List

import logging
import requests


from .api_client import ApiClient

class ApiExtractor(ApiClient):
    def __init__(self, url):
        ApiClient.__init__(self, url)

    # for version 2
    def extract(self, params=None) -> List[dict]:
        response = requests.get(f'{self.url}', params=params)
        print(params)
        return response.json()['data']

    # for version 1
    @staticmethod
    def extract_api(url, params=None) -> List[dict]:
        response = requests.get(f'{url}', params=params)
        print(params)
        return response.json()['data']