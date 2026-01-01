import json
from typing import List

import logging
import requests


from .api_client import ApiClient

class ApiReader(ApiClient):
    def __init__(self, url:str):
        """Reader class to extract data from API. Inherits from ApiClient parent class.

        Args:
            url (str): URL endpoint of the API.
        """
        ApiClient.__init__(self, url)

    # for version 1
    @staticmethod
    def extract_api(url:str, params:dict=None) -> List[dict]:
        """Reads/extracts data from the API using specified parameters.

        Args:
            url (str): Base API endpoint URL.
            params (dict, optional): parameters to pass into URL.

        Returns:
            List[dict]: List of dictionaries (from json format) representing the extracted API data.
        """
        response = requests.get(f'{url}', params=params)
        print(params)
        return response.json()['data']

    # for version 2
    def read(self, region_name: str, date: str) -> List[dict]:
        """
        Reads/extracts data from the API using specified parameters (region name, date).

        Args:
            region_name (str): Country to be used in API params request
            date (str): Date in 'YYYY-MM-DD' format to be used in API params request

        Returns:
            List[dict]: List of dictionaries (from json format) representing the extracted API data.
        """

        params = {
            'region_name': region_name,
            'date': date
        }
        response = requests.get(f'{self.url}', params=params)
        # print(params)
        return response.json()['data']