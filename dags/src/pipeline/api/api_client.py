import json
from typing import List

import logging
import requests


class ApiClient():
    def __init__(self, url, params=None):
        self.url = url
        self.params = params

    def check_api_status(self) -> int:
        """
        Check the status of the api and return the status code.
        
        Args:
            **context: Airflow context dictionary containing:
                - params['Country']: Country enum in Canada, USA, China used to select which country

        Returns:
            int: The status code of the api (ex. 200, 404, etc.)
        """
        response = requests.get(self.url, params=self.params)
        return response.status_code