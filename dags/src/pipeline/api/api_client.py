import json
from typing import List

import logging
import requests


class ApiClient():
    def __init__(self, url:str, params:dict=None):
        """Simple HTTP client for GET-based APIs.

        Args:
            url (str): Base url of the api
            params (dict, optional): Dict of parameters to pass into the api (ex. region_name, date). Defaults to None.
        """
        self.url = url
        self.params = params
        # Use api sessions?

    def check_api_status(self) -> int:
        """
        Check the status of the api and return the status code.

        Returns:
            int: The status code of the api (ex. 200, 404, etc.)
        """
        response = requests.get(self.url, params=self.params)
        return response.status_code