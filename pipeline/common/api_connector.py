import os
from datetime import datetime
from io import StringIO
import json
import pandas as pd
from dotenv import load_dotenv
import requests

class ApiConnector:

    def __init__(
        self, now: datetime,
        data_endpoint: str,
        token_endpoint: str,
        grant_type: str,
        content_type: str):

        load_dotenv()      
        self.now = now
        self.data_endpoint = data_endpoint
        self.token_endpoint = token_endpoint
        self.token = None
        self.token_parameters = {
            "grant_type": grant_type,
            "userName": os.getenv('SAMTECH_API_USERNAME'),
            "password": os.getenv('SAMTECH_API_PASSWORD')
        }
        self.config = { "Content-Type": content_type }


    def get_token(self):
        token_response = requests.post(
            self.token_endpoint,
            self.token_parameters,
            self.config
        )

        if token_response.status_code == 200:

            io = StringIO(token_response.text)
            self.token = json.load(io)

        else:

            print('Token Error: ', token_response.text)

    def get_data_response(self, recursive_call = False):

        self.check_valid_token()

        request_header = {
            "Authorization": ''.join(["Bearer ", self.token["access_token"]])
        }

        api_response = requests.get(
            self.data_endpoint,
            headers=request_header
        )

        if api_response.status_code == 200:

            io = StringIO(api_response.text)
            data = json.load(io)[0]
            data = data['Datos']['Detalle']

            return data

        elif api_response.status_code == 401:

            if not recursive_call:
                self.check_valid_token()
                data = self.get_data_response(recursive_call = True)
                return data

            else:
                print('API Error: ', api_response.text)

        else:
            print('API Error: ', api_response.text)


    def check_valid_token(self):

        try:

            valid_datetime = pd.to_datetime(self.token['.expires'])

            if valid_datetime <= self.now:
                self.get_token()

        except:

            self.get_token()







