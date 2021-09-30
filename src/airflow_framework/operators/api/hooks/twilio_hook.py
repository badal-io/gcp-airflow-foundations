import requests
import time
from datetime import datetime
import json
from requests.auth import HTTPBasicAuth

import hashlib

from airflow.hooks.base_hook import BaseHook

class TwilioHook(BaseHook):
    """
    Airflow Hook to connect to Twilio's API and retrieve Text Message data
    """
    def __init__(self, twilio_conn_id='twilio_default') -> None:
        self.twilio_conn_id = twilio_conn_id,
        self.base_uri = 'https://api.twilio.com/2010-04-01'

        connection = self.get_connection(twilio_conn_id)
        self.sid = connection.login
        self.secret = connection.password

    def transform_text_messages(
        self,
        text_messages
    ) -> list:

        rows = []
        for i in text_messages:
            i['feedback'] = i['subresource_uris']['feedback'] 
            i['media'] = i['subresource_uris']['media']
            i.pop('subresource_uris', None)

            i['date_created'] = datetime.strptime(i['date_created'], '%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S')
            i['date_sent'] = datetime.strptime(i['date_sent'], '%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S')
            i['date_updated'] = datetime.strptime(i['date_updated'], '%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S')

            keys = ['sid']

            joined_keys = ''.join([i[key] for key in keys])

            rows.append(
                {
                    'insertId':hashlib.md5(joined_keys.encode('utf-8')).hexdigest(),
                    'json':i
                    }
            )
        
        return rows

    def get_text_messages(
        self,
        account_sid,
        FromDate=None,
        ToDate=None
    ) -> list:

        URL = f'{self.base_uri}/Accounts/{account_sid}/Messages.json'

        auth = HTTPBasicAuth(self.sid, self.secret)

        params = {
            'PageSize':1000
        }

        if FromDate:
            params['DateSent>'] = FromDate

        if ToDate:
            params['DateSent<'] = ToDate

        text_messages = []

        while True:
            time.sleep(1)

            r = requests.get(URL, params=params, auth=auth)
            r.raise_for_status()

            body = r.json()
            text_messages.extend(body['messages'])

            NextPageURI = body.get('next_page_uri', None)
            if not NextPageURI:
                break

            URL = f'https://api.twilio.com/{NextPageURI}'

        rows = self.transform_text_messages(text_messages)

        return rows