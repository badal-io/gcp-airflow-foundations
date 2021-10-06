import pytest

from airflow_framework.operators.api.hooks.twilio_hook import TwilioHook


from airflow.hooks.base_hook import BaseHook

import requests
import json
from requests.auth import HTTPBasicAuth

def test_twilio_api():
    connection = BaseHook.get_connection('twilio_default')

    sid = connection.login
    secret = connection.password

    auth = HTTPBasicAuth(sid, secret)
    headers = {
        'Authorization': 'Basic U0s2ZWM4NGM4YmI0MzM3NTE3M2FiYTVkZWY4MTgyYTA0MTpNNWRDeTFIVThoWXRyVHpOdEx6VWZqaHk1eXkxejZQWA=='
    }

    r = requests.get('https://api.twilio.com/2010-04-01/Accounts.json', auth=auth)

    assert r.status_code == 200