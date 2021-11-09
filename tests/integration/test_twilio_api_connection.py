import pytest

from gcp_airflow_foundations.operators.api.hooks.twilio_hook import TwilioHook


from airflow.hooks.base_hook import BaseHook

import requests
import json
from requests.auth import HTTPBasicAuth

def test_twilio_api():
    connection = BaseHook.get_connection('twilio_default')

    sid = connection.login
    secret = connection.password

    auth = HTTPBasicAuth(sid, secret)

    r = requests.get('https://api.twilio.com/2010-04-01/Accounts.json', auth=auth)

    assert r.status_code == 200