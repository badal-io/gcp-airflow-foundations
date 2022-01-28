import requests
from airflow.hooks.base_hook import BaseHook
from requests.auth import HTTPBasicAuth


def test_twilio_api():
    connection = BaseHook.get_connection("twilio_default")

    sid = connection.login
    secret = connection.password

    auth = HTTPBasicAuth(sid, secret)

    r = requests.get("https://api.twilio.com/2010-04-01/Accounts.json", auth=auth)

    assert r.status_code == 200
