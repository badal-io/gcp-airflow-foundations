import requests
from airflow.hooks.base import BaseHook

class MailgunHook(BaseHook):
    """
    Mailgun Hook for validating email addresses.
    API payload format example:
    {
        'address': 'zzzzzzzzz@gmail.com',
        'is_disposable_address': False,
        'is_role_address': False,
        'reason': [],
        'result': 'deliverable',
        'risk': 'low'
    }
    """
    def __init__(self, mailgun_conn_id: str) -> dict:
        self.conn = self.get_connection(mailgun_conn_id)
        self.session = requests.Session()
        self.session.auth = ('api', self.conn.login)

    def validate_email(self, email):
        res = self.session.get(
            'https://api.mailgun.net/v4/address/validate',
            params={'address': email}
        )
        return res.json()