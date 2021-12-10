import requests
from airflow.hooks.base import BaseHook
import logging


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

    def validate_email_bulk(self, file, job_id):
        res = self.session.post(
            f'https://api.mailgun.net/v4/address/validate/bulk/{job_id}',
            files={'file': open(file, 'rb')}
        )
        return res.json()

    def check_job_status(self, job_id):
        res = self.session.get(f'https://api.mailgun.net/v4/address/validate/bulk/{job_id}')
        logging.info(f'job status: {res.json()}')
        return res

    def download_csv(self, job_id):
        res_job = self.session.get(f'https://api.mailgun.net/v4/address/validate/bulk/{job_id}')
        csv_link = res_job.json()['download_url']['csv']
        return csv_link