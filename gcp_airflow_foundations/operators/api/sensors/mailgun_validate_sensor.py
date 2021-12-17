from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from gcp_airflow_foundations.operators.api.hooks.mailgun_hook_test import MailgunHook
from typing import Any

class MailgunValidateSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, mailgun_conn: str, job_id: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.hook = MailgunHook(mailgun_conn_id=mailgun_conn)

    def poke(self, context):
        res = self.hook.check_job_status(self.job_id)
        return 'download_url' in res.json()