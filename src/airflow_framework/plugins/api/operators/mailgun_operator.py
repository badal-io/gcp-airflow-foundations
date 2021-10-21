from airflow.models import BaseOperator
from mailgun_hook_test import MailgunHook

class EmailValidationOperator(BaseOperator):
    """
    Custom operator for email validation using Mailgun Hook.

    Attributes:
        mailgun_conn_id
        emails list[str]
    """
    def __init__(self, mailgun_conn_id, emails, *args, **kwargs):
        super(EmailValidationOperator, self).__init__(*args, **kwargs)
        self.mailgun_conn_id = mailgun_conn_id
        self.emails = emails

    def get_hook(self):
        return MailgunHook(mailgun_conn_id=self.mailgun_conn_id)

    def execute(self, context):
        self.mailgun_hook = self.get_hook()

        results = []
        for email in self.emails:
            res = self.mailgun_hook.validate_email(email)
            results.append(res)
        return results