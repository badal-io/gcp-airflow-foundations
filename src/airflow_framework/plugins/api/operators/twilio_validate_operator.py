from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from twilio.rest import Client
import logging
import os


def get_twilio_client(conn_id):
    try:
        connection = BaseHook.get_connection(conn_id)
        account_sid = connection.login
        auth_token = connection.password
        logging.info('Twilio connection established')
        return Client(account_sid, auth_token)
    except Exception as e:
        raise AirflowException(f'Twilio connection not established: {e}')


def get_bigquery_hook(bq_conn):
    try:
        hook = BigQueryHook(bq_conn, use_legacy_sql=False)
        # conn = hook.get_conn()
        logging.info('BigQuery connection established')
        return hook
    except Exception as e:
        raise AirflowException(f'Connection not estabilished: {str(e)}')


class BigQueryToCsv(BaseOperator):
    @apply_defaults
    def __init__(self, *, bq_conn: str, query: str, file: str, **kwargs):
        super().__init__(**kwargs)

        self.bq_hook = get_bigquery_hook(bq_conn)
        self.query = query
        self.file = file

    def execute(self, context):
        # Grab phone_number column from BigQuery source table as a pandas dataframe
        df = self.bq_hook.get_pandas_df(self.query)

        # Check for directory in local FS. If it doesn't exist, create one.
        outdir = '/tmp/twilio'
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        # save dataframe to local system directory
        df.to_csv(os.path.join(outdir, self.file), index=False, header=False)

class CsvToTwilioToCsv(BaseOperator):
    @apply_defaults
    def __init__(self, *, twilio_conn: str, source_file: str, target_file: str, **kwargs):
        super().__init__(**kwargs)
        self.twilio_client = get_twilio_client(twilio_conn)
        self.source_file = source_file
        self.target_file = target_file

    def execute(self, context):
        with open(self.target_file, 'a+') as target:
            with open(self.source_file, 'r') as source:
                for num in source:
                    try:
                        self.twilio_client.lookups.v1.phone_numbers(num).fetch(country_code='US')
                        target.write(f'{num.rstrip()},True\n')
                    except Exception:
                        target.write(f'{num.rstrip()},False\n')