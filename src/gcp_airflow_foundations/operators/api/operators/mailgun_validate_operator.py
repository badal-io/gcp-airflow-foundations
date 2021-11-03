from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from gcp_airflow_foundations.operators.api.hooks.mailgun_hook_test import MailgunHook

import logging
import os
import requests
import zipfile

def get_bigquery_hook(bq_conn: str):
    try:
        hook = BigQueryHook(bq_conn, use_legacy_sql=False)
        logging.info('BigQuery connection established')
        return hook
    except Exception as e:
        raise AirflowException(f'Connection not established: {str(e)}')

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
        outdir = '/tmp/mailgun'
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        # save dataframe to local system directory
        df.to_csv(os.path.join(outdir, self.file), index=False, header=True)

class CsvToMailgunToCsv(BaseOperator):
    @apply_defaults
    def __init__(self, *, mailgun_conn: str, source_file: str, job_id: str, **kwargs):
        super().__init__(**kwargs)
        self.hook= MailgunHook(mailgun_conn_id=mailgun_conn)
        self.source_file = source_file
        self.job_id = job_id

    def execute(self, context):
        res = self.hook.validate_email_bulk(self.source_file, self.job_id)
        logging.info(f'Mailgun job sent. Response status: {res}')

class DownloadCSV(BaseOperator):
    @apply_defaults
    def __init__(self, *, mailgun_conn: str, job_id: str, target_file: str, **kwargs):
        super().__init__(**kwargs)
        self.hook = MailgunHook(mailgun_conn_id=mailgun_conn)
        self.job_id = job_id
        self.target_file = target_file

    def execute(self, context):
        # get link for download
        link = self.hook.download_csv(self.job_id)
        print(link)

        # download file
        outdir = '/tmp/mailgun'
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        csv_zip = requests.get(link)
        with open(os.path.join(outdir, self.target_file)+'.zip', 'wb') as f:
            f.write(csv_zip.content)

        # unzip file to csv
        with zipfile.ZipFile(os.path.join(outdir, self.target_file)+'.zip', 'r') as zip_ref:
            zip_ref.extractall(os.path.join(outdir, 'validated'))