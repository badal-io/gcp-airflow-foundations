from airflow import DAG
from mailgun_validate_operator import BigQueryToCsv, CsvToMailgunToCsv, DownloadCSV
from mailgun_validate_sensor import MailgunValidateSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

import datetime as dt

GCP_CONN = 'gcp_conn' # requires storage.objects.delete access

PROJECT = ''
DATASET = ''
TABLE = ''
COLUMN = 'email' # mailgun requires column name to be named 'email'
BUCKET = ''

FILE_PATH = '/tmp/mailgun/'
FILE = 'emails.csv'
FILE_VALIDATED = 'email_validated.csv'

JOB_ID = ''

dag = DAG("mailgun_validate_csv",
    start_date=dt.datetime(2021, 10, 21),
    schedule_interval=None
)

# custom operator: get list of emails in csv format from bigquery and save to local FS
bigquery_to_csv = BigQueryToCsv(
    task_id='bigquery_to_csv',
    bq_conn=GCP_CONN,
    query=f'SELECT {COLUMN} FROM {PROJECT}.{DATASET}.{TABLE}',
    file=FILE,
    dag=dag
)

# send list of emails in csv to Mailgun's bulk validation API
validate_with_mailgun = CsvToMailgunToCsv(
    task_id='csv_mailgun',
    mailgun_conn='mailgun_conn',
    source_file=FILE_PATH+FILE,
    job_id=JOB_ID,
    dag=dag
)

# sensor checking if job has been completed and csv is ready for download
wait_for_validation = MailgunValidateSensor(
    task_id="wait_for_validation",
    mailgun_conn='mailgun_conn',
    job_id=JOB_ID,
    dag=dag
)

# download email validation csv file from Mailgun's API
download_csv = DownloadCSV(
    task_id="download_csv",
    mailgun_conn='mailgun_conn',
    job_id=JOB_ID,
    target_file=FILE_PATH+FILE_VALIDATED,
    dag=dag
)

# upload csv file to GCS
upload_GCS = LocalFilesystemToGCSOperator(
    task_id="upload_GCS",
    src=FILE_PATH+'/validated/*.csv',
    dst='mailgun-validated.csv',
    bucket=BUCKET,
    dag=dag
)

# export CSV file from GCS to Bigquery
export_bigquery = GCSToBigQueryOperator(
    task_id="export_bigquery",
    bucket=BUCKET,
    source_objects=['mailgun-validated.csv'],
    source_format="CSV",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    bigquery_conn_id="gcp_conn2",
    skip_leading_rows=1,
    schema_fields=[
        {"name": "address", "type": "STRING"},
        {"name": "is_role_address", "type": "BOOL"},
        {"name": "is_disposable_address", "type": "BOOL"},
        {"name": "did_you_mean", "type": "STRING"},
        {"name": "result", "type": "STRING"},
        {"name": "reason", "type": "STRING"},
        {"name": "risk", "type": "STRING"},
        {"name": "root_address", "type": "STRING"}
    ],
    destination_project_dataset_table=(f'{PROJECT}:{DATASET}.email_mailgun_validation'),
    dag=dag,
    )

# delete GCS bucket after exporting to BigQuery
delete_gcs = GCSDeleteObjectsOperator(
    task_id='delete_gcs',
    bucket_name=BUCKET,
    objects=['mailgun-validated.csv'],
    gcp_conn_id=GCP_CONN,
    dag=dag
)

bigquery_to_csv >> validate_with_mailgun >> wait_for_validation >> download_csv >> upload_GCS >> export_bigquery >> delete_gcs