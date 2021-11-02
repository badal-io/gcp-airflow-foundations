from airflow import DAG
from twilio_validate_operator import BigQueryToCsv, CsvToTwilioToCsv
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

import datetime as dt

GCP_CONN = ''

PROJECT = ''
DATASET = ''
TEMP_TABLE = ''
PHONE_NUMBER_COLUMN_NAME = ''
SOURCE_TABLE = ''

FILE = '/tmp/twilio/numbers.csv'
FILE_VALIDATED = '/tmp/twilio/numbers_validated.csv'

dag = DAG("twilio_validate_csv",
    start_date=dt.datetime(2021, 11, 1),
    schedule_interval=None
)

bigquery_to_csv = BigQueryToCsv(
    task_id='bigquery_to_csv',
    bq_conn=GCP_CONN,
    query=f'SELECT {PHONE_NUMBER_COLUMN_NAME} FROM {PROJECT}.{DATASET}.{SOURCE_TABLE}',
    file=FILE,
    dag=dag
)

csv_twilio = CsvToTwilioToCsv(
    task_id='csv_twilio',
    twilio_conn='twillio_conn',
    source_file=FILE,
    target_file=FILE_VALIDATED,
    dag=dag
)

upload_GCS = LocalFilesystemToGCSOperator(
    task_id="upload_GCS",
    src=FILE_VALIDATED,
    dst='',
    bucket="badal-airflow",
    dag=dag
)

export_bigquery = GCSToBigQueryOperator(
    task_id="export_bigquery",
    bucket='badal-airflow',
    source_objects=['numbers_validated.csv'],
    source_format="CSV",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    bigquery_conn_id="gcp_conn2",
    skip_leading_rows=1,
    schema_fields=[
        {"name": "phone_number", "type": "STRING"},
        {"name": "is_valid", "type": "BOOL"}
    ],
    destination_project_dataset_table=(f'{PROJECT}:{DATASET}.{TEMP_TABLE}'),
    dag=dag,
    )

# requires storage.objects.delete access
delete_gcs = GCSDeleteObjectsOperator(
    task_id='delete_gcs',
    bucket_name='badal-airflow',
    objects=['numbers_validated.csv'],
    gcp_conn_id=GCP_CONN,
    dag=dag
)

bigquery_to_csv >> csv_twilio >> upload_GCS >> export_bigquery