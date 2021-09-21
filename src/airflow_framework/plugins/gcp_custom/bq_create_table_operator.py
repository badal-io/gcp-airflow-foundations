import json
import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import (
    GoogleCloudStorageHook
)

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from urllib.parse import urlparse


class BigQueryCreateTableOperator(BaseOperator):
    """
    If this is a first-time ingestion, the operator will create
    a table in BQ with the provided schema. 

    The provided schema can be either in the form of schema_fields or as a gcs_schema_object.

    """
    template_fields = ('dataset_id', 'table_id', 'project_id',
                       'gcs_schema_object', 'labels')

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(self,
                 dataset_id,
                 table_id,
                 ods_metadata=None,
                 hds_metadata=None,
                 project_id=None, 
                 schema_fields=None,
                 gcs_schema_object=None,
                 time_partitioning=None,
                 column_mapping=None,
                 bigquery_conn_id='google_cloud_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 labels=None,
                 encryption_configuration=None,
                 *args, **kwargs):

        super(BigQueryCreateTableOperator, self).__init__(*args, **kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.gcs_schema_object = gcs_schema_object
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.time_partitioning = {} \
            if time_partitioning is None else time_partitioning
        self.column_mapping = column_mapping
        self.labels = labels
        self.encryption_configuration = encryption_configuration
        self.ods_metadata = ods_metadata
        self.hds_metadata = hds_metadata

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               delegate_to=self.delegate_to)

        # First check if the table exists
        if bq_hook.table_exists(project_id=self.project_id,
                                dataset_id=self.dataset_id,
                                table_id=self.table_id
                                ):
            logging.info("The table already exists in BQ")
            return
        logging.info("Creating table in BQ.")

        # Create empty table
        if not self.schema_fields and self.gcs_schema_object:

            parsed_url = urlparse(self.gcs_schema_object)
            gcs_bucket = parsed_url.netloc
            gcs_object = parsed_url.path.lstrip('/')

            gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to)
            schema_fields = json.loads(gcs_hook.download(
                gcs_bucket,
                gcs_object).decode("utf-8"))
        else:
            schema_fields = self.schema_fields

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        if self.column_mapping:
            for field in schema_fields:
                field["name"] = self.column_mapping[field["name"]]

        if self.ods_metadata is not None:
            extra_fields = [
                {
                    "name": self.ods_metadata.ingestion_time_column_name,
                    "type": "TIMESTAMP"
                },
                {
                    "name": self.ods_metadata.primary_key_hash_column_name,
                    "type": "STRING"
                },
                {
                    "name": self.ods_metadata.update_time_column_name,
                    "type": "TIMESTAMP"
                },
                {
                    "name": self.ods_metadata.hash_column_name,
                    "type": "STRING"
                }
            ]
        
        elif self.hds_metadata is not None:
            extra_fields = [
                {
                    "name": self.hds_metadata.eff_start_time_column_name,
                    "type": "TIMESTAMP"
                },
                {
                    "name": self.hds_metadata.eff_end_time_column_name,
                    "type": "TIMESTAMP"
                },
                {
                    "name": self.hds_metadata.status_column_name,
                    "type": "BOOL"
                }
            ]

        else: 
            extra_fields = []   

        schema_fields.extend(extra_fields)

        cursor.create_empty_table(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            schema_fields=schema_fields,
            time_partitioning=self.time_partitioning,
            labels=self.labels,
            encryption_configuration=self.encryption_configuration
        )