import json
import logging
import uuid
from datetime import datetime

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import (
    GoogleCloudStorageHook
)

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_framework.plugins.gcp_custom.schema_migration import SchemaMigration


class BigQueryCreateTableOperator(BaseOperator):
    """
    If this is a first-time ingestion, the operator will create
    a table in BQ with the provided schema. 

    The provided schema can be either in the form of schema_fields or as a gcs_schema_object.

    """
    template_fields = ('dataset_id', 'table_id', 'project_id',
                       'gcs_schema_object', 'labels')

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 table_id,
                 ods_metadata=None,
                 hds_metadata=None,
                 hds_table_type=None,
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
        self.hds_table_type = hds_table_type

    def execute(self, context):

        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               delegate_to=self.delegate_to)
        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        schema_migration = SchemaMigration(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id, 
            schema_fields=self.schema_fields, 
            gcs_schema_object=self.gcs_schema_object,
            column_mapping=self.column_mapping, 
            ods_metadata=self.ods_metadata,
            hds_metadata=self.hds_metadata,
            hds_table_type=self.hds_table_type 
        )

        # First check if the table exists
        if bq_hook.table_exists(project_id=self.project_id,
                                dataset_id=self.dataset_id,
                                table_id=self.table_id
                                ):

            logging.info("The table already exists in BQ")

            schema_migration.migrate_schema()

        else:
            # Create empty table
            logging.info("Creating table in BQ.")

            schema_fields = schema_migration.schema_fields

            cursor.create_empty_table(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                schema_fields=schema_fields,
                time_partitioning=self.time_partitioning,
                labels=self.labels,
                encryption_configuration=self.encryption_configuration
            )
