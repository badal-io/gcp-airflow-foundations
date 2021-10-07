import json
import logging
import uuid
from datetime import datetime

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import (
    GoogleCloudStorageHook
)

from google.cloud import bigquery

class SchemaMigrationAudit:
    """
    Inserts the the audit log rows of the schema migration operations that are executed by the MigrateSchema class. The table is created if it does not exist.
    
    :param project_id: GCP project ID  
    :type project_id: str
    :param table_id: Target table name
    :type table_id: str
    :param dataset_id: Target dataset name
    :type dataset_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority, if any
    :type delegate_to: str
    :param gcp_conn_id: Airflow GCP connection ID
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        project_id,
        dataset_id,
        table_id='schema_migration_audit_table',
        gcp_conn_id='google_cloud_default',
        delegate_to=None
    ):

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

        self.migration_id = uuid.uuid4().hex
        self.migration_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logging.info("Creating audit table in BQ.")
        self.create_audit_table()


    def insert_change_log_rows(
        
        self, 
        change_log: list
    ):
        """
        :param change_log: The schema migration operations executed by the MigrateSchema class
        :type change_log: list
        """
        
        for i in change_log:
            i['schema_updated_at'] = self.migration_time
            i['migration_id'] = self.migration_id
            
        client = bigquery.Client(project=self.project_id)

        table_ref = client.dataset(self.dataset_id).table(self.table_id)
        table = client.get_table(table_ref)

        results = client.insert_rows(table, change_log)
               

    def create_audit_table(self):
            bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
            conn = bq_hook.get_conn()
            cursor = conn.cursor()

            cursor.create_empty_table(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                schema_fields=self.__audit_table_schema_fields(),
                exists_ok=True
            )


    def __audit_table_schema_fields(self):
        return [
            {
                "name":"table_id",
                "type":"STRING"
            },
            {
                "name":"dataset_id",
                "type":"STRING"
            },
            {
                "name":"schema_updated_at",
                "type":"TIMESTAMP"
            },
            {
                "name":"migration_id",
                "type":"STRING"
            },
            {
                "name":"column_name",
                "type":"STRING"
            },
            {
                "name":"type_of_change",
                "type":"STRING"
            }
        ]
