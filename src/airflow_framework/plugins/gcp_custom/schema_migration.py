import json
import logging
import uuid
from datetime import datetime

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import (
    GoogleCloudStorageHook
)

from urllib.parse import urlparse

from airflow_framework.enums.hds_table_type import HdsTableType
from airflow_framework.plugins.gcp_custom.schema_migration_audit import SchemaMigrationAudit


class SchemaMigration:

    def __init__(
        self,
        dataset_id,
        table_id,
        project_id=None, 
        schema_fields=None,
        gcs_schema_object=None,
        column_mapping=None,
        ods_metadata=None,
        hds_metadata=None,
        hds_table_type=None,
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        delegate_to=None,
        encryption_configuration=None):


        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcs_schema_object = gcs_schema_object
        self.schema_fields = schema_fields 
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.column_mapping = column_mapping
        self.encryption_configuration = encryption_configuration
        self.ods_metadata = ods_metadata
        self.hds_metadata = hds_metadata
        self.hds_table_type = hds_table_type      

        self.schema_fields = self.create_target_schema()

    def build_schema_query(self):
        """
        Schema change cases:
        1) Column type
        2) New column
        3) Deleted column
        """
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               delegate_to=self.delegate_to)
        conn = bq_hook.get_conn()
        cursor = conn.cursor()
        
        schema_fields = self.schema_fields

        schema_fields_current = cursor.get_schema(dataset_id=self.dataset_id, table_id=self.table_id).get("fields", None)

        logging.info(f"The current schema is: {schema_fields_current}")
        logging.info(f"The new schema is: {schema_fields}")

        field_names_new = [i["name"] for i in schema_fields]
        field_names_current = [i["name"] for i in schema_fields_current]

        sql_columns = []
        change_log = []
        migration_id = uuid.uuid4().hex
        migration_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for field in schema_fields_current:
            column_name = field["name"]
            column_type = field["type"]

            if column_name not in field_names_new:
                logging.info(f"Column `{column_name}` was removed from the table")
                change_log.append(
                    {
                        "table_id":self.table_id,
                        "dataset_id":self.dataset_id,
                        "schema_updated_at":migration_time,
                        "migration_id":migration_id,
                        "column_name":column_name,
                        "type_of_change":"column deletion"
                    }
                )

            else:
                field_type_new = next((i['type'] for i in schema_fields if i["name"] == column_name), None)
                if field_type_new != column_type:
                    logging.info(f"Data type of column `{column_name}` was changed from {column_type} to {field_type_new}")

                    change_log.append(
                        {
                            "table_id":self.table_id,
                            "dataset_id":self.dataset_id,
                            "schema_updated_at":migration_time,
                            "migration_id":migration_id,
                            "column_name":column_name,
                            "type_of_change":"data type change"
                        }
                    )

                    sql_columns.append(
                        f"""CAST(`{column_name}` AS {self.bigQuery_mapping(field_type_new)}) AS `{column_name}`"""
                    )
                else:
                    sql_columns.append(f"""`{column_name}`""")

        for field in schema_fields:
            column_name = field["name"]
            column_type = field["type"]

            if column_name not in field_names_current:
                logging.info(f"{column_name} was added to the table")

                change_log.append(
                    {
                        "table_id":self.table_id,
                        "dataset_id":self.dataset_id,
                        "schema_updated_at":migration_time,
                        "migration_id":migration_id,
                        "column_name":column_name,
                        "type_of_change":"column addition"
                    }
                )

                sql_columns.append(
                    f"""CAST(NULL AS {column_type}) AS `{column_name}`"""
                )

        query = f"""
                SELECT
                    {",".join(sql_columns)}
                FROM
                    `{self.dataset_id}.{self.table_id}`
            """

        return query, change_log, sql_columns
    
    def migrate_schema(self):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               delegate_to=self.delegate_to)
        conn = bq_hook.get_conn()
        cursor = conn.cursor()
        
        query, change_log, sql_columns = self.build_schema_query()
        
        if change_log:
            logging.info("Migrating new schema to target table")

            query = f"""
                SELECT
                    {",".join(sql_columns)}
                FROM
                    `{self.dataset_id}.{self.table_id}`
            """

            cursor.run_query(
                sql=query,
                use_legacy_sql=False,
                destination_dataset_table=f"{self.dataset_id}.{self.table_id}",
                write_disposition="WRITE_TRUNCATE"
            )

            SchemaMigrationAudit(
                project_id=self.project_id,
                dataset_id=self.dataset_id
            ).insert_change_log_rows(change_log)
        
        else:
            logging.info("No schema changes detected")
            

    def create_target_schema(self):
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
        
        elif self.hds_metadata is not None and self.hds_table_type == HdsTableType.SCD2:
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
                },
                {
                    "name": self.hds_metadata.hash_column_name,
                    "type": "STRING"
                }
            ]

        elif self.hds_metadata is not None and self.hds_table_type == HdsTableType.SNAPSHOT:
            extra_fields = [
                {
                    "name": self.hds_metadata.eff_start_time_column_name,
                    "type": "TIMESTAMP"
                },
                {
                    "name": self.hds_metadata.hash_column_name,
                    "type": "STRING"
                }
            ]

        else: 
            extra_fields = []   

        schema_fields.extend(extra_fields)

        return schema_fields


    def bigQuery_mapping(self, data_type):
        mapping = {
            "FLOAT":"FLOAT64"
        }

        if data_type in mapping.keys():
            return mapping[data_type]
        else:
            return data_type    