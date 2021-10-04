from typing import Optional

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
    BigQueryCreateEmptyTableOperator,
)

from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from airflow.exceptions import AirflowException

import logging

from airflow_framework.operators.gcp.schema_migration.schema_migration_audit import SchemaMigrationAudit


class MigrateSchema(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id,
        table_id,
        project_id, 
        new_schema_fields,
        gcp_conn_id='google_cloud_default',
        delegate_to=None,
        encryption_configuration=None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.new_schema_fields = new_schema_fields 
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.encryption_configuration = encryption_configuration

    def execute(self, context):
        query, schema_fields_updates, sql_columns, change_log = self.build_schema_query()

        if change_log:
            logging.info("Migrating new schema to target table")

            if sql_columns:
                cursor.run_query(
                    sql=query,
                    use_legacy_sql=False,
                    destination_dataset_table=f"{self.dataset_id}.{self.table_id}",
                    write_disposition="WRITE_TRUNCATE"
                )

            if schema_fields_updates:
                hook.update_table_schema(
                    dataset_id=self.dataset_id, 
                    table_id=self.table_id,
                    schema_fields_updates=schema_fields_updates,
                    include_policy_tags=False
                )

            SchemaMigrationAudit(
                project_id=self.project_id,
                dataset_id=self.dataset_id
            ).insert_change_log_rows(change_log)
        
        else:
            logging.info("No schema changes detected")


    def build_schema_query(self):
        """
        Schema change cases:
        1) Column type
        2) New column
        3) Deleted column
        """
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
        )
        conn = hook.get_conn()
        cursor = conn.cursor()

        self.current_schema_fields = cursor.get_schema(dataset_id=self.dataset_id, table_id=self.table_id).get("fields", None)
        
        logging.info(f"The current schema is: {self.current_schema_fields}")

        logging.info(f"The new schema is: {self.new_schema_fields}")
        
        column_names_new = [i["name"] for i in self.new_schema_fields]
        column_names_current = [i["name"] for i in self.current_schema_fields]

        sql_columns = []
        schema_fields_updates = []
        change_log = []

        for field in self.current_schema_fields:
            column_name = field["name"]
            column_type = field["type"]
            column_mode = field["mode"]

            if (column_name not in column_names_new) and (column_mode == "REQUIRED"):
                logging.info(f"Column `{column_name}` was changed from REQUIRED to NULLABLE")

                change_log.append(
                    {
                        "table_id":self.table_id,
                        "dataset_id":self.dataset_id,
                        "column_name":column_name,
                        "type_of_change":"column mode relaxation"
                    }
                )

                schema_fields_updates.append(
                    {"name":column_name,"mode":"NULLABLE"}
                )

                sql_columns.append(f"""`{column_name}`""")

            else:
                column_type_new = self.bigQuery_mapping(next((i['type'] for i in self.new_schema_fields if i["name"] == column_name), None))
                if (column_type_new is not None) and (column_type_new != column_type):
                    self.allowed_casting(column_name, column_type, column_type_new)

                    logging.info(f"Data type of column `{column_name}` was changed from {column_type} to {column_type_new}")

                    change_log.append(
                        {
                            "table_id":self.table_id,
                            "dataset_id":self.dataset_id,
                            "column_name":column_name,
                            "type_of_change":"data type change"
                        }
                    )

                    sql_columns.append(
                        f"""CAST(`{column_name}` AS {column_type_new}) AS `{column_name}`"""
                    )
                else:
                    sql_columns.append(f"""`{column_name}`""")

        for field in self.new_schema_fields:
            column_name = field["name"]
            column_type = field["type"]

            if column_name not in column_names_current:
                logging.info(f"{column_name} was added to the table")

                change_log.append(
                    {
                        "table_id":self.table_id,
                        "dataset_id":self.dataset_id,
                        "column_name":column_name,
                        "type_of_change":"column addition"
                    }
                )

                schema_fields_updates.append(
                    {"name":column_name,"mode":"NULLABLE","type":column_type}
                )

        query = f"""
                SELECT {",".join(sql_columns)} FROM `{self.dataset_id}.{self.table_id}`;
            """

        return query, schema_fields_updates, sql_columns, change_log


    def bigQuery_mapping(self, data_type):
        mapping = {
            "FLOAT":"FLOAT64"
        }

        if data_type in mapping:
            return mapping[data_type]
        else:
            return data_type    

    def allowed_casting(self, column_name, current_data_type, new_data_type):
        casting = {
            "INTEGER":["BOOL","INTEGER","NUMERIC","BIGNUMERIC","FLOAT64","STRING"],
            "NUMERIC":["INTEGER","NUMERIC","BIGNUMERIC","FLOAT64","STRING"],
            "BIGNUMERIC":["INTEGER","NUMERIC","BIGNUMERIC","FLOAT64","STRING"],
            "FLOAT":["INTEGER","NUMERIC","BIGNUMERIC","FLOAT64","STRING"],
            "BOOL":["BOOL","INTEGER","STRING"],
            "STRING":["BOOL","INTEGER","NUMERIC","BIGNUMERIC","FLOAT64","STRING","BYTES","DATE","DATETIME","TIME","TIMESTAMP"],
            "BYTES":["STRING","BYTES"],
            "DATE":["STRING","DATE","DATETIME","TIMESTAMP"],
            "DATETIME":["STRING","DATE","DATETIME","TIME","TIMESTAMP"],
            "TIME":["STRING","TIME"],
            "TIMESTAMP":["STRING","DATE","DATETIME","TIME","TIMESTAMP"],
            "ARRAY":["ARRAY"],
            "STRUCT":["STRUCT"]
        }

        allowed_casting = casting[current_data_type]

        if new_data_type not in allowed_casting:
            AirflowException("Invalid casting type", f"Data type of column {column_name} cannot be changed from {current_data_type} to {current_data_type}")