import os
import pytest

from airflow import DAG
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import test_cycle

import logging

from airflow_framework.parse_dags import DagParser
from airflow_framework.plugins.gcp_custom.schema_migration import SchemaMigration

def test_schema_migration(test_configs):
    for config in test_configs:
        data_source = config.source
        for table_config in config.tables: 
            logging.info(f"Table name is: {table_config.table_name}") 
            schema_migration = SchemaMigration(
                project_id=data_source.gcp_project,
                dataset_id=data_source.dataset_data_name,
                table_id=table_config.table_name, 
                schema_fields=None, 
                gcs_schema_object=table_config.source_table_schema_object,
                column_mapping=table_config.column_mapping, 
                ods_metadata=table_config.ods_metadata,
                hds_metadata=table_config.hds_metadata,
                hds_table_type=table_config.hds_table_type 
            )

            schema = schema_migration.schema_fields

            if table_config.table_name == 'raw_measurements_migrate':  
                assert schema == [
                    {'name': 'datetime', 'type': 'TIMESTAMP'}, 
                    {'name': 'sensorID', 'type': 'STRING'}, 
                    {'name': 'property', 'type': 'STRING'}, 
                    {'name': 'value', 'type': 'FLOAT'}, 
                    {'name': 'UOM', 'type': 'STRING'}, 
                    {'name': 'device_version', 'type': 'STRING'}, 
                    {'name': 'test_column', 'type': 'STRING'}, 
                    {'name': 'af_metadata_inserted_at', 'type': 'TIMESTAMP'}, 
                    {'name': 'af_metadata_primary_key_hash', 'type': 'STRING'}, 
                    {'name': 'af_metadata_updated_at', 'type': 'TIMESTAMP'}, 
                    {'name': 'af_metadata_row_hash', 'type': 'STRING'}]

                query, change_log, sql_columns = schema_migration.build_schema_query()

            if table_config.table_name == 'raw_measurements_staging':  
                assert schema == [
                    {'name': 'datetime', 'type': 'STRING'}, 
                    {'name': 'sensorID', 'type': 'STRING'}, 
                    {'name': 'property', 'type': 'STRING'}, 
                    {'name': 'value', 'type': 'STRING'}, 
                    {'name': 'UOM', 'type': 'STRING'}, 
                    {'name': 'device_version', 'type': 'STRING'}, 
                    {'name': 'af_metadata_inserted_at', 'type': 'TIMESTAMP'}, 
                    {'name': 'af_metadata_primary_key_hash', 'type': 'STRING'}, 
                    {'name': 'af_metadata_updated_at', 'type': 'TIMESTAMP'}, 
                    {'name': 'af_metadata_row_hash', 'type': 'STRING'}]

                query, change_log, sql_columns = schema_migration.build_schema_query()

            