import pytest
import logging
from datetime import datetime

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow import DAG

from airflow_framework.common.gcp.source_schema.gcs import read_schema_from_gcs
from airflow_framework.common.gcp.ods.schema_utils import parse_ods_schema
from airflow_framework.common.gcp.hds.schema_utils import parse_hds_schema
from airflow_framework.enums.ingestion_type import IngestionType
from airflow_framework.enums.hds_table_type import HdsTableType
from airflow_framework.operators.gcp.schema_migration.schema_migration_operator import MigrateSchema
from airflow_framework.operators.gcp.ods.ods_sql_upsert_helpers import SqlHelperODS
from airflow_framework.operators.gcp.hds.hds_sql_upsert_helpers import SqlHelperHDS
from airflow_framework.operators.gcp.hds.load_hds_taskgroup import hds_builder
from airflow_framework.operators.gcp.ods.load_ods_taskgroup import ods_builder

class TestGBQConnectorIntegration(object):
    def test_should_connect_to_gcp(self):
        gcp_conn_id='google_cloud_default'
        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
        assert bq_hook.get_conn() is not None, "Could not establish a connection to BigQuery"
    

class TestSchemaParsing(object):
    def test_should_read_from_gcs(self, config):
        for table in config.tables:
            source_schema_fields = read_schema_from_gcs(
                gcs_schema_object=table.source_table_schema_object,
                schema_fields=None,
                column_mapping=table.column_mapping
            )
            assert source_schema_fields is not None, "Could not read source schema from GCS"

    def test_should_parse_ods_schema(self, config):
        for table in config.tables:
            if table.ods_config:
                schema_fields, _ = parse_ods_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,
                    ods_metadata=table.ods_config.ods_metadata
                )
                assert schema_fields is not None, "Could not parse ODS schema fields"

    def test_should_parse_hds_schema(self, config): 
        for table in config.tables:
            if table.hds_config:
                schema_fields, _ = parse_hds_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,            
                    hds_metadata=table.hds_config.hds_metadata,
                    hds_table_type=table.hds_config.hds_table_type
                )
                assert schema_fields is not None, "Could not parse HDS schema fields"         
    

class TestSqlHelpers(object):
    def setup(self):
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        bq_conn = bq_hook.get_conn()
        self.bq_cursor = bq_conn.cursor()
        
    def test_sql_in_bq(self, staging_dataset, target_dataset, config):
        for table in config.tables:
            if table.ods_config:

                if table.ods_config.ingestion_type == IngestionType.INCREMENTAL:
                    table_id = f"{table.table_name}_ODS_Incremental"

                elif table.ods_config.ingestion_type == IngestionType.FULL:
                    table_id = f"{table.table_name}_ODS_Full"

                schema_fields, columns = parse_ods_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,
                    ods_metadata=table.ods_config.ods_metadata
                )

                sql_helper = SqlHelperODS(
                    source_dataset=staging_dataset,
                    target_dataset=target_dataset,
                    source=table.table_name,
                    target=table_id,
                    columns=columns,
                    surrogate_keys=table.surrogate_keys,
                    column_mapping=table.column_mapping,
                    ods_metadata=table.ods_config.ods_metadata
                )

                sql = sql_helper.create_upsert_sql_with_hash()

            elif table.hds_config:

                if table.hds_config.hds_table_type == HdsTableType.SNAPSHOT:
                    table_id = f"{table.table_name}_HDS_Snapshot"

                elif table.hds_config.hds_table_type == HdsTableType.SCD2:
                    table_id = f"{table.table_name}_HDS_SCD2"

                schema_fields, _ = parse_hds_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,            
                    hds_metadata=table.hds_config.hds_metadata,
                    hds_table_type=table.hds_config.hds_table_type
                )        

                sql_helper = SqlHelperHDS(
                    source_dataset=staging_dataset,
                    target_dataset=target_dataset,
                    source=table.table_name,
                    target=table_id,
                    columns=columns,
                    surrogate_keys=table.surrogate_keys,
                    column_mapping=table.column_mapping,
                    hds_metadata=table.hds_config.hds_metadata
                )

                sql = sql_helper.create_scd2_sql_with_hash()

            # Execute dry-run query job in BigQuery to test SQL
            output = self.bq_cursor.run_with_configuration({'query':{"query": sql,'useQueryCache':False,'useLegacySql':False,'dryRun':True}})
            assert output is not None, f"Could not execute merge SQL query in BigQuery: {sql}"


class TestTaskGroupBuilder(object):
    def test_should_load_ods_task_group(self, test_dag, config, project_id, staging_dataset, target_dataset):
        data_source = config.source

        for table in config.tables:
            if table.ods_config:
                schema_fields, columns = parse_ods_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,
                    ods_metadata=table.ods_config.ods_metadata
                )

                with test_dag as dag:

                    ods_task_group = ods_builder(
                        project_id=project_id,
                        table_id=table.table_name,
                        dataset_id=target_dataset,
                        landing_zone_dataset=staging_dataset,
                        landing_zone_table_name_override=table.table_name,
                        surrogate_keys=table.surrogate_keys,
                        column_mapping=table.column_mapping,
                        columns=columns,
                        schema_fields=schema_fields,
                        ods_table_config=table.ods_config,
                        dag=dag
                    )

                    assert ods_task_group is not None
                
    def test_should_load_hds_task_group(self, test_dag, config, project_id, staging_dataset, target_dataset):
        data_source = config.source

        for table in config.tables:
            if table.hds_config:
                schema_fields, columns = parse_hds_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,
                    hds_metadata=table.hds_config.hds_metadata,
                    hds_table_type=table.hds_config.hds_table_type
                )

                with test_dag as dag:

                    hds_task_group = hds_builder(
                        project_id=project_id,
                        table_id=table.table_name,
                        dataset_id=target_dataset,
                        landing_zone_dataset=staging_dataset,
                        landing_zone_table_name_override=table.table_name,
                        surrogate_keys=table.surrogate_keys,
                        column_mapping=table.column_mapping,
                        columns=columns,
                        schema_fields=schema_fields,
                        hds_table_config=table.hds_config,
                        dag=dag
                    )

                    assert hds_task_group is not None
                

class TestSchemaMigrationOperator(object):
    def test_should_pick_columns_for_schema_migration(self, project_id, target_dataset, config):
        for table in config.tables:
            if table.ods_config:

                if table.ods_config.ingestion_type == IngestionType.INCREMENTAL:
                    table_id = f"{table.table_name}_ODS_Incremental"

                elif table.ods_config.ingestion_type == IngestionType.FULL:
                    table_id = f"{table.table_name}_ODS_Full"

                schema_fields, _ = parse_ods_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,
                    ods_metadata=table.ods_config.ods_metadata
                )

            elif table.hds_config:
                if table.hds_config.hds_table_type == HdsTableType.SNAPSHOT:
                    table_id = f"{table.table_name}_HDS_Snapshot"

                elif table.hds_config.hds_table_type == HdsTableType.SCD2:
                    table_id = f"{table.table_name}_HDS_SCD2"

                schema_fields, _ = parse_hds_schema(
                    gcs_schema_object=table.source_table_schema_object,
                    schema_fields=None,
                    column_mapping=table.column_mapping,            
                    hds_metadata=table.hds_config.hds_metadata,
                    hds_table_type=table.hds_config.hds_table_type
                )        

            migrate_schema = MigrateSchema(
                task_id="schema_migration",
                project_id=project_id,
                table_id=table_id,
                dataset_id=target_dataset, 
                new_schema_fields=schema_fields
            )

            query, schema_fields_updates, sql_columns, change_log = migrate_schema.build_schema_query()

            assert sql_columns is not None, "Could not validate MigrateSchema operator"     