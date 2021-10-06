import pytest

from airflow_framework.common.gcp.ods.schema_utils import parse_ods_schema
from airflow_framework.common.gcp.hds.schema_utils import parse_hds_schema
from airflow_framework.enums.ingestion_type import IngestionType
from airflow_framework.enums.hds_table_type import HdsTableType
from airflow_framework.operators.gcp.schema_migration.schema_migration_operator import MigrateSchema


class TestSchemaMigrationOperator(object):
    """
    Assert that the SchemaMigration Operator successfully returns a list of SQL statemets for migrating the schema of the test table columns
    """
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

            query, schema_fields_updates, sql_column_statements, change_log = migrate_schema.build_schema_query()

            assert sql_column_statements is not None, "Could not validate MigrateSchema operator"     