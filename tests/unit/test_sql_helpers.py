import pytest

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow_framework.common.gcp.ods.schema_utils import parse_ods_schema
from airflow_framework.common.gcp.hds.schema_utils import parse_hds_schema
from airflow_framework.enums.ingestion_type import IngestionType
from airflow_framework.enums.hds_table_type import HdsTableType
from airflow_framework.operators.gcp.ods.ods_sql_upsert_helpers import SqlHelperODS
from airflow_framework.operators.gcp.hds.hds_sql_upsert_helpers import SqlHelperHDS


class TestSqlHelpers(object):
    """
    Assert that the SQL queries used for updating the ODS and HDS tables are run without erros on BigQuery using the dry-run query configuration
    """
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

