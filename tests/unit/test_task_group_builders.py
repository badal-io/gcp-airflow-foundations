import pytest

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from gcp_airflow_foundations.common.gcp.ods.schema_utils import parse_ods_schema
from gcp_airflow_foundations.common.gcp.hds.schema_utils import parse_hds_schema
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.operators.gcp.hds.load_hds_taskgroup import hds_builder
from gcp_airflow_foundations.operators.gcp.ods.load_ods_taskgroup import ods_builder

class TestTaskGroupBuilder(object):
    """
    Tests that the task groups for ODS and HDS tables are loaded
    """
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
                        ingestion_type=IngestionType.INCREMENTAL,
                        ods_table_config=table.ods_config,
                        dag=dag
                    )

                    assert ods_task_group is not None, "Clould not load the ODS task group"
                
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
                        ingestion_type=IngestionType.INCREMENTAL,
                        hds_table_config=table.hds_config,
                        dag=dag
                    )

                    assert hds_task_group is not None, "Clould not load the HDS task group"