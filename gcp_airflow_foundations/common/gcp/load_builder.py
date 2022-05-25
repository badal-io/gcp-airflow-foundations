from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

from gcp_airflow_foundations.base_class.dlp_table_config import DlpTableConfig
from gcp_airflow_foundations.base_class.source_config import SourceConfig
from gcp_airflow_foundations.base_class.source_table_config import SourceTableConfig
from gcp_airflow_foundations.operators.gcp.delete_staging_table import (
    BigQueryDeleteStagingTableOperator,
)
from gcp_airflow_foundations.operators.gcp.dlp.dlp_to_datacatalog_taskgroup import (
    schedule_dlp_to_datacatalog_taskgroup_multiple_tables,
)
from gcp_airflow_foundations.operators.gcp.hds.load_hds_taskgroup import hds_builder
from gcp_airflow_foundations.operators.gcp.ods.load_ods_taskgroup import ods_builder
from gcp_airflow_foundations.operators.gcp.schema_parsing.schema_parsing_operator import (
    ParseSchema,
)
from gcp_airflow_foundations.source_class.schema_source_config import SchemaSourceConfig
from gcp_airflow_foundations.operators.gcp.create_dataset import CustomBigQueryCreateEmptyDatasetOperator


def load_builder(
    data_source: SourceConfig,
    table_config: SourceTableConfig,
    schema_config: SchemaSourceConfig,
    preceding_task: BaseOperator,
    dag: DAG,
):

    """
    Method for building all needed Task Groups based on the HdsTableConfig and OdsTableConfig options declared in the config files
    Takes data from landing-zone/stating-area and ingests it into the Data Lake.
    """

    project_id = data_source.gcp_project
    dataset_id = data_source.dataset_data_name
    dataset_hds_id = data_source.dataset_hds_override
    landing_zone_dataset = data_source.landing_zone_options.landing_zone_dataset
    landing_zone_table_name_override = table_config.landing_zone_table_name_override
    surrogate_keys = table_config.surrogate_keys
    column_mapping = table_config.column_mapping
    column_casting = table_config.column_casting
    new_column_udfs = table_config.new_column_udfs
    ingestion_type = table_config.ingestion_type
    partition_expiration = data_source.partition_expiration
    ods_table_config = table_config.ods_config
    hds_table_config = table_config.hds_config
    location = data_source.location
    cluster_fields = table_config.cluster_fields
    dlp_table_config = DlpTableConfig()
    dlp_table_config.set_source_config(data_source.dlp_config)
    ods_suffix = data_source.ods_suffix
    hds_suffix = data_source.hds_suffix

    dag_table_id = table_config.table_name
    ods_table_id = f"{landing_zone_table_name_override}{ods_suffix}"
    if hds_table_config:
        hds_table_id = f"{landing_zone_table_name_override}{hds_suffix}"

    parse_schema = ParseSchema(
        task_id="schema_parsing",
        schema_config=schema_config,
        ods_table_id=ods_table_id,
        column_mapping=column_mapping,
        column_casting=column_casting,
        new_column_udfs=new_column_udfs,
        data_source=data_source,
        table_config=table_config,
        dag=dag,
    )

    ods_task_group = ods_builder(
        project_id=project_id,
        table_id=ods_table_id,
        dag_table_id=dag_table_id,
        dataset_id=dataset_id,
        landing_zone_dataset=landing_zone_dataset,
        landing_zone_table_name_override=landing_zone_table_name_override,
        surrogate_keys=surrogate_keys,
        column_mapping=column_mapping,
        column_casting=column_casting,
        new_column_udfs=new_column_udfs,
        ingestion_type=ingestion_type,
        cluster_fields=cluster_fields,
        partition_expiration=partition_expiration,
        ods_table_config=ods_table_config,
        location=location,
        dag=dag,
    )

    hds_task_group = None
    if hds_table_config:
        hds_table_config.table_id = f"{landing_zone_table_name_override}{hds_suffix}"

        hds_task_group = hds_builder(
            project_id=project_id,
            table_id=hds_table_id,
            dag_table_id=dag_table_id,
            dataset_id=dataset_hds_id,
            landing_zone_dataset=dataset_id,
            landing_zone_table_name_override=ods_table_id,
            surrogate_keys=surrogate_keys,
            column_mapping=column_mapping,
            column_casting=column_casting,
            new_column_udfs=new_column_udfs,
            ingestion_type=ingestion_type,
            cluster_fields=cluster_fields,
            partition_expiration=partition_expiration,
            hds_table_config=hds_table_config,
            location=location,
            dag=dag,
        )

    delete_staging_table = BigQueryDeleteStagingTableOperator(
        task_id="delete_staging_table",
        project_id=project_id,
        dataset_id=landing_zone_dataset,
        table_id=landing_zone_table_name_override,
        dag=dag,
    )

    if hds_task_group:
        preceding_task >> parse_schema >> ods_task_group >> hds_task_group >> delete_staging_table
    else:
        preceding_task >> parse_schema >> ods_task_group >> delete_staging_table

    done = DummyOperator(task_id="done", trigger_rule=TriggerRule.ALL_DONE)

    if dlp_table_config.get_is_on():

        create_dlp_dataset = CustomBigQueryCreateEmptyDatasetOperator(
            task_id="create_dlp_dataset",
            project_id=project_id,
            dataset_id=data_source.dlp_config.results_dataset_id,
            location=location,
            exists_ok=True,
            dag=dag
        )

        dlp_tasks_configs = [
            {
                "datastore": "ods",
                "project_id": project_id,
                "table_id": ods_table_id,
                "dataset_id": dataset_id,
            }
        ]
        if hds_table_config:
            dlp_tasks_configs.append(
                {
                    "datastore": "hds",
                    "project_id": project_id,
                    "table_id": hds_table_id,
                    "dataset_id": dataset_hds_id,
                }
            )

        ods_dlp_task_groups = schedule_dlp_to_datacatalog_taskgroup_multiple_tables(
            table_configs=dlp_tasks_configs,
            table_dlp_config=dlp_table_config,
            next_task=done,
            dag=dag,
        )

        delete_staging_table >> create_dlp_dataset >> ods_dlp_task_groups >> done
    else:
        delete_staging_table >> done
