from gcp_airflow_foundations.operators.gcp.hds.load_hds_taskgroup import hds_builder
from gcp_airflow_foundations.operators.gcp.ods.load_ods_taskgroup import ods_builder

from gcp_airflow_foundations.operators.gcp.schema_parsing.schema_parsing_operator import ParseSchema
from gcp_airflow_foundations.enums.ingestion_type import IngestionType
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType

import logging


def load_builder(
    data_source,
    table_config,
    schema_config,
    preceding_task,
    dag):

    """
    Method for building all needed Task Groups based on the HdsTableConfig and OdsTableConfig options declared in the config files
    """

    project_id = data_source.gcp_project
    table_id = table_config.table_name
    dataset_id = data_source.dataset_data_name
    landing_zone_dataset = data_source.landing_zone_options.landing_zone_dataset
    landing_zone_table_name_override = table_config.landing_zone_table_name_override
    surrogate_keys = table_config.surrogate_keys
    column_mapping = table_config.column_mapping
    ingestion_type = table_config.ingestion_type
    partition_expiration = data_source.partition_expiration
    ods_table_config = table_config.ods_config
    hds_table_config = table_config.hds_config

    if ingestion_type == IngestionType.INCREMENTAL:
        ods_table_config.table_id = f"{table_id}_ODS_Incremental"

    elif ingestion_type == IngestionType.FULL:
        ods_table_config.table_id = f"{table_id}_ODS_Full"

    parse_schema = ParseSchema(
        task_id="schema_parsing",
        schema_config=schema_config,
        column_mapping=column_mapping,
        data_source=data_source,
        table_config=table_config,
        dag=dag
    )

    ods_task_group= ods_builder(
        project_id=project_id,
        table_id=ods_table_config.table_id,
        dataset_id=dataset_id,
        landing_zone_dataset=landing_zone_dataset,
        landing_zone_table_name_override=landing_zone_table_name_override,
        surrogate_keys=surrogate_keys,
        column_mapping=column_mapping,
        ingestion_type=ingestion_type,
        ods_table_config=ods_table_config,
        dag=dag
    )
    
    hds_task_group = None
    if hds_table_config:
        if hds_table_config.hds_table_type == HdsTableType.SNAPSHOT:
            hds_table_config.table_id = f"{table_id}_HDS_Snapshot"
        
        elif hds_table_config.hds_table_type == HdsTableType.SCD2:
            hds_table_config.table_id = f"{table_id}_HDS_SCD2"

        hds_task_group = hds_builder(
            project_id=project_id,
            table_id=hds_table_config.table_id,
            dataset_id=dataset_id,
            landing_zone_dataset=dataset_id,
            landing_zone_table_name_override=ods_table_config.table_id,
            surrogate_keys=[column_mapping[i] for i in surrogate_keys],
            column_mapping=column_mapping,
            ingestion_type=ingestion_type,
            partition_expiration=partition_expiration,
            hds_table_config=hds_table_config,
            dag=dag
        )

    if hds_task_group:
        preceding_task >> parse_schema >> ods_task_group >> hds_task_group
    else:
        preceding_task >> parse_schema >> ods_task_group
