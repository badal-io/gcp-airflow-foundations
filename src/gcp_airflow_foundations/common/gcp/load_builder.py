from gcp_airflow_foundations.operators.gcp.hds.load_hds_taskgroup import hds_builder
from gcp_airflow_foundations.operators.gcp.ods.load_ods_taskgroup import ods_builder
from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_operator import MigrateSchema

from gcp_airflow_foundations.common.gcp.ods.schema_utils import parse_ods_schema
from gcp_airflow_foundations.common.gcp.hds.schema_utils import parse_hds_schema

import logging

def load_builder(
    project_id,
    table_id,
    dataset_id,
    landing_zone_dataset,
    landing_zone_table_name_override,
    surrogate_keys,
    column_mapping,
    gcs_schema_object,
    schema_fields,
    hds_table_config,
    ods_table_config,
    preceding_task,
    dag):

    """
    Method for building all needed Task Groups based on the HdsTableConfig and OdsTableConfig options declared in the config files
    """

    builders = []

    if ods_table_config:
        ods_schema_fields, source_table_columns = parse_ods_schema(
            gcs_schema_object=gcs_schema_object,
            schema_fields=schema_fields,
            column_mapping=column_mapping,
            ods_metadata=ods_table_config.ods_metadata
        )

        logging.info(f"ODS table schema: {ods_schema_fields}")

        ods_task_group = ods_builder(
            project_id=project_id,
            table_id=table_id,
            dataset_id=dataset_id,
            landing_zone_dataset=landing_zone_dataset,
            landing_zone_table_name_override=landing_zone_table_name_override,
            surrogate_keys=surrogate_keys,
            column_mapping=column_mapping,
            columns=source_table_columns,
            schema_fields=ods_schema_fields,
            ods_table_config=ods_table_config,
            dag=dag
        )
        
        builders.append(ods_task_group)

    if hds_table_config:
        hds_schema_fields, source_table_columns = parse_hds_schema(
            gcs_schema_object=gcs_schema_object,
            schema_fields=schema_fields,
            column_mapping=column_mapping,
            hds_metadata=hds_table_config.hds_metadata,
            hds_table_type=hds_table_config.hds_table_type
        )

        logging.info(f"HDS table schema: {hds_schema_fields}")

        hds_task_group = hds_builder(
            project_id=project_id,
            table_id=table_id,
            dataset_id=dataset_id,
            landing_zone_dataset=landing_zone_dataset,
            landing_zone_table_name_override=landing_zone_table_name_override,
            surrogate_keys=surrogate_keys,
            column_mapping=column_mapping,
            columns=source_table_columns,
            schema_fields=hds_schema_fields,
            hds_table_config=hds_table_config,
            dag=dag
        )
        
        builders.append(hds_task_group)

    for taskgroup in builders:
        preceding_task >> taskgroup

    return builders