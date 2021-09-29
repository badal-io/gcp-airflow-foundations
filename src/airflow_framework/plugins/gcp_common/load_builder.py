from airflow_framework.plugins.gcp_hds.load_hds_taskgroup import hds_builder
from airflow_framework.plugins.gcp_ods.load_ods_taskgroup import ods_builder
from airflow_framework.plugins.gcp_common.parse_schema_from_gcs import parse_schema

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
    dag):

    schema_fields = parse_schema(
        gcs_schema_object=gcs_schema_object,
        column_mapping=column_mapping,
        hds_metadata=hds_table_config.hds_metadata,
        hds_table_type=hds_table_config.hds_table_type
    )

    builders = []

    if ods_table_config:
        ods_task_group = ods_builder(
                    project_id=project_id,
                    table_id=table_id,
                    dataset_id=dataset_id,
                    landing_zone_dataset=landing_zone_dataset,
                    landing_zone_table_name_override=landing_zone_table_name_override,
                    surrogate_keys=surrogate_keys,
                    column_mapping=column_mapping,
                    schema_fields=schema_fields,
                    ods_table_config=ods_table_config,
                    dag=dag
                )
        
        builders.append(ods_task_group)

    if hds_table_config:
        hds_task_group = hds_builder(
                    project_id=project_id,
                    table_id=table_id,
                    dataset_id=dataset_id,
                    landing_zone_dataset=landing_zone_dataset,
                    landing_zone_table_name_override=landing_zone_table_name_override,
                    surrogate_keys=surrogate_keys,
                    column_mapping=column_mapping,
                    schema_fields=schema_fields,
                    hds_table_config=hds_table_config,
                    dag=dag
                )
        
        builders.append(hds_task_group)

    return builders