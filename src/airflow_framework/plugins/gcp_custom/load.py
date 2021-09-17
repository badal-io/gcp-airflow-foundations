from airflow import DAG
from airflow.exceptions import AirflowException

from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import MergeBigQueryODS
from airflow_framework.plugins.gcp_custom.bq_truncate_table_operator import TruncateBigQueryODS
from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator
from airflow_framework.enums.ingestion_type import IngestionType

from airflow.utils.task_group import TaskGroup


def build_create_load_taskgroup(
    data_source,
    table_config,
    dag
    ) -> TaskGroup:
    
    ods_metadata = table_config.ods_metadata
    ingestion_type = table_config.ingestion_type

    taskgroup = TaskGroup(group_id="create_merge_taskgroup")

    #1 Check if ODS table exists and if not create it using the provided schema file
    check_table = BigQueryCreateTableOperator(
        task_id='check_table',
        project_id=data_source.gcp_project,
        table_id=table_config.dest_table_override,
        dataset_id=data_source.dataset_data_name,
        column_mapping=table_config.column_mapping,
        gcs_schema_object=table_config.source_table_schema_object,
        ods_metadata=ods_metadata,
        task_group=taskgroup,
        dag=dag
    )

    #2 Merge or truncate tables based on the ingestion type defined in the config file and insert metadata columns
    if ingestion_type == IngestionType.INCREMENTAL:
        # Append staging table to ODS table
        insert_into_ods = MergeBigQueryODS(
            task_id="insert_delta_into_ods",
            project_id=data_source.gcp_project,
            stg_dataset_name=data_source.landing_zone_options.landing_zone_dataset,
            data_dataset_name=data_source.dataset_data_name,
            stg_table_name=table_config.landing_zone_table_name_override,
            data_table_name=table_config.dest_table_override,
            surrogate_keys=table_config.surrogate_keys,
            update_columns=table_config.update_columns,
            merge_type=table_config.merge_type,
            column_mapping=table_config.column_mapping,
            ods_metadata=ods_metadata,
            task_group=taskgroup,
            dag=dag
        )

    elif ingestion_type == IngestionType.FULL:
        # Overwrite ODS table with the staging table data
        insert_into_ods = TruncateBigQueryODS(
            task_id="insert_delta_into_ods",
            project_id=data_source.gcp_project,
            stg_dataset_name=data_source.landing_zone_options.landing_zone_dataset,
            data_dataset_name=data_source.dataset_data_name,
            stg_table_name=table_config.landing_zone_table_name_override,
            data_table_name=table_config.dest_table_override,
            surrogate_keys=table_config.surrogate_keys,
            update_columns=table_config.update_columns,
            merge_type=table_config.merge_type,
            column_mapping=table_config.column_mapping,
            ods_metadata=ods_metadata,
            task_group=taskgroup,
            dag=dag
        )
    else:
        raise AirflowException("Invalid ingestion type", ingestion_type)

    check_table >> insert_into_ods

    return taskgroup