from airflow import DAG
from airflow.exceptions import AirflowException

from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import MergeBigQueryODS
from airflow_framework.plugins.gcp_custom.bq_truncate_table_operator import TruncateBigQueryODS
from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator
from airflow_framework.enums.ingestion_type import IngestionType

from airflow.utils.task_group import TaskGroup


def build_create_load_taskgroup(
    project_id,
    table_id,
    dataset_id,
    landing_zone_dataset,
    landing_zone_table_name_override,
    column_mapping,
    gcs_schema_object,
    schema_fields,
    ods_metadata,
    surrogate_keys,
    update_columns,
    merge_type,
    ingestion_type,
    dag
    ) -> TaskGroup:
    
    taskgroup = TaskGroup(group_id="create_merge_taskgroup")

    #1 Check if ODS table exists and if not create it using the provided schema file
    if gcs_schema_object and not schema_fields:
        check_table = BigQueryCreateTableOperator(
            task_id='check_table',
            project_id=project_id,
            table_id=table_id,
            dataset_id=dataset_id,
            column_mapping=column_mapping,
            gcs_schema_object=gcs_schema_object,
            ods_metadata=ods_metadata,
            task_group=taskgroup,
            dag=dag
        )

    else:
        check_table = BigQueryCreateTableOperator(
            task_id='check_table',
            project_id=project_id,
            table_id=table_id,
            dataset_id=dataset_id,
            column_mapping=column_mapping,
            schema_fields=schema_fields,
            ods_metadata=ods_metadata,
            task_group=taskgroup,
            dag=dag
        )      

    #2 Merge or truncate tables based on the ingestion type defined in the config file and insert metadata columns
    if ingestion_type == IngestionType.INCREMENTAL:
        # Append staging table to ODS table
        insert_into_ods = MergeBigQueryODS(
            task_id="insert_delta_into_ods",
            project_id=project_id,
            stg_dataset_name=landing_zone_dataset,
            data_dataset_name=dataset_id,
            stg_table_name=landing_zone_table_name_override,
            data_table_name=table_id,
            surrogate_keys=surrogate_keys,
            update_columns=update_columns,
            merge_type=merge_type,
            column_mapping=column_mapping,
            ods_metadata=ods_metadata,
            task_group=taskgroup,
            dag=dag
        )

    elif ingestion_type == IngestionType.FULL:
        # Overwrite ODS table with the staging table data
        insert_into_ods = TruncateBigQueryODS(
            task_id="insert_delta_into_ods",
            project_id=project_id,
            stg_dataset_name=landing_zone_dataset,
            data_dataset_name=dataset_id,
            stg_table_name=landing_zone_table_name_override,
            data_table_name=table_id,
            surrogate_keys=surrogate_keys,
            update_columns=update_columns,
            merge_type=merge_type,
            column_mapping=column_mapping,
            ods_metadata=ods_metadata,
            task_group=taskgroup,
            dag=dag
        )
    else:
        raise AirflowException("Invalid ingestion type", ingestion_type)

    check_table >> insert_into_ods

    return taskgroup