from airflow import DAG

from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import MergeType
from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import MergeBigQueryODS
from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator
from airflow.utils.task_group import TaskGroup

def build_create_merge_taskgroup(
    data_source,
    table_config,
    dag
    ) -> TaskGroup:
    
    ods_metadata = table_config.ods_metadata

    taskgroup = TaskGroup(group_id="create_merge_taskgroup")

    #1 Check if ODS table exists and if not create it using the provided schema file
    check_table = BigQueryCreateTableOperator(
        task_id='check_table',
        project_id=data_source.gcp_project,
        table_id=table_config.dest_table_override,
        dataset_id=data_source.dataset_data_name,
        column_mapping=table_config.column_mapping,
        gcs_schema_object='gs://airflow-datasets-poc/schema.json',
        ods_metadata=ods_metadata,
        task_group=taskgroup,
        dag=dag
    )

    #2 Merge tables based on surrogate keys and insert metadata columns
    insert_delta_into_ods = MergeBigQueryODS(
        task_id="insert_delta_into_ods",
        project_id=data_source.gcp_project,
        stg_dataset_name=data_source.landing_zone_options["dataset_tmp_name"],
        data_dataset_name=data_source.dataset_data_name,
        stg_table_name=table_config.temp_table_name,
        data_table_name=table_config.dest_table_override,
        surrogate_keys=table_config.surrogate_keys,
        update_columns=table_config.update_columns,
        merge_type=MergeType.SG_KEY_WITH_HASH,
        column_mapping=table_config.column_mapping,
        ods_metadata=ods_metadata,
        task_group=taskgroup,
        dag=dag
    )

    check_table >> insert_delta_into_ods

    return taskgroup