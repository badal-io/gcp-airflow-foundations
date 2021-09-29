from airflow import DAG
from airflow.exceptions import AirflowException

from airflow_framework.enums.ingestion_type import IngestionType

from airflow_framework.plugins.gcp_ods.ods_sql_upsert_helpers import SqlHelperODS

from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator
)

from airflow_framework.plugins.gcp_ods.ods_merge_table_operator import MergeBigQueryODS

def ods_builder(
    project_id,
    table_id,
    dataset_id,
    landing_zone_dataset,
    landing_zone_table_name_override,
    column_mapping,
    schema_fields,
    surrogate_keys,
    ods_table_config,
    dag,
    time_partitioning=None,
    labels=None,
    encryption_configuration=None) -> TaskGroup:

    taskgroup = TaskGroup(group_id="create_ods_merge_taskgroup")

    if ods_table_config.ingestion_type == IngestionType.INCREMENTAL:
        table_id = f"{table_id}_ODS_Incremental"

    elif ods_table_config.ingestion_type == IngestionType.FULL:
        table_id = f"{table_id}_ODS_Full"

    #1 Check if ODS table exists and if not create it using the provided schema file
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_ods_table",
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=schema_fields,
        time_partitioning=time_partitioning,
        exists_ok=True,
        labels=labels,
        encryption_configuration=encryption_configuration,
        task_group=taskgroup,
        dag=dag
    )

    #2 Merge or truncate tables based on the ingestion type defined in the config file and insert metadata columns
    insert = MergeBigQueryODS(
        task_id="insert_into_ods_table",
        project_id=project_id,
        stg_dataset_name=landing_zone_dataset,
        data_dataset_name=dataset_id,
        stg_table_name=landing_zone_table_name_override,
        data_table_name=table_id,
        surrogate_keys=surrogate_keys,
        column_mapping=column_mapping,
        ods_table_config=ods_table_config,
        task_group=taskgroup,
        dag=dag
    )

    create_table >> insert

    return taskgroup
