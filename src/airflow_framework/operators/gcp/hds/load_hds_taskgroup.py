from airflow import DAG
from airflow.exceptions import AirflowException

from airflow_framework.enums.hds_table_type import HdsTableType

from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator
)

from airflow_framework.operators.gcp.hds.hds_merge_table_operator import MergeBigQueryHDS
from airflow_framework.operators.gcp.hds.hds_sql_upsert_helpers import SqlHelperHDS
from airflow_framework.operators.gcp.schema_migration.schema_migration_operator import MigrateSchema


def hds_builder(
    project_id,
    table_id,
    dataset_id,
    landing_zone_dataset,
    landing_zone_table_name_override,
    column_mapping,
    columns,
    schema_fields,
    surrogate_keys,
    hds_table_config,
    dag,
    time_partitioning=None,
    labels=None,
    encryption_configuration=None) -> TaskGroup:

    """
    Method for building a Task Group consisting of the following operators:
    1) BigQueryCreateEmptyTableOperator for creating (if it doesn't already exist) the target HDS table using the parsed schema
    2) MergeBigQueryHDS for merging the staging table data into the target HDS table
    """

    taskgroup = TaskGroup(group_id="create_hds_merge_taskgroup")

    if hds_table_config.hds_table_type == HdsTableType.SNAPSHOT:
        table_id = f"{table_id}_HDS_Snapshot"

        time_partitioning = {
            "type":hds_table_config.hds_table_time_partitioning.value,
            "field":hds_table_config.hds_metadata.partition_time_column_name
        }

    elif hds_table_config.hds_table_type == HdsTableType.SCD2:
        table_id = f"{table_id}_HDS_SCD2"

        time_partitioning = None
        
    else:
        raise AirflowException("Invalid HDS table type", hds_table_config.hds_table_type)
        
    #1 Check if HDS table exists and if not create it using the provided schema file
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_hds_table",
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=schema_fields,
        time_partitioning=time_partitioning,
        labels=labels,
        encryption_configuration=encryption_configuration,
        task_group=taskgroup,
        dag=dag
    )

    #2 Migrate schema
    migrate_schema = MigrateSchema(
        task_id="schema_migration",
        project_id=project_id,
        table_id=table_id,
        dataset_id=dataset_id, 
        new_schema_fields=schema_fields,
        task_group=taskgroup,
        dag=dag
    )

    #3 Load staging table to HDS table
    insert = MergeBigQueryHDS(
        task_id="insert_into_hds_table",
        project_id=project_id,
        stg_dataset_name=landing_zone_dataset,
        data_dataset_name=dataset_id,
        stg_table_name=landing_zone_table_name_override,
        data_table_name=table_id,
        surrogate_keys=surrogate_keys,
        column_mapping=column_mapping,
        columns=columns,
        hds_table_config=hds_table_config,
        task_group=taskgroup,
        dag=dag
    )

    create_table >> migrate_schema >> insert

    return taskgroup