from airflow import DAG
from airflow.exceptions import AirflowException

from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator
)

from gcp_airflow_foundations.operators.gcp.ods.ods_merge_table_operator import MergeBigQueryODS
from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_operator import MigrateSchema

from gcp_airflow_foundations.operators.gcp.create_table import CustomBigQueryCreateEmptyTableOperator


def ods_builder(
    project_id,
    table_id,
    dataset_id,
    landing_zone_dataset,
    landing_zone_table_name_override,
    column_mapping,
    surrogate_keys,
    ingestion_type,
    ods_table_config,
    location,
    dag,
    time_partitioning=None,
    labels=None,
    encryption_configuration=None) -> TaskGroup:

    """
    Method for returning a Task Group for 1) creating an empty target ODS table (if it doesn't already exist) and for 2) for merging the staging table data into the target ODS table
    """
    taskgroup = TaskGroup(group_id="create_ods_merge_taskgroup")

    #1 Check if ODS table exists and if not create an empty table
    create_table = CustomBigQueryCreateEmptyTableOperator(
        task_id="create_ods_table",
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        time_partitioning=None,
        task_group=taskgroup,
        dag=dag
    )
    
    #2 Migrate schema
    migrate_schema = MigrateSchema(
        task_id="schema_migration",
        project_id=project_id,
        table_id=table_id,
        dataset_id=dataset_id, 
        task_group=taskgroup,
        dag=dag
    )
    
    #3 Merge or truncate tables based on the ingestion type defined in the config file and insert metadata columns
    insert = MergeBigQueryODS(
        task_id=f"upsert_{table_id}",
        project_id=project_id,
        stg_dataset_name=landing_zone_dataset,
        data_dataset_name=dataset_id,
        stg_table_name=landing_zone_table_name_override,
        data_table_name=table_id,
        surrogate_keys=surrogate_keys,
        column_mapping=column_mapping,
        ingestion_type=ingestion_type,
        ods_table_config=ods_table_config,
        location=location,
        task_group=taskgroup,
        dag=dag
    )

    create_table >> migrate_schema >> insert

    return taskgroup
