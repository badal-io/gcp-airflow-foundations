from airflow import DAG
from airflow.exceptions import AirflowException

from gcp_airflow_foundations.enums.hds_table_type import HdsTableType

from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator
)

from gcp_airflow_foundations.operators.gcp.hds.hds_merge_table_operator import MergeBigQueryHDS
from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_operator import MigrateSchema

from gcp_airflow_foundations.operators.gcp.create_table import CustomBigQueryCreateEmptyTableOperator

def hds_builder(
    project_id,
    table_id,
    dataset_id,
    landing_zone_dataset,
    landing_zone_table_name_override,
    column_mapping,
    surrogate_keys,
    ingestion_type,
    hds_table_config,
    partition_expiration,
    location,
    dag,
    time_partitioning=None,
    labels=None,
    encryption_configuration=None) -> TaskGroup:

    """
    Method for returning a Task Group for 1) creating an empty target HDS table (if it doesn't already exist) and for 2) for merging the staging table data into the target HDS table
    """

    taskgroup = TaskGroup(group_id="create_hds_merge_taskgroup")

    if hds_table_config.hds_table_type == HdsTableType.SNAPSHOT:
        time_partitioning = {
            "type":hds_table_config.hds_table_time_partitioning.value,
            "field":hds_table_config.hds_metadata.partition_time_column_name,
            "expirationMs":partition_expiration
        }
    elif hds_table_config.hds_table_type == HdsTableType.SCD2:
        time_partitioning = None
    else:
        raise AirflowException("Invalid HDS table type", hds_table_config.hds_table_type)
        
    #1 Check if HDS table exists and if not create an empty table
    create_table = CustomBigQueryCreateEmptyTableOperator(
        task_id="create_hds_table",
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        time_partitioning=time_partitioning,
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

    #3 Load staging table to HDS table
    insert = MergeBigQueryHDS(
        task_id=f"upsert_{table_id}",
        project_id=project_id,
        stg_dataset_name=landing_zone_dataset,
        data_dataset_name=dataset_id,
        stg_table_name=landing_zone_table_name_override,
        data_table_name=table_id,
        surrogate_keys=surrogate_keys,
        column_mapping=column_mapping,
        ingestion_type=ingestion_type,
        hds_table_config=hds_table_config,
        location=location,
        task_group=taskgroup,
        dag=dag
    )

    create_table >> migrate_schema >> insert

    return taskgroup
