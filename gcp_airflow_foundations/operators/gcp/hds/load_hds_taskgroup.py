from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from gcp_airflow_foundations.enums.hds_table_type import HdsTableType

from airflow.utils.task_group import TaskGroup

from gcp_airflow_foundations.operators.gcp.hds.hds_merge_table_operator import (
    MergeBigQueryHDS,
)
from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_operator import (
    MigrateSchema,
)

from gcp_airflow_foundations.operators.gcp.create_table import (
    CustomBigQueryCreateEmptyTableOperator,
)

from gcp_airflow_foundations.operators.gcp.create_dataset import CustomBigQueryCreateEmptyDatasetOperator


def hds_builder(
    project_id,
    table_id,
    dag_table_id,
    dataset_id,
    landing_zone_dataset,
    landing_zone_table_name_override,
    column_mapping,
    column_casting,
    new_column_udfs,
    surrogate_keys,
    ingestion_type,
    hds_table_config,
    partition_expiration,
    location,
    dag,
    cluster_fields=None,
    time_partitioning=None,
    labels=None,
    encryption_configuration=None,
) -> TaskGroup:

    """
    Method for returning a Task Group for 1) creating an empty target HDS table (if it doesn't already exist) and for 2) for merging the staging table data into the target HDS table
    """

    taskgroup = TaskGroup(group_id="create_hds_merge_taskgroup")

    if hds_table_config.hds_table_type == HdsTableType.SNAPSHOT:
        time_partitioning = {
            "type": hds_table_config.hds_table_time_partitioning.value,
            "field": hds_table_config.hds_metadata.partition_time_column_name,
            "expirationMs": partition_expiration,
        }
    elif hds_table_config.hds_table_type == HdsTableType.SCD2:
        time_partitioning = None
    else:
        raise AirflowException(
            "Invalid HDS table type", hds_table_config.hds_table_type
        )

    hds_task = PythonOperator(
        task_id="run_hds_tasks",
        op_kwargs={
            "project_id": project_id,
            "dataset_id": dataset_id,
            "location": location,
            "taskgroup": taskgroup,
            "table_id": table_id,
            "dag_table_id": dag_table_id,
            "cluster_fields": cluster_fields,
            "time_partitioning": time_partitioning,
            "dag": dag,
            "landing_zone_dataset": landing_zone_dataset,
            "landing_zone_table_name_override": landing_zone_table_name_override,
            "surrogate_keys": surrogate_keys,
            "column_mapping": column_mapping,
            "column_casting": column_casting,
            "new_column_udfs": new_column_udfs,
            "ingestion_type": ingestion_type,
            "hds_table_config": hds_table_config
        },
        python_callable=hds_task_operator,
        task_group=taskgroup,
        dag=dag
    )

    hds_task

    return taskgroup


def hds_task_operator(
    project_id,
    dataset_id,
    location,
    taskgroup,
    table_id,
    dag_table_id,
    cluster_fields,
    time_partitioning,
    landing_zone_dataset,
    landing_zone_table_name_override,
    surrogate_keys,
    column_mapping,
    column_casting,
    new_column_udfs,
    ingestion_type,
    hds_table_config,
    **kwargs
):
    create_dataset = CustomBigQueryCreateEmptyDatasetOperator(
        task_id="create_hds_dataset",
        project_id=project_id,
        dataset_id=dataset_id,
        location=location,
        exists_ok=True,
        task_group=taskgroup
    )
    create_dataset.execute(context=kwargs)

    # 1 Check if HDS table exists and if not create an empty table
    create_table = CustomBigQueryCreateEmptyTableOperator(
        task_id="create_hds_table",
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        dag_table_id=dag_table_id,
        cluster_fields=cluster_fields,
        time_partitioning=time_partitioning,
        task_group=taskgroup
    )
    create_table.pre_execute(context=kwargs)
    create_table.execute(context=kwargs)

    # 2 Migrate schema
    migrate_schema = MigrateSchema(
        task_id="schema_migration",
        project_id=project_id,
        table_id=table_id,
        dag_table_id=dag_table_id,
        dataset_id=dataset_id,
        task_group=taskgroup
    )
    migrate_schema.execute(context=kwargs)

    # 3 Load staging table to HDS table
    insert = MergeBigQueryHDS(
        task_id=f"upsert_{table_id}",
        project_id=project_id,
        stg_dataset_name=landing_zone_dataset,
        data_dataset_name=dataset_id,
        stg_table_name=landing_zone_table_name_override,
        data_table_name=table_id,
        dag_table_id=dag_table_id,
        surrogate_keys=surrogate_keys,
        column_mapping=column_mapping,
        column_casting=column_casting,
        new_column_udfs=new_column_udfs,
        ingestion_type=ingestion_type,
        hds_table_config=hds_table_config,
        location=location,
        task_group=taskgroup
    )
    insert.pre_execute(context=kwargs)
    insert.execute(context=kwargs)
