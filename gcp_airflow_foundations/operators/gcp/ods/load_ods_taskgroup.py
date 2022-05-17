from airflow import DAG

from airflow.utils.task_group import TaskGroup

from typing import List, Optional

from gcp_airflow_foundations.operators.gcp.ods.ods_merge_table_operator import (
    MergeBigQueryODS
)
from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_operator import (
    MigrateSchema,
)

from gcp_airflow_foundations.operators.gcp.create_table import (
    CustomBigQueryCreateEmptyTableOperator
)
from gcp_airflow_foundations.enums.ingestion_type import IngestionType

from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig

from gcp_airflow_foundations.operators.gcp.create_dataset import CustomBigQueryCreateEmptyDatasetOperator


def ods_builder(
    project_id: str,
    table_id: str,
    dag_table_id: str,
    dataset_id: str,
    landing_zone_dataset: str,
    landing_zone_table_name_override: Optional[str],
    column_mapping: Optional[dict],
    column_casting: Optional[dict],
    new_column_udfs: Optional[dict],
    surrogate_keys: List[str],
    ingestion_type: IngestionType,
    ods_table_config: Optional[OdsTableConfig],
    partition_expiration: Optional[int],
    location: str,
    dag: DAG,
    cluster_fields=None,
    labels=None,
    encryption_configuration=None,
) -> TaskGroup:

    """
    Method for returning a Task Group for 1) creating an empty target ODS table (if it doesn't already exist) and for 2) for merging the staging table data into the target ODS table
    """
    taskgroup = TaskGroup(group_id="create_ods_merge_taskgroup")

    if ods_table_config.ods_table_time_partitioning is not None:
        field = (
            column_mapping.get(
                ods_table_config.partition_column_name,
                ods_table_config.partition_column_name,
            )
            if column_mapping
            else ods_table_config.partition_column_name
        )

        time_partitioning = {
            "type": ods_table_config.ods_table_time_partitioning.value,
            "field": field,
            "expirationMs": partition_expiration,
        }

    else:
        time_partitioning = None

    create_dataset = CustomBigQueryCreateEmptyDatasetOperator(
        task_id="create_ods_dataset",
        project_id=project_id,
        dataset_id=dataset_id,
        location=location,
        exists_ok=True,
        task_group=taskgroup,
        dag=dag
    )

    # 1 Check if ODS table exists and if not create an empty table
    create_table = CustomBigQueryCreateEmptyTableOperator(
        task_id="create_ods_table",
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        dag_table_id=dag_table_id,
        cluster_fields=cluster_fields,
        time_partitioning=time_partitioning,
        task_group=taskgroup,
        dag=dag
    )

    # 2 Migrate schema
    migrate_schema = MigrateSchema(
        task_id="schema_migration",
        project_id=project_id,
        table_id=table_id,
        dag_table_id=dag_table_id,
        dataset_id=dataset_id,
        task_group=taskgroup,
        dag=dag
    )

    # 3 Merge or truncate tables based on the ingestion type defined in the config file and insert metadata columns
    insert = MergeBigQueryODS(
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
        ods_table_config=ods_table_config,
        location=location,
        task_group=taskgroup,
        dag=dag
    )

    create_dataset >> create_table >> migrate_schema >> insert

    return taskgroup
