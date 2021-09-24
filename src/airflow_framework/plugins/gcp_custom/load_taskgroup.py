from typing import Optional

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from airflow_framework.plugins.gcp_custom.bq_merge_table_operator import MergeBigQueryODS
from airflow_framework.plugins.gcp_custom.bq_merge_hds_table_operator import MergeBigQueryHDS
from airflow_framework.base_class.ods_metadata_config import OdsTableMetadataConfig
from airflow_framework.base_class.hds_metadata_config import HdsTableMetadataConfig
from airflow_framework.plugins.gcp_custom.bq_truncate_table_operator import TruncateBigQueryODS
from airflow_framework.plugins.gcp_custom.bq_create_table_operator import BigQueryCreateTableOperator
from airflow_framework.enums.ingestion_type import IngestionType
from airflow_framework.enums.hds_table_type import HdsTableType

from airflow.utils.task_group import TaskGroup

class TaskGroupBuilder:

    @apply_defaults
    def __init__(
        self,
        project_id: str,
        table_id: str,
        dataset_id: str,
        landing_zone_dataset: str,
        landing_zone_table_name_override: str,
        surrogate_keys: list,
        dag: DAG,
        column_mapping: Optional[dict] = None,
        gcs_schema_object: Optional[str] = None,
        schema_fields: Optional[list] = None,
        hds_metadata: Optional[HdsTableMetadataConfig] = None,
        ods_metadata: Optional[OdsTableMetadataConfig] = None,
        merge_type: Optional[str] = None,
        ingestion_type: Optional[IngestionType] = None,
        hds_table_type: Optional[HdsTableType] = None
        ):

        self.project_id = project_id
        self.landing_zone_table_name_override = landing_zone_table_name_override
        self.table_id = table_id
        self.landing_zone_dataset = landing_zone_dataset
        self.dataset_id = dataset_id
        self.surrogate_keys = surrogate_keys
        self.column_mapping = column_mapping
        self.hds_metadata = hds_metadata
        self.ods_metadata = ods_metadata
        self.gcs_schema_object = gcs_schema_object
        self.schema_fields = schema_fields
        self.merge_type = merge_type
        self.ingestion_type = ingestion_type
        self.hds_table_type = hds_table_type
        self.dag = dag


    def build_hds_load_taskgroup(self) -> TaskGroup:

        taskgroup = TaskGroup(group_id="create_hds_merge_taskgroup")
        if self.hds_table_type == HdsTableType.SNAPSHOT:
            time_partitioning = {
                "type":"DAY",
                "field":self.hds_metadata.eff_start_time_column_name
            }
        else:
            time_partitioning = None


        check_table = BigQueryCreateTableOperator(
            task_id='check_table',
            project_id=self.project_id,
            table_id=self.table_id,
            dataset_id=self.dataset_id,
            hds_table_type=self.hds_table_type,
            column_mapping=self.column_mapping,
            schema_fields=self.schema_fields,
            gcs_schema_object=self.gcs_schema_object,
            hds_metadata=self.hds_metadata,
            time_partitioning=time_partitioning,
            task_group=taskgroup,
            dag=self.dag
        )


        # Insert staging table to HDS table
        insert_into_hds = MergeBigQueryHDS(
            task_id="insert_delta_into_HDS",
            project_id=self.project_id,
            stg_dataset_name=self.landing_zone_dataset,
            data_dataset_name=self.dataset_id,
            stg_table_name=self.landing_zone_table_name_override,
            data_table_name=self.table_id,
            surrogate_keys=self.surrogate_keys,
            column_mapping=self.column_mapping,
            hds_metadata=self.hds_metadata,
            hds_table_type=self.hds_table_type,
            task_group=taskgroup,
            dag=self.dag
        )

        check_table >> insert_into_hds

        return taskgroup


    def build_ods_load_taskgroup(self) -> TaskGroup:
        taskgroup = TaskGroup(group_id="create_ods_merge_taskgroup")

        #1 Check if ODS table exists and if not create it using the provided schema file
        if self.gcs_schema_object and not self.schema_fields:
            check_table = BigQueryCreateTableOperator(
                task_id='check_table',
                project_id=self.project_id,
                table_id=self.table_id,
                dataset_id=self.dataset_id,
                column_mapping=self.column_mapping,
                gcs_schema_object=self.gcs_schema_object,
                ods_metadata=self.ods_metadata,
                task_group=taskgroup,
                dag=self.dag
            )

        else:
            check_table = BigQueryCreateTableOperator(
                task_id='check_table',
                project_id=self.project_id,
                table_id=self.table_id,
                dataset_id=self.dataset_id,
                column_mapping=self.column_mapping,
                schema_fields=self.schema_fields,
                ods_metadata=self.ods_metadata,
                task_group=taskgroup,
                dag=self.dag
            )      

        #2 Merge or truncate tables based on the ingestion type defined in the config file and insert metadata columns
        if self.ingestion_type == IngestionType.INCREMENTAL:
            # Append staging table to ODS table
            insert_into_ods = MergeBigQueryODS(
                task_id="insert_delta_into_ods",
                project_id=self.project_id,
                stg_dataset_name=self.landing_zone_dataset,
                data_dataset_name=self.dataset_id,
                stg_table_name=self.landing_zone_table_name_override,
                data_table_name=self.table_id,
                surrogate_keys=self.surrogate_keys,
                merge_type=self.merge_type,
                column_mapping=self.column_mapping,
                ods_metadata=self.ods_metadata,
                task_group=taskgroup,
                dag=self.dag
            )

        elif self.ingestion_type == IngestionType.FULL:
            # Overwrite ODS table with the staging table data
            insert_into_ods = TruncateBigQueryODS(
                task_id="insert_delta_into_ods",
                project_id=self.project_id,
                stg_dataset_name=self.landing_zone_dataset,
                data_dataset_name=self.dataset_id,
                stg_table_name=self.landing_zone_table_name_override,
                data_table_name=self.table_id,
                surrogate_keys=self.surrogate_keys,
                merge_type=self.merge_type,
                column_mapping=self.column_mapping,
                ods_metadata=self.ods_metadata,
                task_group=taskgroup,
                dag=self.dag
            )
        else:
            raise AirflowException("Invalid ingestion type", self.ingestion_type)

        check_table >> insert_into_ods

        return taskgroup

    def build_task_group(self):
        if self.hds_table_type is None:
            return self.build_ods_load_taskgroup()
        
        else:
            return self.build_hds_load_taskgroup()