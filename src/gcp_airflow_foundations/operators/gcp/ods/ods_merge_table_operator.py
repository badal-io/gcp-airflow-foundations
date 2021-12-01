from typing import Optional

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
    BigQueryCreateEmptyTableOperator,
)

from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from airflow.exceptions import AirflowException

import logging

from gcp_airflow_foundations.operators.gcp.ods.ods_sql_upsert_helpers import SqlHelperODS
from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig
from gcp_airflow_foundations.enums.ingestion_type import IngestionType

class MergeBigQueryODS(BigQueryOperator):
    """
    Merges data into a BigQuery ODS table.
    
    :param project_id: GCP project ID  
    :type project_id: str
    :param stg_table_name: Source table name
    :type stg_table_name: str
    :param data_table_name: Target table name
    :type data_table_name: str
    :param stg_dataset_name: Source dataset name
    :type stg_dataset_name: str
    :param data_dataset_name: Target dataset name
    :type data_dataset_name: str
    :param surrogate_keys: Surrogate keys of the target table
    :type surrogate_keys: list
    :param delegate_to: The account to impersonate using domain-wide delegation of authority, if any
    :type delegate_to: str
    :param gcp_conn_id: Airflow GCP connection ID
    :type gcp_conn_id: str
    :param column_mapping: Column mapping
    :type column_mapping: dict
    :param ingestion_type: Source table ingestion time (Full or Incremental)
    :type ingestion_type: IngestionType
    :param ods_table_config: User-provided ODS configuration options
    :type ods_table_config: OdsTableConfig
    :param location: The geographic location of the job. 
    :type location: str
    """

    template_fields = (
        "stg_table_name",
        "data_table_name",
        "stg_dataset_name"
    )

    @apply_defaults
    def __init__(
        self,
        *,
        project_id: str,
        stg_table_name: str,
        data_table_name: str,
        stg_dataset_name: str,
        data_dataset_name: str,
        surrogate_keys: [str],
        columns: Optional[list] = None,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        column_mapping: dict,
        ingestion_type: IngestionType,
        ods_table_config: OdsTableConfig,
        location: Optional[str] = None,
        **kwargs,
    ) -> None:
        super(MergeBigQueryODS, self).__init__(
            delegate_to=delegate_to,
            gcp_conn_id=gcp_conn_id,
            use_legacy_sql=False,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_NEVER",
            location=location,
            sql="",
            **kwargs,
        )
        self.project_id = project_id
        self.stg_table_name = stg_table_name
        self.data_table_name = data_table_name
        self.stg_dataset_name = stg_dataset_name
        self.data_dataset_name = data_dataset_name
        self.surrogate_keys = surrogate_keys
        self.columns = columns
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.column_mapping = column_mapping
        self.ingestion_type = ingestion_type
        self.ods_table_config = ods_table_config

    def pre_execute(self, context) -> None:
        ds = context['ds']

        if not self.columns:
            staging_columns = self.xcom_pull(context=context, task_ids="schema_parsing")['source_table_columns']
        else:
            staging_columns = self.columns

        if self.column_mapping:
            for i in staging_columns:
                if i not in self.column_mapping:
                    self.column_mapping[i] = i
            
        else:
            self.column_mapping = {i:i for i in staging_columns}
            
        source_columns = staging_columns

        self.log.info(
            f"Execute BigQueryMergeTableOperator {self.stg_table_name}, {self.data_table_name}"
        )

        sql = ""

        sql_helper = SqlHelperODS(
            source_dataset=self.stg_dataset_name,
            target_dataset=self.data_dataset_name ,
            source=f"{self.stg_table_name}_{ds}",
            target=self.data_table_name,
            columns=source_columns,
            surrogate_keys=self.surrogate_keys,
            column_mapping=self.column_mapping,
            ods_metadata=self.ods_table_config.ods_metadata
        )

        if self.ingestion_type == IngestionType.INCREMENTAL:
            # Append staging table to ODS table
            sql = sql_helper.create_upsert_sql_with_hash()

        elif self.ingestion_type == IngestionType.FULL:
            # Overwrite ODS table with the staging table data
            sql = sql_helper.create_full_sql()
            self.write_disposition = "WRITE_TRUNCATE"
            self.destination_dataset_table = f"{self.data_dataset_name}.{self.data_table_name}"

        else:
            raise AirflowException("Invalid ingestion type", self.ingestion_type)

        logging.info(f"Executing sql: {sql}. Write disposition: {self.write_disposition}")

        self.sql = sql