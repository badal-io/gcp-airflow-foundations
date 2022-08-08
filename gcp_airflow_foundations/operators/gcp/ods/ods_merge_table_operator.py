from typing import Optional
from datetime import datetime

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryCreateEmptyTableOperator,
)

# from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.exceptions import AirflowException

import logging

from gcp_airflow_foundations.operators.gcp.ods.ods_sql_upsert_helpers import (
    SqlHelperODS,
)
from gcp_airflow_foundations.base_class.ods_table_config import OdsTableConfig
from gcp_airflow_foundations.enums.ingestion_type import IngestionType


class MergeBigQueryODS(BigQueryExecuteQueryOperator):
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
    :param new_column_udfs: New column UDFs
    :type new_column_udfs: dict
    :param ingestion_type: Source table ingestion time (Full or Incremental)
    :type ingestion_type: IngestionType
    :param ods_table_config: User-provided ODS configuration options
    :type ods_table_config: OdsTableConfig
    :param location: The geographic location of the job.
    :type location: str
    """

    template_fields = ("stg_table_name", "data_table_name", "stg_dataset_name")

    # @apply_defaults
    def __init__(
        self,
        *,
        project_id: str,
        stg_table_name: str,
        data_table_name: str,
        dag_table_id: str,
        stg_dataset_name: str,
        data_dataset_name: str,
        surrogate_keys: [str],
        columns: Optional[list] = None,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        column_mapping: dict,
        ingestion_type: IngestionType,
        ods_table_config: OdsTableConfig,
        column_casting: dict = {},
        new_column_udfs: dict = {},
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
        self.dag_table_id = dag_table_id
        self.stg_dataset_name = stg_dataset_name
        self.data_dataset_name = data_dataset_name
        self.surrogate_keys = surrogate_keys
        self.columns = columns
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.column_mapping = column_mapping
        self.column_casting = column_casting
        self.new_column_udfs = new_column_udfs
        self.ingestion_type = ingestion_type
        self.ods_table_config = ods_table_config

    def pre_execute(self, context) -> None:
        ds = context["ds"]

        if not self.columns:
            staging_columns = self.xcom_pull(
                context=context, task_ids=f"{self.dag_table_id}.schema_parsing"
            )["source_table_columns"]
        else:
            staging_columns = self.columns

        if self.column_mapping:
            for i in staging_columns:
                if i not in self.column_mapping:
                    self.column_mapping[i] = i

        else:
            self.column_mapping = {i: i for i in staging_columns}

        source_columns = staging_columns

        self.log.info(
            f"Execute BigQueryMergeTableOperator {self.stg_table_name}, {self.data_table_name}"
        )

        sql = ""

        sql_helper = SqlHelperODS(
            source_dataset=f"{self.project_id}.{self.stg_dataset_name}",
            target_dataset=f"{self.project_id}.{self.data_dataset_name}",
            source=f"{self.stg_table_name}_{ds}",
            target=self.data_table_name,
            columns=source_columns,
            surrogate_keys=self.surrogate_keys,
            column_mapping=self.column_mapping,
            column_casting=self.column_casting,
            new_column_udfs=self.new_column_udfs,
            ods_metadata=self.ods_table_config.ods_metadata,
        )

        if self.ods_table_config.ods_table_time_partitioning:
            partitioning_dimension = (
                self.ods_table_config.ods_table_time_partitioning.value
            )
            sql_helper.time_partitioning = partitioning_dimension
            sql_helper.partition_column_name = (
                self.ods_table_config.partition_column_name
            )

            ts = context["ts"]

            sql_helper.partition_timestamp = ts

        if self.ingestion_type == IngestionType.INCREMENTAL:
            if self.surrogate_keys:
                # Append staging table to ODS table
                sql = sql_helper.create_upsert_sql_with_hash()
            else:
                sql = sql_helper.create_full_sql()
                self.write_disposition = "WRITE_APPEND"
                self.destination_dataset_table = (
                    f"{self.project_id}.{self.data_dataset_name}.{self.data_table_name}"
                )

        elif self.ingestion_type == IngestionType.FULL:
            # Overwrite ODS table with the staging table data
            sql = sql_helper.create_full_sql()
            self.write_disposition = "WRITE_TRUNCATE"
            self.destination_dataset_table = (
                f"{self.project_id}.{self.data_dataset_name}.{self.data_table_name}"
            )

        else:
            raise AirflowException("Invalid ingestion type", self.ingestion_type)

        logging.info(
            f"Executing sql: {sql}. Write disposition: {self.write_disposition}"
        )

        self.sql = sql
