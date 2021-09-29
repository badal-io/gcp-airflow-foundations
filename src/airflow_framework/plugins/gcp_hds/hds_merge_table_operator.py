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

from airflow_framework.plugins.gcp_hds.hds_sql_upsert_helpers import SqlHelperHDS
from airflow_framework.enums.hds_table_type import HdsTableType
from airflow_framework.base_class.hds_table_config import HdsTableConfig

class MergeBigQueryHDS(BigQueryOperator):
    """
    Merge data into a BigQuery HDS table.
    
    Attributes:
        project_id: GCP project ID               
        stg_table_name: Source table name    
        data_table_name: Target table name
        stg_dataset_name: Source dataset name
        data_dataset_name: Target dataset name
        surrogate_keys: List of surrogate keys
        delegate_to: The account to impersonate using domain-wide delegation of authority, if any
        gcp_conn_id: Airflow GCP connection ID
        column_mapping: Column mapping dictionary
        hds_table_config: HdsTableConfig object with user-provided HDS configuration options
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
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        column_mapping: dict,
        hds_table_config: HdsTableConfig,
        **kwargs,
    ) -> None:
        super(MergeBigQueryHDS, self).__init__(
            delegate_to=delegate_to,
            gcp_conn_id=gcp_conn_id,
            use_legacy_sql=False,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_NEVER",
            sql="sql",
            **kwargs,
        )
        self.project_id = project_id
        self.stg_table_name = stg_table_name
        self.data_table_name = data_table_name
        self.stg_dataset_name = stg_dataset_name
        self.data_dataset_name = data_dataset_name
        self.surrogate_keys = surrogate_keys
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.column_mapping = column_mapping
        self.hds_table_config = hds_table_config

    def pre_execute(self, context) -> None:
        self.log.info(
            f"Execute BigQueryMergeTableOperator {self.stg_table_name}, {self.data_table_name}"
        )

        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
        )
        conn = hook.get_conn()
        bq_cursor = conn.cursor()

        schema = bq_cursor.get_schema(
            dataset_id=self.stg_dataset_name, table_id=self.stg_table_name
        )
        self.log.info("Target table schema is : %s", schema)

        columns: list[str] = list(map(lambda x: x["name"], schema["fields"]))

        sql = ""

        sql_helper = SqlHelperHDS(
            source_dataset=self.stg_dataset_name,
            target_dataset=self.data_dataset_name,
            source=self.stg_table_name,
            target=self.data_table_name,
            columns=columns,
            surrogate_keys=self.surrogate_keys,
            column_mapping=self.column_mapping,
            time_partitioning=self.hds_table_config.hds_table_time_partitioning.value,
            hds_metadata=self.hds_table_config.hds_metadata
        )

        if self.hds_table_config.hds_table_type == HdsTableType.SNAPSHOT:
            sql = sql_helper.create_snapshot_sql_with_hash()
            write_disposition = "WRITE_TRUNCATE"

        elif self.hds_table_config.hds_table_type == HdsTableType.SCD2:
            sql = sql_helper.create_scd2_sql_with_hash()
            write_disposition = "WRITE_APPEND"

        else:
            raise AirflowException("Invalid HDS table type", self.hds_table_config.hds_table_type)

        logging.info(f"Executing sql: {sql}")

        self.sql = sql
        self.write_disposition = write_disposition