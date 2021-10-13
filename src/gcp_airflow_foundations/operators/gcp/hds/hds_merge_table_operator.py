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

from gcp_airflow_foundations.operators.gcp.hds.hds_sql_upsert_helpers import SqlHelperHDS
from gcp_airflow_foundations.enums.hds_table_type import HdsTableType
from gcp_airflow_foundations.base_class.hds_table_config import HdsTableConfig

class MergeBigQueryHDS(BigQueryOperator):
    """
    Merges data into a BigQuery HDS table.
    
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
    :param hds_table_config: User-provided HDS configuration options
    :type hds_table_config: HdsTableConfig
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
        columns: list,
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
        self.columns = columns
        self.hds_table_config = hds_table_config

    def pre_execute(self, context) -> None:
        self.log.info(
            f"Execute BigQueryMergeTableOperator {self.stg_table_name}, {self.data_table_name}"
        )

        sql = ""

        sql_helper = SqlHelperHDS(
            source_dataset=self.stg_dataset_name,
            target_dataset=self.data_dataset_name,
            source=self.stg_table_name,
            target=self.data_table_name,
            columns=self.columns,
            surrogate_keys=self.surrogate_keys,
            column_mapping=self.column_mapping,
            hds_metadata=self.hds_table_config.hds_metadata
        )

        if self.hds_table_config.hds_table_type == HdsTableType.SNAPSHOT:
            sql_helper.time_partitioning = self.hds_table_config.hds_table_time_partitioning.value
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