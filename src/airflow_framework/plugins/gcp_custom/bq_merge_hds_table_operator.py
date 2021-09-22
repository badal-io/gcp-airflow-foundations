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

from airflow_framework.plugins.gcp_custom.sql_upsert_helpers import create_scd2_sql_with_hash, create_snapshot_sql_with_hash
from airflow_framework.enums.hds_table_type import HdsTableType

class MergeBigQueryHDS(BigQueryOperator):
    """
    Merge data into a BigQuery HDS table.
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
        hds_table_type: HdsTableType,
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        column_mapping: dict,
        hds_metadata: dict,

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
        self.hds_table_type = hds_table_type
        self.delegate_to = delegate_to
        self.column_mapping = column_mapping
        self.hds_metadata = hds_metadata

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

        if self.hds_table_type == HdsTableType.SCD2:
            sql = create_scd2_sql_with_hash(
                self.stg_dataset_name,
                self.data_dataset_name,
                self.stg_table_name,
                self.data_table_name,
                self.surrogate_keys,
                columns,
                self.column_mapping,
                self.hds_metadata
            )
            
        elif self.hds_table_type == HdsTableType.SNAPSHOT:
            sql = create_snapshot_sql_with_hash(
                self.stg_dataset_name,
                self.data_dataset_name,
                self.stg_table_name,
                self.data_table_name,
                self.surrogate_keys,
                columns,
                self.column_mapping,
                self.hds_metadata
            )

        logging.info(f"Executing sql: {sql}")

        self.sql = sql