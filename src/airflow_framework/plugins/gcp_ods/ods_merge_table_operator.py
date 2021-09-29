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

from airflow_framework.plugins.gcp_ods.ods_sql_upsert_helpers import SqlHelperODS
from airflow_framework.base_class.ods_table_config import OdsTableConfig
from airflow_framework.enums.ingestion_type import IngestionType

class MergeBigQueryODS(BigQueryOperator):
    """
    Merge data into a BigQuery ODS table.
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
        merge_type="SG_KEY",
        column_mapping: dict,
        ods_table_config: OdsTableConfig,
        **kwargs,
    ) -> None:
        super(MergeBigQueryODS, self).__init__(
            delegate_to=delegate_to,
            gcp_conn_id=gcp_conn_id,
            use_legacy_sql=False,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_NEVER",
            sql="sql",
            **kwargs,
        )
        self.project_id = project_id
        self.merge_type = merge_type
        self.stg_table_name = stg_table_name
        self.data_table_name = data_table_name
        self.stg_dataset_name = stg_dataset_name
        self.data_dataset_name = data_dataset_name
        self.surrogate_keys = surrogate_keys
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.column_mapping = column_mapping
        self.ods_table_config = ods_table_config

    def pre_execute(self, context) -> None:
        self.log.info(
            f"Execute BigQueryMergeTableOperator {self.stg_table_name}, {self.data_table_name}, {self.merge_type}"
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

        sql_helper = SqlHelperODS(
            source_dataset=self.stg_dataset_name,
            target_dataset=self.data_dataset_name ,
            source=self.stg_table_name,
            target=self.data_table_name,
            columns=columns,
            surrogate_keys=self.surrogate_keys,
            column_mapping=self.column_mapping,
            ods_metadata=self.ods_table_config.ods_metadata
        )

        if self.ods_table_config.ingestion_type == IngestionType.INCREMENTAL:
            # Append staging table to ODS table
            sql = sql_helper.create_upsert_sql_with_hash()
            write_disposition = "WRITE_APPEND"

        elif self.ods_table_config.ingestion_type == IngestionType.FULL:
            # Overwrite ODS table with the staging table data
            sql = sql_helper.create_truncate_sql()
            write_disposition = "WRITE_TRUNCATE"

        else:
            raise AirflowException("Invalid merge type", self.ingestion_type)

        logging.info(f"Executing sql: {sql}. Write disposition: {write_disposition}")

        self.sql = sql
        self.write_disposition = write_disposition