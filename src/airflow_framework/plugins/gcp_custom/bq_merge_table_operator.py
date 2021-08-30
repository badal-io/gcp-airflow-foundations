from typing import Optional

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
    BigQueryCreateEmptyTableOperator,
)
from enum import Enum, unique
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from airflow.exceptions import AirflowException

import logging

from airflow_framework.plugins.gcp_custom.sql_upsert_helpers import create_upsert_sql, create_upsert_sql_with_hash


@unique
class MergeType(Enum):
    """ Method for matching records  """

    SG_KEY = "SG_KEY"  # 'use surrogate keys to match records'
    SG_KEY_WITH_HASH = "SG_KEY_WITH_HASH" # 'add row hash and inserted_at but still use surrogate keys to match records'
    # HASH_KEY = "HASH_KEY"              # 'add row hash and inserted_at but and use hash to match records'


class BigQueryMergeTableOperator(BigQueryOperator):
    """
    Merge data into a table BigQuery table
    Should be used mostly for CDC or Type2 SCDs
    Assumes the target table schema is right - i.e if any schema changes (specifically adding columns) needed to be done, they have been done upstream
    """

    template_fields = (
        "stg_table_name",
        "data_table_name",
        "stg_dataset_name",
        "stg_dataset_name",
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
        update_columns: [str],
        delegate_to: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        merge_type=MergeType.SG_KEY,
        column_mapping: dict,
        **kwargs,
    ) -> None:
        super(BigQueryMergeTableOperator, self).__init__(
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
        self.update_columns = update_columns
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.column_mapping = column_mapping

    def pre_execute(self, context) -> None:
        self.log.info(
            f"Execute BigQueryMergeTableOperator {self.stg_table_name}, {self.data_table_name}, {self.merge_type}"
        )

        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            # impersonation_chain=self.impersonation_chain,
        )
        conn = hook.get_conn()
        bq_cursor = conn.cursor()

        schema = bq_cursor.get_schema(
            dataset_id=self.stg_dataset_name, table_id=self.stg_table_name
        )
        self.log.info("Target table schema is : %s", schema)

        columns: list[str] = list(map(lambda x: x["name"], schema["fields"]))

        sql = ""

        if self.merge_type.value == MergeType.SG_KEY.value:
            sql = create_upsert_sql(
                self.stg_dataset_name,
                self.data_dataset_name,
                self.stg_table_name,
                self.data_table_name,
                self.surrogate_keys,
                self.update_columns,
                self.column_mapping
            )
        elif self.merge_type.value == MergeType.SG_KEY_WITH_HASH.value:
            sql = create_upsert_sql_with_hash(
                self.stg_dataset_name,
                self.data_dataset_name,
                self.stg_table_name,
                self.data_table_name,
                self.surrogate_keys,
                self.update_columns,
                columns,
                self.column_mapping
            )
        else:
            raise AirflowException("Invalid merge type", self.merge_type)

        logging.info(f"Executing sql: {sql}")

        self.sql = sql