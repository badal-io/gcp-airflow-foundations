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

from gcp_airflow_foundations.operators.gcp.schema_migration.schema_migration_audit import SchemaMigrationAudit
from operators.gcp.dlp.dlp_helpers import get_dlp_results_sql


class DlpBQInspectionResultsOperator(BaseOperator):
    """
    Reads DLP inspection results from BigQuery .

    :param project_id: GCP project ID
    :type project_id: str
    :param table_id: Target table name
    :type table_id: str
    :param dataset_id: Target dataset name
    :type dataset_id: str
    :param min_match_count: Minimum number of findings per column/likelihood level pair
    :type min_match_count: int
    """

    @apply_defaults
    def __init__(
            self,
            *,
            dataset_id,
            table_id,
            project_id,
            min_match_count=0,
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.min_match_count = min_match_count

        self.hook =  BigQueryHook(
            gcp_conn_id=gcp_conn_id,
            use_legacy_sql=False,
            impersonation_chain=impersonation_chain
        )
        conn = self.hook.get_conn()
        self.cursor = conn.cursor()

    def execute(self, context):
        sql = get_dlp_results_sql(self.project_id, self.dataset_id, self.table_id, self.min_match_count)
        results = self.hook.get_records(sql)

        logging.debug("DLP Results are", results)
        return results
