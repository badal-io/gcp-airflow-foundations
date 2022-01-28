import logging
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.decorators import apply_defaults

from gcp_airflow_foundations.operators.gcp.dlp.dlp_helpers import get_dlp_results_sql


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
        do_xcom_push=True,
        gcp_conn_id="google_cloud_default",
        impersonation_chain=None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.min_match_count = min_match_count

        self.hook = BigQueryHook(
            gcp_conn_id=gcp_conn_id,
            use_legacy_sql=False,
            impersonation_chain=impersonation_chain,
        )
        conn = self.hook.get_conn()
        self.cursor = conn.cursor()

    def execute(self, context):
        sql = get_dlp_results_sql(
            self.project_id, self.dataset_id, self.table_id, self.min_match_count
        )
        results = self.hook.get_records(sql)

        logging.info(f"DLP Results are {results} ")
        if self.do_xcom_push:
            self.xcom_push(context, "results", results)
        return results
