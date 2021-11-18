from typing import Optional

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator
)

from airflow.utils.decorators import apply_defaults

from airflow.exceptions import AirflowException

import logging


class BigQueryDeleteStagingTableOperator(BigQueryDeleteTableOperator):
    @apply_defaults
    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        table_id: str,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:

        deletion_dataset_table = f"{project_id}.{dataset_id}.{table_id}"

        super(BigQueryDeleteStagingTableOperator, self).__init__(
            deletion_dataset_table=deletion_dataset_table,
            ignore_if_missing=True,
            **kwargs
        )

    def pre_execute(self, context) -> None:
        ds = context['ds']
        self.deletion_dataset_table = f"{self.deletion_dataset_table}_{ds}"
