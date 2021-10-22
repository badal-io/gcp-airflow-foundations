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

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator
)


class CustomBigQueryCreateEmptyTableOperator(BigQueryCreateEmptyTableOperator):
    @apply_defaults
    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        table_id: str,
        time_partitioning: Optional[dict] = None,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super(CustomBigQueryCreateEmptyTableOperator, self).__init__(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            table_resource=None,
            exists_ok=True,
            **kwargs,
        )

        self.table = table_id
        self.time_partitioning = time_partitioning

    def pre_execute(self, context) -> None:
        schema_fields = self.xcom_pull(context=context, task_ids="schema_parsing")[self.table_id]

        self.table_resource={
                "schema":{'fields': schema_fields},
                "timePartitioning":self.time_partitioning,
                "encryptionConfiguration":None,
                "labels":None
        }