from typing import Optional, List

# from airflow.utils.decorators import apply_defaults

from airflow.exceptions import AirflowException

import logging

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)


class CustomBigQueryCreateEmptyTableOperator(BigQueryCreateEmptyTableOperator):
    # @apply_defaults
    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        table_id: str,
        dag_table_id: str,
        cluster_fields: Optional[List[str]] = None,
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
        self.dataset_id = dataset_id
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.schema_task_id = f"{dag_table_id}.schema_parsing"

    def pre_execute(self, context) -> None:
        logging.info(self.schema_task_id)
        schema_fields = self.xcom_pull(context=context, task_ids=self.schema_task_id)[
            f"{self.dataset_id}.{self.table_id}"
        ]

        if self.cluster_fields:
            self.table_resource = {
                "schema": {"fields": schema_fields},
                "timePartitioning": self.time_partitioning,
                "encryptionConfiguration": None,
                "labels": None,
                "clustering": {"fields": self.cluster_fields},
            }
        else:
            self.table_resource = {
                "schema": {"fields": schema_fields},
                "timePartitioning": self.time_partitioning,
                "encryptionConfiguration": None,
                "labels": None,
            }
