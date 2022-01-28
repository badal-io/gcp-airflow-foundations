from airflow.contrib.operators.bigquery_operator import (
    BigQueryUpdateTableSchemaOperator,
)
from operators.gcp.dlp.dlp_helpers import fields_to_policy_tags
from typing import Optional


class UpdatePolicyTagsOperator(BigQueryUpdateTableSchemaOperator):
    template_fields = ("dataset_id", "table_id", "fields", "policy_tag")

    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        project_id: str,
        fields: list[str],
        policy_tag: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        **kwargs,
    ) -> None:
        schema_fields_updates = fields_to_policy_tags(fields, policy_tag)
        super(BigQueryUpdateTableSchemaOperator, self).__init__(
            delegate_to=delegate_to,
            gcp_conn_id=gcp_conn_id,
            dataset_id=dataset_id,
            project_id=project_id,
            table_id=table_id,
            schema_fields_updates=schema_fields_updates,
            include_policy_tags=True,
            **kwargs,
        )
