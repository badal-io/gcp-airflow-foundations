from typing import Optional, List, Sequence

from airflow.utils.decorators import apply_defaults

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from google.cloud.bigquery.dataset import AccessEntry, Dataset, DatasetListItem
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class CustomBigQueryCreateEmptyDatasetOperator(BaseOperator):
    """
    This operator is used to create new dataset for your Project in BigQuery.
    :param project_id: The name of the project where we want to create the dataset.
    :param dataset_id: The id of dataset. Don't need to provide, if datasetId in dataset_reference.
    :param location: The geographic location where the dataset should reside.
    :param exists_ok: If ``True``, ignore "already exists" errors when creating the dataset.
    """

    template_fields: Sequence[str] = (
        'dataset_id',
        'project_id'
    )

    ui_color = "#5F86FF"

    def __init__(
        self,
        *,
        dataset_id: Optional[str] = None,
        project_id: Optional[str] = None,
        location: Optional[str] = None,
        exists_ok: bool = False,
        gcp_conn_id: str = 'google_cloud_default',
        **kwargs,
    ) -> None:

        self.dataset_id = dataset_id
        self.project_id = project_id
        self.location = location
        self.exists_ok = exists_ok
        self.gcp_conn_id = gcp_conn_id

        super().__init__(**kwargs)

    def execute(self, context: 'Context') -> None:
        dataset_reference = {
            "datasetReference": {
                "datasetId":self.dataset_id, 
                "projectId":self.project_id
            }, 
            "location":self.location
        }

        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location
        )

        bq_client = bq_hook.get_client(
            project_id=self.project_id,
            location=self.location
        )

        dataset: Dataset = Dataset.from_api_repr(dataset_reference)

        self.log.info('Creating dataset: %s in project: %s ', self.dataset_id, self.project_id)
        bq_client.create_dataset(dataset=dataset, exists_ok=self.exists_ok)
        self.log.info('Dataset created successfully.')
