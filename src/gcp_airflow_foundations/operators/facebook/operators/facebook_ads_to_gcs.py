import csv
import tempfile
import warnings
from typing import Any, Dict, List, Optional, Sequence, Union
import pandas as pd

import pyarrow.parquet as pq
import pyarrow

from airflow.exceptions import AirflowException

from gcp_airflow_foundations.operators.facebook.hooks.ads import CustomFacebookAdsReportingHook

from airflow.models import BaseOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from google.cloud import bigquery


class FacebookAdsReportToBqOperator(BaseOperator):
    """
    Fetches the results from the Facebook Ads API as desired in the params
    Converts and saves the data as a temporary JSON file
    Uploads the JSON to Google Cloud Storage

    :param bucket_name: The GCS bucket to upload to
    :type bucket_name: str
    :param object_name: GCS path to save the object. Must be the full file path (ex. `path/to/file.txt`)
    :type object_name: str
    :param gcp_conn_id: Airflow Google Cloud connection ID
    :type gcp_conn_id: str
    :param facebook_conn_id: Airflow Facebook Ads connection ID
    :type facebook_conn_id: str
    :param api_version: The version of Facebook API. Default to None. If it is None,
        it will use the Facebook business SDK default version.
    :type api_version: str
    :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
        https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
    :type fields: List[str]
    :param params: Parameters that determine the query for Facebook. This keyword is deprecated,
        please use `parameters` keyword to pass the parameters.
        https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
    :type params: Dict[str, Any]
    :param parameters: Parameters that determine the query for Facebook
        https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
    :type parameters: Dict[str, Any]
    :param gzip: Option to compress local file or file data for upload
    :type gzip: bool
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "facebook_conn_id",
        "impersonation_chain",
        "parameters",
    )

    def __init__(
        self,
        *,
        facebook_acc_ids: List[str],
        gcp_project: str,
        destination_project_dataset_table: str,
        fields: List[str],
        parameters: Dict[str, Any] = None,
        api_version: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        facebook_conn_id: str = "facebook_custom",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super(FacebookAdsReportToBqOperator, self).__init__(
            **kwargs
        )

        self.facebook_acc_ids = facebook_acc_ids
        self.gcp_project = gcp_project
        self.destination_project_dataset_table = destination_project_dataset_table
        self.gcp_conn_id = gcp_conn_id
        self.facebook_conn_id = facebook_conn_id
        self.api_version = api_version
        self.fields = fields
        self.parameters = parameters
        self.impersonation_chain = impersonation_chain

    def execute(self, context: dict):
        ds = context['ds']

        self.parameters['time_range'] = {'since':ds,'until':ds}

        self.log.info("Currently loading data for date range: %s", self.parameters['time_range'])

        converted_rows = []
        for facebook_acc_id in self.facebook_acc_ids:
            self.log.info("Currently loading data from Account ID: %s", facebook_acc_id)

            service = CustomFacebookAdsReportingHook(
                facebook_acc_id=facebook_acc_id, facebook_conn_id=self.facebook_conn_id, api_version=self.api_version
            )
            
            rows = service.bulk_facebook_report(params=self.parameters, fields=self.fields)

            converted_rows.extend(
                [dict(row) for row in rows]
            )

        self.log.info("Facebook Returned %s data points", len(converted_rows))

        df = pd.DataFrame.from_dict(converted_rows)

        writer = pyarrow.BufferOutputStream()
        pq.write_table(
            pyarrow.Table.from_pandas(df),
            writer,
            use_compliant_nested_type=True
        )
        reader = pyarrow.BufferReader(writer.getvalue())

        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id
        )

        client = hook.get_client(project_id=self.gcp_project)

        parquet_options = bigquery.format_options.ParquetOptions()
        parquet_options.enable_list_inference = True

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.parquet_options = parquet_options
        job_config.write_disposition='WRITE_TRUNCATE'

        job = client.load_table_from_file(
            reader, self.destination_project_dataset_table, job_config=job_config
        )
