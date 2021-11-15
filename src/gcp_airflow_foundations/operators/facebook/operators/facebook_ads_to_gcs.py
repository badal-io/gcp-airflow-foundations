import csv
import tempfile
import warnings
from typing import Any, Dict, List, Optional, Sequence, Union
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pyarrow.parquet as pq
import pyarrow

from airflow.exceptions import AirflowException

from gcp_airflow_foundations.operators.facebook.hooks.ads import CustomFacebookAdsReportingHook
from gcp_airflow_foundations.enums.facebook import AccountLookupScope

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
        gcp_project: str,
        account_lookup_scope: AccountLookupScope,
        destination_project_dataset_table: str,
        fields: List[str],
        parameters: Dict[str, Any] = None,
        time_range: Dict[str, Any] = None,
        api_version: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        facebook_conn_id: str = "facebook_custom",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super(FacebookAdsReportToBqOperator, self).__init__(
            **kwargs
        )

        self.gcp_project = gcp_project
        self.account_lookup_scope = account_lookup_scope
        self.destination_project_dataset_table = destination_project_dataset_table
        self.gcp_conn_id = gcp_conn_id
        self.facebook_conn_id = facebook_conn_id
        self.api_version = api_version
        self.fields = fields
        self.parameters = parameters
        self.time_range = time_range
        self.impersonation_chain = impersonation_chain

    def execute(self, context: dict):
        ds = context['ds']
        interval_start = datetime.strptime(ds, '%Y-%m-%d')
        interval_end = interval_start + relativedelta(day=31)
        
        if not self.time_range:
            self.parameters['time_range'] = {'since':ds, 'until':ds}
        else:
            self.parameters['time_range'] = {'since':self.time_range['since'], 'until':ds}
        
        #self.parameters['time_range'] = {'since':'2020-01-01', 'until':ds}
        #self.parameters['time_range'] = {
        #    'since':interval_start.strftime('%Y-%m-%d'), 
        #    'until':interval_end.strftime('%Y-%m-%d')
        #}

        self.log.info("Currently loading data for date range: %s", self.parameters['time_range'])

        service = CustomFacebookAdsReportingHook(
            facebook_conn_id=self.facebook_conn_id, api_version=self.api_version
        )

        if self.account_lookup_scope == AccountLookupScope.ALL:
            facebook_acc_ids = service.get_all_accounts()

        elif self.account_lookup_scope == AccountLookupScope.ACTIVE:
            facebook_acc_ids = service.get_active_accounts_from_bq(
                project_id=self.gcp_project, 
                table_id="dev-eyereturn-data-warehouse.facebook_dev.accounts_ODS_Full"
            )

        converted_rows = []
        for facebook_acc_id in facebook_acc_ids:
            self.log.info("Currently loading data from Account ID: %s", facebook_acc_id)
        
            rows = service.bulk_facebook_report(facebook_acc_id=facebook_acc_id, params=self.parameters, fields=self.fields)

            converted_rows.extend(
                [dict(row) for row in rows]
            )

            self.log.info("Extracting data for account %s completed", facebook_acc_id)

        self.log.info("Facebook Returned %s data points", len(converted_rows))

        self.transform_data_types(converted_rows)

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

    def transform_data_types(self, rows):
        for i in rows:
            for j in i:
                if j.endswith('id'):
                    continue
                elif j in ('date_start', 'date_stop'):
                    i[j] = datetime.strptime(i[j], '%Y-%m-%d').date()
                elif type(i[j]) == str:
                    i[j] = self.get_float(i[j])
                elif type(i[j]) == list:
                    for k in i[j]:
                        for w in k:
                            if (type(k[w]) == str) and (not w.endswith('id')):
                                k[w] = self.get_float(k[w])

    def get_float(self, element):
        try:
            return float(element)
        except ValueError:
            return element