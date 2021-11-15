import time
from typing import Any, Dict, List, Optional
from enum import Enum

import requests
import json

from airflow.exceptions import AirflowException
from airflow.providers.facebook.ads.hooks.ads import FacebookAdsReportingHook

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


class JobStatus(Enum):
    """Available options for facebook async task status"""

    COMPLETED = 'Job Completed'
    STARTED = 'Job Started'
    RUNNING = 'Job Running'
    FAILED = 'Job Failed'
    SKIPPED = 'Job Skipped'


class CustomFacebookAdsReportingHook(FacebookAdsReportingHook):
    """
    Hook for the Facebook Ads API

    :param facebook_conn_id: Airflow Facebook Ads connection ID
    :type facebook_conn_id: str
    :param api_version: The version of Facebook API. Default to None. If it is None,
        it will use the Facebook business SDK default version.
    :type api_version: Optional[str]

    """

    conn_name_attr = 'facebook_conn_id'
    default_conn_name = 'facebook_custom'

    def __init__(
        self,
        facebook_conn_id: str = default_conn_name,
        api_version: Optional[str] = None,
        **kwargs
    ) -> None:
        super(CustomFacebookAdsReportingHook, self).__init__(
            facebook_conn_id=facebook_conn_id,
            api_version=api_version,
            **kwargs
        )

        self.facebook_conn_id = facebook_conn_id
        self.api_version = api_version
        self.client_required_fields = ["app_id", "app_secret", "access_token"]
        self.config = self.facebook_ads_config      

    def _get_service(self, facebook_acc_id) -> FacebookAdsApi:
        """Returns Facebook Ads Client using a service account"""
        return FacebookAdsApi.init(
            app_id=self.config["app_id"],
            app_secret=self.config["app_secret"],
            access_token=self.config["access_token"],
            account_id=facebook_acc_id,
            api_version=self.api_version,
        )

    def bulk_facebook_report_async(
        self,
        facebook_acc_id: str,
        params: Dict[str, Any],
        fields: List[str],
        sleep_time: int = 5,
    ) -> List[AdsInsights]:
        """
        Pulls data from the Facebook Ads API
        :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: List[str]
        :param params: Parameters that determine the query for Facebook
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: Dict[str, Any]
        :param sleep_time: Time to sleep when async call is happening
        :type sleep_time: int
        :return: Facebook Ads API response, converted to Facebook Ads Row objects
        :rtype: List[AdsInsights]
        """
        api = self._get_service(facebook_acc_id=facebook_acc_id)
        ad_account = AdAccount(api.get_default_account_id(), api=api)
        _async = ad_account.get_insights(params=params, fields=fields, is_async=True)
        while True:
            request = _async.api_get()
            async_status = request[AdReportRun.Field.async_status]
            percent = request[AdReportRun.Field.async_percent_completion]
            self.log.info("%s %s completed, async_status: %s", percent, "%", async_status)
            if async_status == JobStatus.COMPLETED.value:
                self.log.info("Job run completed")
                break
            if async_status in [JobStatus.SKIPPED.value, JobStatus.FAILED.value]:
                message = f"{async_status}. Please retry."
                raise AirflowException(message)
            time.sleep(sleep_time)
        report_run_id = _async.api_get()["report_run_id"]
        report_object = AdReportRun(report_run_id, api=api)
        insights = report_object.get_insights()
        self.log.info("Extracting data from returned Facebook Ads Iterators")
        return list(insights)

    def bulk_facebook_report(
        self,
        facebook_acc_id: str,
        params: Dict[str, Any],
        fields: List[str],
        sleep_time: int = 5,
    ) -> List[AdsInsights]:
        """
        Pulls data from the Facebook Ads API
        :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: List[str]
        :param params: Parameters that determine the query for Facebook
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: Dict[str, Any]
        :param sleep_time: Time to sleep when async call is happening
        :type sleep_time: int
        :return: Facebook Ads API response, converted to Facebook Ads Row objects
        :rtype: List[AdsInsights]
        """
        api = self._get_service(facebook_acc_id=facebook_acc_id)
        ad_account = AdAccount(api.get_default_account_id(), api=api)
        insights = ad_account.get_insights(params=params, fields=fields, is_async=False)

        return list(insights)

    def get_active_accounts_from_bq(self, project_id, table_id) -> List[str]:
        sql = f"SELECT account_id FROM `{table_id}`"

        query_config = bigquery.QueryJobConfig(use_legacy_sql=False)

        client = bigquery.Client(project=project_id)

        df = client.query(sql, job_config=query_config).to_dataframe()

        return [f"act_{i}" for i in df.account_id]

    def get_all_accounts(self) -> List[str]:
        self.log.info("Extracting all accounts")

        user_id = self.config["user_id"]

        URL = f"https://graph.facebook.com/v12.0/{user_id}/adaccounts"
        params = {
            "access_token":self.config["access_token"],
            "limit":10000
        }

        accounts = requests.get(URL, params=params).json()['data']

        return [i['id'] for i in accounts]