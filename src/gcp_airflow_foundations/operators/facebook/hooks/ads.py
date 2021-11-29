import time
from typing import Any, Dict, List, Optional
from enum import Enum
from datetime import datetime
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
    Custom Hook for the Facebook Ads API. It extends the default FacebookAdsReportingHook.

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

    def _get_service(
        self, 
        facebook_acc_id
    ) -> FacebookAdsApi:
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
        Pulls data from the Facebook Ads API using async calls.
        :param facebook_acc_id: The Facebook account ID to pull data from.
        :type facebook_acc_id: str
        :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: List[str]
        :param params: Parameters that determine the query for Facebook
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: Dict[str, Any]
        :param sleep_time: Time to sleep when async call is happening
        :type sleep_time: int
        :return: Facebook Ads API response, converted to rows.
        :rtype: List[dict]
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

        max_current_usage = self.usage_throttle(insights)

        if max_current_usage >= 75:
            return -1

        self.log.info("Extracting data from returned Facebook Ads Iterators")

        rows = []
        while True:
            max_current_usage = self.usage_throttle(insights)
            if max_current_usage >= 75:
                self.log.info('75% Rate Limit Reached. Cooling Time 5 Minutes.')
                time.sleep(300)
            try:
                rows.append( next(insights) )
            except StopIteration:
                break

        return [dict(row) for row in rows]

    def bulk_facebook_report(
        self,
        facebook_acc_id: str,
        params: Dict[str, Any],
        fields: List[str],
        sleep_time: int = 5,
    ) -> List[AdsInsights]:
        """
        Pulls data from the Facebook Ads API using sync calls.
        :param facebook_acc_id: The Facebook account ID to pull data from.
        :type facebook_acc_id: str
        :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: List[str]
        :param params: Parameters that determine the query for Facebook
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: Dict[str, Any]
        :return: Facebook Ads API response, converted to rows.
        :rtype: List[dict]
        """

        api = self._get_service(facebook_acc_id=facebook_acc_id)
        ad_account = AdAccount(api.get_default_account_id(), api=api)
        insights = ad_account.get_insights(params=params, fields=fields, is_async=False)
        rows = list(insights)

        self.usage_throttle(insights)

        return [dict(row) for row in rows]

    def get_campaigns(
        self,
        facebook_acc_id: str,
        params: Dict[str, Any]
    ) -> List[dict]:
        """
        Pulls campaign data from the Facebook Ads API using sync calls.
        :param facebook_acc_id: The Facebook account ID to pull data from.
        :type facebook_acc_id: str
        :param params: Parameters that determine the query for Facebook
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type params: Dict[str, Any]
        :return: Facebook Ads API response, converted to rows.
        :rtype: List[dict]
        """

        api = self._get_service(facebook_acc_id=facebook_acc_id)
        ad_account = AdAccount(api.get_default_account_id(), api=api)

        campaigns = ad_account.get_campaigns(
            params={'limit':'20000','time_range':params['time_range']},
            fields=[
            'account_id',
            #'name', TO-DO: troubleshoot why pyarrow fails to convert the `name` column
            'daily_budget',
            'effective_status',
            'lifetime_budget',
            'start_time',
            'stop_time'
            ]
        )

        rows = []
        for row in campaigns:
            converted_row = row._data
            if 'name' in converted_row:
                converted_row['name'] = str(converted_row['name'])
            if 'start_time' in converted_row:
                converted_row['start_time'] = datetime.strptime(converted_row['start_time'],'%Y-%m-%dT%H:%M:%S%z')
            if 'stop_time' in row:
                converted_row['stop_time'] = datetime.strptime(converted_row['stop_time'],'%Y-%m-%dT%H:%M:%S%z')
            rows.append(converted_row)

        return rows

    def get_adsets(
        self,
        facebook_acc_id: str,
        params: Dict[str, Any]
    ) -> List[dict]:
        """
        Pulls adset data from the Facebook Ads API using sync calls.
        :param facebook_acc_id: The Facebook account ID to pull data from.
        :type facebook_acc_id: str
        :param params: Parameters that determine the query for Facebook
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: Dict[str, Any]
        :return: Facebook Ads API response, converted to rows.
        :rtype: List[dict]
        """

        api = self._get_service(facebook_acc_id=facebook_acc_id)
        ad_account = AdAccount(api.get_default_account_id(), api=api)

        adsets = ad_account.get_ad_sets(
            params={'limit':'20000','time_range':params['time_range']},
            fields=[
                'account_id',
                'name',
                'daily_budget',
                'effective_status',
                'lifetime_budget',
                'created_time',
                'end_time'
            ]
        )

        rows = []
        for row in adsets:
            converted_row = row._data
            if 'name' in converted_row:
                converted_row['name'] = str(converted_row['name'])
            if 'created_time' in converted_row:
                converted_row['created_time'] = datetime.strptime(converted_row['created_time'],'%Y-%m-%dT%H:%M:%S%z')
            if 'end_time' in row:
                converted_row['end_time'] = datetime.strptime(converted_row['end_time'],'%Y-%m-%dT%H:%M:%S%z')
            rows.append(converted_row)

        return rows

    def get_active_accounts_from_bq(
        self, 
        project_id, 
        table_id
     ) -> List[str]:
        """
        Pulls a list of Facebook account IDs from a BigQuery table.
        :param project_id: The Google Cloud Platform project ID.
        :type project_id: str
        :param table_id: Name of BigQuery table that contains the Facebook account IDS.
        :type table_id: str
        :return: A list with the Facebook account IDs.
        :rtype: List[str]
        """

        sql = f"SELECT account_id FROM `{table_id}`"

        query_config = bigquery.QueryJobConfig(use_legacy_sql=False)

        client = bigquery.Client(project=project_id)

        df = client.query(sql, job_config=query_config).to_dataframe()

        return [f"act_{i}" for i in df.account_id]

    def get_all_accounts(
        self
    ) -> List[str]:
        """
        Pulls a list of Facebook account IDs from the Facebook API.
        :return: A list with the Facebook account IDs.
        :rtype: List[str]
        """

        self.log.info("Extracting all accounts")

        user_id = self.config["user_id"]

        URL = f"https://graph.facebook.com/v12.0/{user_id}/adaccounts"
        params = {
            "access_token":self.config["access_token"],
            "limit":10000
        }

        accounts = requests.get(URL, params=params).json()['data']

        return [i['id'] for i in accounts]

    def usage_throttle(
        self, 
        insights
    ) -> int:
        """
        Queries the 'x-business-use-case-usage' header of the Cursor object returned by the Facebook API.
        """
        
        usage_header = json.loads(insights._headers['x-business-use-case-usage'])
        values = list(usage_header.values())[0][0]
        return max(values['call_count'], values['total_cputime'], values['total_time'])