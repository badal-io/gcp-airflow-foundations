import time
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.providers.facebook.ads.hooks.ads import FacebookAdsReportingHook

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

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
        facebook_acc_id: str,
        facebook_conn_id: str = default_conn_name,
        api_version: Optional[str] = None,
        **kwargs
    ) -> None:
        super(CustomFacebookAdsReportingHook, self).__init__(
            facebook_conn_id=facebook_conn_id,
            api_version=api_version,
            **kwargs
        )

        self.facebook_acc_id = facebook_acc_id
        self.facebook_conn_id = facebook_conn_id
        self.api_version = api_version
        self.client_required_fields = ["app_id", "app_secret", "access_token"]

    def _get_service(self) -> FacebookAdsApi:
        """Returns Facebook Ads Client using a service account"""
        config = self.facebook_ads_config
        return FacebookAdsApi.init(
            app_id=config["app_id"],
            app_secret=config["app_secret"],
            access_token=config["access_token"],
            account_id=self.facebook_acc_id,
            api_version=self.api_version,
        )

    def bulk_facebook_report(
        self,
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
        api = self._get_service()
        ad_account = AdAccount(api.get_default_account_id(), api=api)
        insights = ad_account.get_insights(params=params, fields=fields, is_async=False)

        self.log.info(f"Extracting data for account {api.get_default_account_id()} completed")

        return list(insights)