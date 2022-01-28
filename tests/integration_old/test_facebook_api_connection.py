from airflow.hooks.base_hook import BaseHook
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi


def test_facebook_api():
    conn = BaseHook.get_connection("facebook_default")
    config = conn.extra_dejson

    api = FacebookAdsApi.init(
        app_id=config["app_id"],
        app_secret=config["app_secret"],
        access_token=config["access_token"],
        account_id=config["account_id"],
    )

    my_account = AdAccount(api.get_default_account_id(), api=api)
    campaigns = my_account.get_campaigns()
    assert len(campaigns) > 0, "Unable to retrieve campaigns from Facebook Insights API"
