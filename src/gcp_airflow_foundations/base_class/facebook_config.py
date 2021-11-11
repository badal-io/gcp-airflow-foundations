from typing import List, Optional
from datetime import datetime

from pydantic import validator, root_validator
from pydantic.dataclasses import dataclass

from enum import Enum, unique

from facebook_business.adobjects.adsinsights import AdsInsights


#TODO add support for additional fields
valid_fields = {
    "account_name":AdsInsights.Field.account_name,
    "account_id":AdsInsights.Field.account_id,
    "campaign_name":AdsInsights.Field.campaign_name,
    "campaign_id":AdsInsights.Field.campaign_id,    
    "ad_id":AdsInsights.Field.ad_id,
    "impressions":AdsInsights.Field.impressions,
    "spend":AdsInsights.Field.spend,
    "reach":AdsInsights.Field.reach,
    "clicks":AdsInsights.Field.clicks,
    "cpc":AdsInsights.Field.cpc,
    "ctr":AdsInsights.Field.ctr,
    "cpm":AdsInsights.Field.cpm,
    "inline_link_clicks":AdsInsights.Field.inline_link_clicks,
    "inline_link_click_ctr":AdsInsights.Field.inline_link_click_ctr,
    "cost_per_unique_click":AdsInsights.Field.cost_per_unique_click,
    "video_30_sec_watched_actions":AdsInsights.Field.video_30_sec_watched_actions,
    "video_p25_watched_actions":AdsInsights.Field.video_p25_watched_actions,
    "video_p50_watched_actions":AdsInsights.Field.video_p50_watched_actions,
    "video_p75_watched_actions":AdsInsights.Field.video_p75_watched_actions,
    "video_p100_watched_actions":AdsInsights.Field.video_p100_watched_actions,
    "video_play_actions":AdsInsights.Field.video_play_actions,
    "conversion_values":AdsInsights.Field.conversion_values,
    "conversions":AdsInsights.Field.conversions,
    "cost_per_conversion":AdsInsights.Field.cost_per_conversion,
    "cost_per_action_type":AdsInsights.Field.cost_per_action_type
}

@unique
class Level(Enum):
    AD = "ad"
    ADSET = "adset"
    CAMPAIGN = "campaign"
    ACCOUNT = 'account'


@unique
class DatePreset(Enum):
    TODAY = "today"
    YESTERDAY = "yesterday"
    THIS_MONTH = "this_month"
    LAST_MONTH = "last_month"
    MAXIMUM = "maximum"


@dataclass
class FacebookConfig:
    accounts: List[str]
    fields: List[str]
    level: Level
    date_preset: Optional[DatePreset]
    time_range_since: Optional[str]
    time_range_until: Optional[str]
    time_increment: Optional[int]

    @root_validator(pre=True)
    def valid_date_preset(cls, values):
        if values["date_preset"] is not None:
            assert (values["time_range_since"] is None and values["time_range_until"] is None), \
                "Either `date_preset` or `time_range` must be provided but not both"

        return values

    @root_validator(pre=True)
    def valid_time_range(cls, values):
        if values["time_range_since"] is not None:
            assert (values["time_range_until"] is not None), \
                "`time_range_until` cannot be empty"  

            assert datetime.strptime(values["time_range_since"], "%Y-%m-%d"), \
                "The date format for `time_range_since` should be YYYY-MM-DD"

            assert datetime.strptime(values["time_range_until"], "%Y-%m-%d"), \
                "The date format for `time_range_until` should be YYYY-MM-DD"

        return values

    @validator("fields")
    def valid_fields(cls, v):
        for field in v:
            assert field in valid_fields, f"`{field}` is not a valid field for the Facebook API"

        return [valid_fields[field] for field in v]

    @validator("accounts")
    def valid_accounts(cls, v):
        formatted_accounts = []
        for account in v:
            if not account.startswith("act_"):
                account = f"act_{account}"
            formatted_accounts.append(account)

        return formatted_accounts