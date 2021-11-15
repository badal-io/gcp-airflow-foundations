from typing import List, Optional
from datetime import datetime

from pydantic import validator, root_validator
from pydantic.dataclasses import dataclass

from facebook_business.adobjects.adsinsights import AdsInsights

from gcp_airflow_foundations.enums.facebook import Level, DatePreset, AccountLookupScope

valid_fields = {
    "account_name":AdsInsights.Field.account_name,
    "account_id":AdsInsights.Field.account_id,
    "attribution_setting":AdsInsights.Field.attribution_setting,
    "account_currency":AdsInsights.Field.account_currency,
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
    "unique_clicks":AdsInsights.Field.unique_clicks,
    "inline_link_clicks":AdsInsights.Field.inline_link_clicks,
    "unique_inline_link_click_ctr":AdsInsights.Field.unique_inline_link_click_ctr,
    "inline_link_click_ctr":AdsInsights.Field.inline_link_click_ctr,
    "unique_inline_link_clicks":AdsInsights.Field.unique_inline_link_clicks,
    "cost_per_unique_inline_link_click":AdsInsights.Field.cost_per_unique_inline_link_click,
    "cost_per_unique_outbound_click":AdsInsights.Field.cost_per_unique_outbound_click,
    "cost_per_unique_click":AdsInsights.Field.cost_per_unique_click,
    "cost_per_thruplay":AdsInsights.Field.cost_per_thruplay,
    "video_30_sec_watched_actions":AdsInsights.Field.video_30_sec_watched_actions,
    "video_p25_watched_actions":AdsInsights.Field.video_p25_watched_actions,
    "video_p50_watched_actions":AdsInsights.Field.video_p50_watched_actions,
    "video_p75_watched_actions":AdsInsights.Field.video_p75_watched_actions,
    "video_p100_watched_actions":AdsInsights.Field.video_p100_watched_actions,
    "video_play_actions":AdsInsights.Field.video_play_actions,
    "conversion_values":AdsInsights.Field.conversion_values,
    "conversions":AdsInsights.Field.conversions,
    "cost_per_conversion":AdsInsights.Field.cost_per_conversion,
    "actions":AdsInsights.Field.actions,
    "cost_per_action_type":AdsInsights.Field.cost_per_action_type
}


@dataclass
class FacebookConfig:
    fields: List[str]
    level: Level
    account_lookup_scope: AccountLookupScope
    time_increment: Optional[str]
    time_range: Optional[dict]

    @validator("fields")
    def valid_fields(cls, v):
        for field in v:
            assert field in valid_fields, f"`{field}` is not a valid field for the Facebook API"

        return [valid_fields[field] for field in v]