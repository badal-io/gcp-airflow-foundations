from typing import List, Optional
from datetime import datetime

from pydantic import validator, root_validator
from pydantic.dataclasses import dataclass

from enum import Enum, unique

from facebook_business.adobjects.adsinsights import AdsInsights


#TODO add support for additional fields
valid_fields = {
    "campaign_name":AdsInsights.Field.campaign_name,
    "campaign_id":AdsInsights.Field.campaign_id,    
    "ad_id":AdsInsights.Field.ad_id,
    "clicks":AdsInsights.Field.clicks,
    "impressions":AdsInsights.Field.impressions,
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
    fields: List[str]
    level: Level
    date_preset: Optional[DatePreset]
    time_range_since: Optional[str]
    time_range_until: Optional[str]

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

            values["time_range_since"] = datetime.strptime(values["time_range_since"], "%Y-%m-%d")
            values["time_range_until"] = datetime.strptime(values["time_range_until"], "%Y-%m-%d")

        return values

    @validator("fields")
    def valid_fields(cls, v):
        for field in v:
            assert field in valid_fields, f"`{field}` is not a valid field for the ADs Insights API"

        return [valid_fields[field] for field in v]