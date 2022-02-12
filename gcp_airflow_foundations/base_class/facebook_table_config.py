from dacite import Config
from dataclasses import dataclass, field
from pydantic import validator
from typing import List, Optional

from gcp_airflow_foundations.enums.facebook import ApiObject


@dataclass
class FacebookTableConfig:
    """
    Attributes:
        api_object: The API object to query {insights, campaign, adset}
        breakdowns: How to break down the result. For more than one breakdown, only certain combinations are available.
        action_breakdowns: How to break down action results. Supports more than one breakdowns. Default value is ["action_type"].
    """

    api_object: Optional[ApiObject]
    breakdowns: Optional[List[str]]
    action_breakdowns: Optional[List[str]]

    @validator("api_object")
    def valid_fields(cls, v):
        if v is None:
            return ApiObject.INSIGHTS
        else:
            return v
