from dacite import Config
from dataclasses import dataclass, field
from pydantic import validator
from typing import List, Optional

from gcp_airflow_foundations.enums.facebook import ApiObject


@dataclass
class FacebookTableConfig:
    """
    Attributes:
        breakdowns:
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