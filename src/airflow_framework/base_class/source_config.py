import datetime
from typing import List

from pydantic import validator
from pydantic.dataclasses import dataclass

from airflow_framework.source_type import SourceType
from airflow_framework.base_class.landing_zone_config import LandingZoneConfig


@dataclass
class SourceConfig:
    name: str
    source_type: SourceType
    ingest_schedule: str
    gcp_project: str
    dataset_data_name: str
    connection: str
    extra_options: dict
    landing_zone_options: LandingZoneConfig
    acceptable_delay_minutes: int
    notification_emails: List[str]
    owner: str
    start_date: str
    start_date_tz: str = "EST"
    version: int = 1
    sla_mins: int = 900

    @validator("name")
    def valid_name(cls, v):
        assert v, "Name must not be empty"
        return v

    @validator("source_type")
    def valid_source_type(cls, v):
        assert v, "Source type must not be empty"
        return v

    @validator("ingest_schedule")
    def valid_ingest_schedule(cls, v):
        assert (v in ["@hourly", "@daily", "@weekly", "@monthly"] or croniter.is_valid(v)), \
            "invalid ingest schedule: see Airflow documentation for more details"
        return v

    @validator("landing_zone")
    def valid_landing_zone(cls, v):
        assert v, "Landing zone table name must not be empty"
        return v

    @validator("ods_target")
    def valid_ods_target(cls, v):
        assert v, "ODS target table name must not be empty"
        return

    @validator("start_date")
    def valid_start_date(cls, v):
        assert datetime.datetime.strptime(v, "%Y-%m-%d"), \
            "The date format for Start Date should be YYYY-MM-DD"
