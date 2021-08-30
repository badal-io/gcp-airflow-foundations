import datetime
from typing import List

from pydantic import validator
from pydantic.dataclasses import dataclass

from airflow_framework.source_type import SourceType
import pytz


@dataclass
class SourceConfig:
    name: str
    source_type: SourceType
    ingest_schedule: str
    gcp_project: str
    dataset_data_name: str
    connection: str
    extra_options: dict
    landing_zone_options: dict
    acceptable_delay_minutes: int
    notification_emails: List[str]
    owner: str
    start_date: str
    start_date_tz: str = "EST"
    version: int = 1
    sla_mins: int = 900

    @validator("ingest_schedule")
    def valid_ingest_schedule(cls, v):
        assert "@daily" == v, "only @daily schedule is currently supported"
        return v

    @validator("extra_options")
    def cloud_storage_options(cls, v, values, **kwargs):
        assert v["gcs_bucket"], "bucket_name must not be empty"
        return v