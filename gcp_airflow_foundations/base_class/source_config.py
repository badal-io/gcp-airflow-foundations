import datetime
from typing import List, Optional
from croniter import croniter
from pydantic import validator, root_validator
from pydantic.dataclasses import dataclass

from gcp_airflow_foundations.enums.source_type import SourceType
from gcp_airflow_foundations.base_class.landing_zone_config import LandingZoneConfig
from gcp_airflow_foundations.base_class.schema_options_config import SchemaOptionsConfig
from gcp_airflow_foundations.base_class.facebook_config import FacebookConfig

partition_limit = 4000
ms_day = 86400000
expiration_options = {
    "@hourly":partition_limit/24,
    "@daily":partition_limit, 
    "@monthly":partition_limit*30
}


@dataclass
class SourceConfig:
    """
    Source configuration data class.

    Attributes:
        name : Name of source
        source_type : Source type selection. See SourceType class
        ingest_schedule : Ingestion schedule. Currently only supporting @hourly, @daily, @weekly, and @monthly
        gcp_project : Google Cloud Platform project ID
        dataset_data_name : Target dataset name
        connection : Aiflow Google Cloud Platform connection
        extra_options : Google Cloud Storage bucket and objects for source data if loading from GCS
        landing_zone_options : Staging dataset name
        acceptable_delay_minutes : Delay minutes limit
        notification_emails : Email address for notification emails
        owner : Airflow user owning the DAG
        partition_expiration: Expiration time for HDS Snapshot partitions in days.
        facebook_options: Extra options for ingesting data from Facebook Marketing API.
        dag_args: Optional dictionary of parameters to be passed as keyword arguments to the ingestion DAG. 
                    Refer to :class:`airflow.models.dag.DAG` for the available parameters.
        location: BigQuery job location.
        start_date : Start date for DAG
        start_date_tz : Timezone
        version : The Dag version. Can be incremented if logic changes
        sla_mins : Service Level Agreement (SLA) timeout minutes. This is is an expectation for the maximum time a Task should take.
    """
    name: str
    source_type: str
    ingest_schedule: str
    external_dag_id: Optional[str]
    gcp_project: str
    dataset_data_name: str
    dataset_hds_override: Optional[str]
    connection: str
    extra_options: Optional[dict]
    landing_zone_options: LandingZoneConfig
    acceptable_delay_minutes: int
    notification_emails: List[str]
    owner: str
    partition_expiration: Optional[int]
    schema_options: SchemaOptionsConfig
    facebook_options: Optional[FacebookConfig]
    dag_args: Optional[dict]
    location: str
    start_date: str
    start_date_tz: str = "EST"
    version: int = 1
    sla_mins: int = 900

    @validator("name")
    def valid_name(cls, v):
        assert v, "Source name must not be empty"
        assert "." not in v, "Source Name cannot contain the period character"
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

    @validator("landing_zone_options")
    def valid_landing_zone(cls, v):
        assert v.landing_zone_dataset, "Landing zone dataset name must not be empty"
        return v

    @validator("start_date")
    def valid_start_date(cls, v):
        assert datetime.datetime.strptime(v, "%Y-%m-%d"), \
            "The date format for Start Date should be YYYY-MM-DD"
        return v

    @validator("schema_options")
    def valid_schema_options(cls, v):
        assert v is not None, 'Schema options configuration must be provided'
        return v

    @root_validator(pre=True)
    def valid_partition_expiration(cls, values):
        if values['partition_expiration'] is not None:
            assert values['partition_expiration'] < expiration_options[values['ingest_schedule']], \
                f"The partition limit should be smaller than {expiration_options[values['ingest_schedule']]} days. It is currently set to {values['partition_expiration']}"
            values['partition_expiration'] = values['partition_expiration']*ms_day
        return values

    @root_validator(pre=True)
    def valid_hds_dataset(cls, values):
        if values['dataset_hds_override'] is None:
            values['dataset_hds_override'] = values['dataset_data_name']

        return values
