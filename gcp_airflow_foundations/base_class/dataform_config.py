from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field

from datetime import datetime
from typing import List, Optional

from gcp_airflow_foundations.base_class.ods_metadata_config import OdsTableMetadataConfig

@dataclass
class DataformConfig:
    """
    Dataform configuration class.

    Attributes:
        environment: Only production is currently supported.
        project_id: Dataform project ID
        schedule: Schedules can be used to run any section of a dataset at a user-defined frequency.
        tags: Dataform operation tags
    """
    
    environment: str
    project_id: str
    schedule: str
    tags: List[str]
