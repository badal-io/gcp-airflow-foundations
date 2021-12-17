from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field

from datetime import datetime
from typing import List, Optional


@dataclass
class DataformConfig:
    """
    Dataform configuration class.

    Attributes:
        environment: Only production is currently supported.
        schedule: Schedules can be used to run any section of a dataset at a user-defined frequency.
        tags: Dataform operation tags
    
    Note: the project_id is provided in the Airflow connection for Dataform.
    
    """
    
    environment: str
    schedule: str
    tags: List[str]
