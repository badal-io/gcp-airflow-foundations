from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field

from datetime import datetime
from typing import List, Optional

from gcp_airflow_foundations.base_class.ods_metadata_config import OdsTableMetadataConfig

@dataclass
class OdsTableConfig:
    """
    Attributes:
        merge_type : Only SG_KEY_WITH_HASH is currently supported
        ods_metadata : See OdsTableMetadataConfig class 
    """
    ods_metadata: OdsTableMetadataConfig
    merge_type: str = "SG_KEY_WITH_HASH"
