from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field

from datetime import datetime
from typing import List, Optional

from gcp_airflow_foundations.base_class.ods_metadata_config import OdsTableMetadataConfig
from gcp_airflow_foundations.enums.ingestion_type import IngestionType

@dataclass
class OdsTableConfig:
    """
    Attributes:
        merge_type : Only SG_KEY_WITH_HASH is currently supported
        ingestion_type: FULL or INCREMENTAL
        ods_metadata : See OdsTableMetadataConfig class 
    """
    ods_metadata: OdsTableMetadataConfig
    ingestion_type: IngestionType = "FULL" # FULL or INCREMENTAL
    merge_type: str = "SG_KEY_WITH_HASH"
