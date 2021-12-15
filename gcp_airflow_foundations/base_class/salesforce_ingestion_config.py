from airflow.exceptions import AirflowException

from dacite import Config
from dataclasses import dataclass, field

from datetime import datetime
from typing import List, Optional


@dataclass
class SalesforceIngestionConfig:
    """
    Salesforce configuration class.
    
    Attributes:
        ingest_all_columns: SELECT * the Salesforce object if true
        fields_to_omit: a list of object fields to omit from ingestion
        field_names: an explicit list of fields to ingest
    """
    api_table_name: str
    ingest_all_columns: bool
    fields_to_omit: Optional[List[str]]
    field_names: Optional[List[str]]
