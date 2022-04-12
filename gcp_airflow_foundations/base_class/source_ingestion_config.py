from dacite import Config
from pydantic import validator, root_validator
from pydantic.dataclasses import dataclass

from typing import List, Optional
import regex as re

from gcp_airflow_foundations.enums.schema_source_type import SchemaSourceType


@dataclass
class FullIngestionConfig:
    """
    Attributes:
        ingest_all_tables: if true, ingest all tables from source
        ingestion_name: name to use for DAG id, should correspond to the relevant partition of the source cut out by the regex expression provided
        dag_creation_mode: if "SOURCE", then one DAG per source is created. if "TABLE", one DAG per table.
        regex_table_matching: regex pattern to match tables to, if ingest_all_tables is false
    """
    ingest_all_tables: bool = False
    ingestion_name: str = ""
    dag_creation_mode: str = "TABLE"
    regex_table_pattern: str = "ANY"
