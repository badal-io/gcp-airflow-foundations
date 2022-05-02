from xxlimited import Str
from dacite import Config
from pydantic import validator, root_validator
from pydantic.dataclasses import dataclass
from dataclasses import field

from typing import List, Optional
import regex as re

from gcp_airflow_foundations.enums.template_ingestion import TemplateIngestionMode, TemplateDagCreationMode


@dataclass
class TemplateIngestionOptionsConfig:
    """
    Attributes:
        ingest_all_tables: if true, ingest all tables from source
        ingestion_name: name to use for DAG id, should correspond to the relevant partition of the source cut out by the regex expression provided
        dag_creation_mode: if "SOURCE", then one DAG per source is created. if "TABLE", one DAG per table.
        regex_table_matching: regex pattern to match tables to, if ingest_all_tables is false
    """
    ingestion_name: str
    ingest_mode: TemplateIngestionMode = TemplateIngestionMode("INGEST_BY_TABLE_NAMES"),
    dag_creation_mode: TemplateDagCreationMode = TemplateDagCreationMode("TABLE"),
    table_names: list = field(default_factory=list)
    regex_pattern: Optional[str] = None
