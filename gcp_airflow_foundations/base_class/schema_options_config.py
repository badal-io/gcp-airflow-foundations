from dacite import Config
from pydantic import validator, root_validator
from pydantic.dataclasses import dataclass

from typing import List, Optional
import regex as re

from gcp_airflow_foundations.enums.schema_source_type import SchemaSourceType

supported_fields = ['table_name','date']

@dataclass
class SchemaOptionsConfig:
    schema_source_type: SchemaSourceType
    schema_object_template: Optional[str] # Supported tamplate fields: {table_name}, {date}
    
    @root_validator(pre=True)
    def valid_config(cls, values):
        if values['schema_source_type'] == SchemaSourceType.GCS:
            assert values['schema_object_template'] is not None, 'To read schema from GCS a file template must be provided'
        return values

    @validator("schema_object_template")
    def valid_schema_object_template(cls, v):
        if v is not None:
            fields = re.findall(r'\{(.*?)\}', v)
            assert all([i in supported_fields for i in fields]), f'The GCS schema file template contains invalid fields: {[i for i in fields if i not in supported_fields]}'
        return v
