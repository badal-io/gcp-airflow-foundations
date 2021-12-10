from enum import Enum, unique


@unique
class SchemaSourceType(Enum):
    GCS = "GCS"
    BQ = "BQ"
    CUSTOM = "CUSTOM"
    AUTO = "AUTO"