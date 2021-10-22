from enum import Enum, unique


@unique
class SchemaSourceType(Enum):
    GCS = "GCS"
    LANDINGZONE = "LANDINGZONE"
    CUSTOM = "CUSTOM"