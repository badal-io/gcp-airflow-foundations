from enum import Enum, unique


@unique
class IngestionType(Enum):
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"