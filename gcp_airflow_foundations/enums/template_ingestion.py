from enum import Enum

class TemplateIngestion(Enum):
    INGEST_ALL = "INGEST_ALL"
    INGEST_BY_TABLE_NAMES = "INGEST_BY_TABLE_NAMES"
    INGEST_BY_REGEX = "INGEST_BY_REGEX"
