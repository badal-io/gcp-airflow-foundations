from enum import Enum


class TemplateIngestionMode(Enum):
    INGEST_ALL = "INGEST_ALL"
    INGEST_BY_TABLE_NAMES = "INGEST_BY_TABLE_NAMES"
    INGEST_BY_REGEX = "INGEST_BY_REGEX"


class TemplateDagCreationMode(Enum):
    SOURCE = "SOURCE"
    TABLE = "TABLE"
