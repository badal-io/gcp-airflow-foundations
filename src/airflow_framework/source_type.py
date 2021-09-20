from enum import Enum, unique


@unique
class SourceType(Enum):
    GCS = "GCS"
    TWILIO = "TWILIO"
    BQ = "BQ"