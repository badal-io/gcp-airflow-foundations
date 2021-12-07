from enum import Enum, unique


@unique
class SourceType(Enum):
    GCS = "GCS"
    TWILIO = "TWILIO"
    BQ = "BQ"
    SALESFORCE = "SALESFORCE"
    ORACLE = "ORACLE"
    FACEBOOK = "FACEBOOK"
