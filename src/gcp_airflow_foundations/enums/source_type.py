from enum import Enum, unique


@unique
class SourceType(Enum):
    GCS = "GCS"
    TWILIO = "TWILIO"
    BQ = "BQ"
<<<<<<< HEAD
    SALESFORCE = "SALESFORCE"
    ORACLE = "ORACLE"
=======
    SALESFORCE = "SALESFORCE"
>>>>>>> 07e642acb5747f577c9dda5f86471ba95b72e454
