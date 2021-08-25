from enum import Enum, unique


@unique
class SourceType(Enum):
    ADW = "ADW"
    SALESFORCE = "SALESFORCE"
    GCS = "GCS"
    GSHEET = "GSHEET"
    JDBC = "JDBC"
    COMPASS = "COMPASS"
    BQ = "BQ"
    QUALTRICS = "QUALTRICS"
    SFTP = "SFTP"