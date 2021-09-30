from enum import Enum, unique


@unique
class HdsTableType(Enum):
    SCD2 = "SCD2"
    SNAPSHOT = "SNAPSHOT"