from enum import Enum, unique


@unique
class TimePartitioning(Enum):
    HOUR = "HOUR"
    DAY = "DAY"
    MONTH = "MONTH"