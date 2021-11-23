from enum import Enum, unique


@unique
class Level(Enum):
    AD = "ad"
    ADSET = "adset"
    CAMPAIGN = "campaign"
    ACCOUNT = 'account'


@unique
class DatePreset(Enum):
    TODAY = "today"
    YESTERDAY = "yesterday"
    THIS_MONTH = "this_month"
    LAST_MONTH = "last_month"
    MAXIMUM = "maximum"


@unique
class AccountLookupScope(Enum):
    ALL = "all"
    ACTIVE = "active"


@unique
class ApiObject(Enum):
    INSIGHTS = "INSIGHTS"
    CAMPAIGNS = "CAMPAIGNS"
    ADSETS = "ADSETS"