from pydantic import validator
from pydantic.dataclasses import dataclass

@dataclass
class LandingZoneConfig:
    landing_zone_dataset: str
