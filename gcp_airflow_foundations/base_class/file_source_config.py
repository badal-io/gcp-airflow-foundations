from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

from pydantic.networks import stricturl

@dataclass
class FileSourceConfig:
    """
    Attributes:
        file_name_template: a templated file name that applies to all tables. The DAGs will search for one file per table accordingly,
            and no metadata file is required if this field is supplied.
        file_prefix_filtering: option to search for file by prefix (e.g. if the end of file name is a randomly generated string.)
        airflow_date_template: the airflow templated variable to use for loading dated file names: either "ds" or "prev_ds"
        date_format: The date format expected in the file naming convention
        delimiter: delimiter used in the file
        source_format: format of the files
        delete_gcs_files: whether to delete the GCS after transferring them to BigQuery
        sensor_timeout: time, in seconds, before the sensors time out: default is 10800 (3 hours)
    """
    file_name_template: Optional[str]
    file_prefix_filtering: bool
    date_format: str
    airflow_date_template: str
    delimeter: str
    source_format: str
    delete_gcs_files: bool
    gcs_bucket_prefix: str
    sensor_timeout: int = 10800