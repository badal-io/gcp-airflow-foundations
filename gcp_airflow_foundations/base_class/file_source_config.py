from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional


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

    source_format: str
    file_name_template: Optional[str]

    file_prefix_filtering: bool = False

    date_format: str = "%Y-%m-%d"
    airflow_date_template: str = "ds"
    delete_gcs_files: bool = False
    gcs_bucket_prefix: str = ""
    delimeter: str = ","
    sensor_timeout: int = 10800
