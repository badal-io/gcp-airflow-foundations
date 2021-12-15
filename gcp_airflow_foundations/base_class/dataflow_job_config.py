from pydantic import validator
from pydantic.dataclasses import dataclass

from typing import List, Optional

@dataclass
class DataflowJobConfig:
    """
    Dataflow configuration class.
    
    Attributes:
        project: the GCP project in which the Dataflow job runs
        region: the region in which the Dataflow job should run
        subnetwork: the specific subnetwork in which the Dataflow job should run
        bq_load_temp_directory: GCS directory for loading temporary Dataflow files
        template_path: GCS path to Dataflow template
        jdbc_driver_class: the name of the JDBC driver class to use (e.g. oracle.jdbc.driver.OracleDriver)
        jdbc_jar_path: the GCS path to the driver .jar file
        jdbc_url: a valid JDBC url for connecting to the database
        jdbc_user: the database username
        jdbc_pass_secret_name: the secret name of the database password
        kms_key_path: the KMS key path for encrypting/decrypting JDBC credentials
        sql_casts: a dictionary of sql casts to use when querying the source DB
        database_owner: owner of the tables to query (query scope)
    """
    system_name: str
    project: str
    region: str
    subnetwork: str
    bq_load_temp_directory: str
    template_path: str
    jdbc_driver_class: str
    jdbc_jar_path: str
    jdbc_url: str
    jdbc_user: str
    jdbc_pass_secret_name: str
    kms_key_path: str
    sql_casts: Optional[dict]
    bq_schema_table: str
    database_owner: str
    connection_pool: str