from gcp_airflow_foundations.enums.source_type import SourceType

from gcp_airflow_foundations.source_class.gcs_source import GCStoBQDagBuilder
from gcp_airflow_foundations.source_class.twilio_source import TwilioToBQDagBuilder
from gcp_airflow_foundations.source_class.salesforce_source import SalesforcetoBQDagBuilder
<<<<<<< HEAD
from gcp_airflow_foundations.source_class.oracle_dataflow_source import OracleToBQDataflowDagBuilder
=======
>>>>>>> 07e642acb5747f577c9dda5f86471ba95b72e454
from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig

import logging


def get_dag_builder(source: SourceType, default_task_args: dict, config: DataSourceTablesConfig):
    logging.info(f"Get DAG builder for {source}")
    if source == SourceType.GCS:
        logging.info("Selecting GCS builder")
        return GCStoBQDagBuilder(default_task_args, config)
    elif source == SourceType.TWILIO:
        logging.info("Selecting Twilio builder")
        return TwilioToBQDagBuilder(default_task_args, config)
    elif source == SourceType.SALESFORCE:
        logging.info("Selecting Salesforce builder")
        return SalesforcetoBQDagBuilder(default_task_args, config)
<<<<<<< HEAD
    elif source == SourceType.ORACLE:
        logging.info("Selecting Oracle builder")
        return OracleToBQDataflowDagBuilder(default_task_args, config)
=======
>>>>>>> 07e642acb5747f577c9dda5f86471ba95b72e454
    else:
        logging.info(f"Invalid source {source}")
        raise Exception(f"Invalid source {source}")