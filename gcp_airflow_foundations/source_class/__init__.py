from gcp_airflow_foundations.enums.source_type import SourceType

from gcp_airflow_foundations.source_class.twilio_source import TwilioToBQDagBuilder
from gcp_airflow_foundations.source_class.salesforce_source import SalesforcetoBQDagBuilder
from gcp_airflow_foundations.source_class.oracle_dataflow_source import OracleToBQDataflowDagBuilder
from gcp_airflow_foundations.source_class.facebook import FacebooktoBQDagBuilder

from gcp_airflow_foundations.base_class.data_source_table_config import DataSourceTablesConfig

import logging


def get_dag_builder(source: SourceType, default_task_args: dict, config: DataSourceTablesConfig):
    logging.info(f"Get DAG builder for {source}")
    if source == SourceType.TWILIO:
        logging.info("Selecting Twilio builder")
        return TwilioToBQDagBuilder(default_task_args, config)
    elif source == SourceType.SALESFORCE:
        logging.info("Selecting Salesforce builder")
        return SalesforcetoBQDagBuilder(default_task_args, config)
    elif source == SourceType.ORACLE:
        logging.info("Selecting Oracle builder")
        return OracleToBQDataflowDagBuilder(default_task_args, config)
    elif source == SourceType.FACEBOOK:
        logging.info("Selecting Facebook builder")
        return FacebooktoBQDagBuilder(default_task_args, config)
    else:
        logging.info(f"Invalid source {source}")
        raise Exception(f"Invalid source {source}")