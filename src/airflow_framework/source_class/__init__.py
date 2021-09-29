from airflow_framework.enums.source_type import SourceType

from airflow_framework.source_class.gcs_source import GCStoBQDagBuilder
from airflow_framework.source_class.twilio_source import TwilioToBQDagBuilder

from airflow_framework.base_class.data_source_table_config import DataSourceTablesConfig

import logging


def get_dag_builder(source: SourceType, default_task_args: dict, config: DataSourceTablesConfig):
    logging.info(f"Get DAG builder for {source}")
    if source == SourceType.GCS:
        logging.info("Selecting GCS builder")
        return GCStoBQDagBuilder(default_task_args, config)
    elif source == SourceType.TWILIO:
        logging.info("Selecting Twilio builder")
        return TwilioToBQDagBuilder(default_task_args, config)
    else:
        logging.info(f"Invalid source {source}")
        raise Exception(f"Invalid source {source}")