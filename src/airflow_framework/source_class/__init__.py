from airflow_framework.source_type import SourceType

from airflow_framework.source_class.gcs_source import GCStoBQDagBuilder
from airflow_framework.source_class.twilio_source import TwilioToBQDagBuilder

import logging


def get_dag_builder(source: SourceType, default_task_args: dict):
    logging.info(f"Get DAG builder for {source}")
    if source == SourceType.GCS:
        logging.info("Selecting GCS builder")
        return GCStoBQDagBuilder(default_task_args)
    elif source == SourceType.TWILIO:
        logging.info("Selecting Twilio builder")
        return TwilioToBQDagBuilder(default_task_args)
    else:
        logging.info(f"Invalid source {source}")
        raise Exception(f"Invalid source {source}")