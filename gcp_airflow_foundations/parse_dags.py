import datetime

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowException
from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir
from gcp_airflow_foundations.source_class.source import DagBuilder
from gcp_airflow_foundations.source_class import get_dag_builder
from gcp_airflow_foundations.source_class import (
    generic_file_source,
    gcs_source,
    sftp_source,
    jdbc_dataflow_source,
    oracle_dataflow_source,
    salesforce_source,
    twilio_source,
    mysql_dataflow_source,
)

import logging


class DagParser:
    """ Parsed Airflow DAGs from the user-provided YAML configuration files """

    def __init__(self):
        logging.info("Initialize DagParser")
        self.conf_location = Variable.get("CONFIG_FILE_LOCATION", "dags/config")
        self.max_task_retries = Variable.get("max_task_retries", 3)

    def parse_dags(self):
        logging.info(f"Loading config from dir: {self.conf_location}")

        configs = load_tables_config_from_dir(self.conf_location)

        parsed_dags = {}
        for config in configs:
            logging.info(
                f"StartDate for {config.source.name}: {config.source_start_date()}"
            )

            default_task_args = {
                "owner": config.source.owner,
                "retries": self.max_task_retries,
                "retry_exponential_backoff": True,
                "retry_delay": datetime.timedelta(seconds=300),
                "project_id": config.source.gcp_project,
                "email": config.source.notification_emails,
                "email_on_failure": True,
                "email_on_retry": False,
                "depends_on_past": False,
            }

            """
            # Old way of adding DAGs
            builder = get_dag_builder(config.source.source_type, default_task_args, config)
            dags = builder.build_dags()

            for dag in dags:
                parsed_dags[f"dags:source:{config.source.name}.{dag.dag_id}"] = dag
            """

            # new way
            builder = None
            dags = []
            for dag_builder in DagBuilder.sources:
                # if matching subclass of DagBuilder exists, then use it
                if config.source.source_type == dag_builder.source_type:
                    # source_name = dag_builder.__class__.__name__
                    # pick out the right source
                    builder = dag_builder(default_task_args, config)
                    dags = builder.build_dags()

            if builder is None:
                raise AirflowException(
                    f'Source "{config.source.source_type}" is not found in DagBuilder Class'
                )

            for dag in dags:
                parsed_dags[f"dags:source:{config.source.name}.{dag.dag_id}"] = dag

        return parsed_dags
