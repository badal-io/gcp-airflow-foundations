import datetime

from airflow.models.dag import DAG
from airflow.models import Variable
from gcp_airflow_foundations.base_class.utils import load_tables_config_from_dir

from gcp_airflow_foundations.source_class import get_dag_builder

import logging


class DagParser:
    """ Parsed Airflow DAGs from the user-provided YAML configuration files """
    def __init__(self):
        self.conf_location = Variable.get("CONFIG_FILE_LOCATION", "/opt/airflow/gcp_airflow_foundations/examples/config")
        self.max_task_retries = Variable.get("max_task_retries", 3)

    def parse_dags(self):
        logging.info(f"Loading config from dir: {self.conf_location}")

        configs = load_tables_config_from_dir(self.conf_location)

        parsed_dags = {}
        for config in configs:
            logging.info(f"StartDate for {config.source.name}: {config.source_start_date()}")

            default_task_args = {
                "owner": config.source.owner,
                "retries": self.max_task_retries,
                "retry_exponential_backoff": True,
                "retry_delay": datetime.timedelta(seconds=300),
                "project_id": config.source.gcp_project,
                "start_date": config.source_start_date(),
                "email": config.source.notification_emails,
                "email_on_failure": True,
                "email_on_retry": False,
                "depends_on_past": False
            }

            # Old way of adding DAGs
            builder = get_dag_builder(config.source.source_type, default_task_args, config)
            dags = builder.build_dags()

            for dag in dags:
                parsed_dags[f"dags:source:{config.source.name}.{dag.dag_id}"] = dag

            """
            # new way
            builder = None
            for dag_builder in DagBuilder.sources:
                # if matching subclass of DagBuilder exists, then use it
                if config.source.source_type == dag_builder.source_type:
                    source_name = dag_builder.__class__.__name__
                    # pick out the right source
                    builder = globals()[source_name](default_task_args, config)
                    dags = builder.build_dags()
            
            if dags:
                parsed_dags[f"dags:source:{config.source.name}.{dag.dag_id}"] = dag
            """
            
        return parsed_dags
