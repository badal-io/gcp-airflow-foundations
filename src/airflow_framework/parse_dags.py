import datetime

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow_framework.base_class.utils import load_tables_config_from_dir

from airflow_framework.source_class import get_dag_builder

import logging

# Global Vars - try to keep them to minimum since they get fetched every time the schedule gets evaluated (i.e every few seconds

class DagParser():

    def __init__(self, conf_path):
        self.conf_path = conf_path

    def parse_dags(self):
        max_task_retries = Variable.get("max_task_retries", 3)

        conf_location = Variable.get("CONFIG_FILE_LOCATION", self.conf_path)
        logging.info(f"Loading config from dir: {conf_location}")

        configs = load_tables_config_from_dir(conf_location)

        for config in configs:
            logging.info(f"StartDate for {config.source.name}: {config.source_start_date()}")

            default_task_args = {
                "owner": config.source.owner,
                "retries": 3,
                "retry_exponential_backoff": True,
                "retry_delay": datetime.timedelta(seconds=300),
                "project_id": config.source.gcp_project,
                "start_date": config.source_start_date(),
                "email": config.source.notification_emails,
                "email_on_failure": True,
                "email_on_retry": False,
                "depends_on_past": False
            }

            builder = get_dag_builder(config.source.source_type, default_task_args)
            dags = builder.build_dags(config)

            return config, dags