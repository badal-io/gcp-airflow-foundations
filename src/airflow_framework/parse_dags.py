import datetime

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow_framework.base_class.utils import load_tables_config_from_dir
from airflow_framework.dag_utils.done_all_dag import wait_for_all_and_notify

from airflow_framework.source_class import get_dag_builder

import logging

# Global Vars - try to keep them to minimum since they get fetched every time the schedule gets evaluated (i.e every few seconds
max_task_retries = Variable.get("max_task_retries", 3)

conf_location = Variable.get("CONFIG_FILE_LOCATION", "config")
logging.info(f"Loading config from dir: {conf_location}")

configs = load_tables_config_from_dir(conf_location)

for config in configs:
    logging.info(f"StartDate for {config.source.name}: {config.source_start_date()}")

    default_task_args = {
        "owner": config.source.owner,
        "retries": max_task_retries,
        "retry_exponential_backoff": True,
        "retry_delay": datetime.timedelta(seconds=300),
        "project_id": config.source.gcp_project,
        "start_date": config.source_start_date(),
        "email": config.source.notification_emails,
        "email_on_failure": True,
        "email_on_retry": False,
        "depends_on_past": False,
    }

    builder = get_dag_builder(config.source.source_type, default_task_args)
    dags = builder.build_dags(config)
    for dag in dags:
        globals()[f"dags:source:{config.source.name}.{dag.dag_id}"] = dag

    new_args = {**default_task_args, "depends_on_past": False}
    source_done_dag = wait_for_all_and_notify(
        dags=dags,
        dag_id=f"finished_all_dags_for_{config.source.name}",
        task_id_to_wait_for="done",
        notify_email=config.source.notification_emails[0],
        subject=f"{config.source.name} ELT finished",
        dagrun_timeout=config.dagrun_timeout(),
        default_task_args=new_args,
    )
    logging.info(f"Created done dag for {source_done_dag}")

    globals()[f"dag_id:source:{config.source.name}"] = source_done_dag
