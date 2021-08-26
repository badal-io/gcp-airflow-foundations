from airflow_framework.parse_dags import DagParser
from airflow.models import Variable

parser = DagParser()

config, dags = parser.parse_dags()

for dag in dags:
    globals()[f"dags:source:{config.source.name}.{dag.dag_id}"] = dag