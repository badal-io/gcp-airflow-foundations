from airflow_framework.parse_dags import DagParser
from airflow.models import Variable

parser = DagParser()

parsed_dags = parser.parse_dags()

if parsed_dags:
    globals().update(parsed_dags)