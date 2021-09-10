from airflow_framework.parse_dags import DagParser
from airflow.models import Variable

parser = DagParser()

parsed_dags = parser.parse_dags()

if len(parsed_dags) > 0:
    globals().update(parsed_dags)