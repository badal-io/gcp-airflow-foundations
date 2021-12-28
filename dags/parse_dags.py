from gcp_airflow_foundations.parse_dags import DagParser

parser = DagParser()

parsed_dags = parser.parse_dags()

if parsed_dags:
    globals().update(parsed_dags)