from gcp_airflow_foundations.parse_dags import DagParser
import logging

logging.info("Initialize Airflow Framework")
parser = DagParser()

logging.info("Parse Dags")
parsed_dags = parser.parse_dags()

if parsed_dags:
    globals().update(parsed_dags)