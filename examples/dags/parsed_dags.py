from airflow_framework.parse_dags import DagParser

parser = DagParser() 

# Can overwrite the "CONFIG_FILE_LOCATION" variable here by setting the parser.conf_location attribute to the desired path

parsed_dags = parser.parse_dags()

if parsed_dags:
    globals().update(parsed_dags)
