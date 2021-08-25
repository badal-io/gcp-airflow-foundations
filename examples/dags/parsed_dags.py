from airflow_framework.parse_dags import DagParser

# Provide path for config .yaml file
conf_path = "/opt/airflow-framework/src/airflow_framework/config"

config, dags = DagParser(conf_path).parse_dags()

for dag in dags:
    globals()[f"dags:source:{config.source.name}.{dag.dag_id}"] = dag