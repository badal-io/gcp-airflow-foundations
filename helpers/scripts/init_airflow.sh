#!/bin/bash
#
# Airflow init steps
#
mkdir -p ./logs ./plugins ./data ./dags ./tests/integration ./tests/unit && \
touch ./variables/docker-env-secrets ./dags/.airflowignore
chmod -R 0777 ./logs ./plugins ./data ./dags ./tests ./helpers/scripts
docker-compose up airflow-init
docker-compose up
