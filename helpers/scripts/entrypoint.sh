#!/bin/bash

# -- IMPORT VARIABLES FORM A JSON FILE
airflow variables import /opt/airflow/variables/docker-airflow-vars.json

# -- Add GCP connection
airflow connections delete google-cloud-default
airflow connections add 'google-cloud-default' --conn-uri $AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT
