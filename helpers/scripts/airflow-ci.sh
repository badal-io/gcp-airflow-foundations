#!/bin/bash
pip install -U pip

PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

#CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-$1/constraints-${PYTHON_VERSION}.txt

#pip install apache-airflow[gcp_api]==$1 --constraint ${CONSTRAINT_URL}
