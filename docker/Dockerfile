ARG AIRFLOW_IMAGE_NAME="2.2.5-python3.8"

FROM "apache/airflow:${AIRFLOW_IMAGE_NAME}"

# - Install GCP util
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/home/airflow/google-cloud-sdk/bin

# - Copy a custom airflow config file
#COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# - install gcp_airflow_foundations package in EDITABLE mode.

# The gcp_airflow_foundations folder is later mounted into the container, so the latest version is picked up
RUN mkdir /opt/airflow/gcp_airflow_foundations

# Copying only files essential for installing gcp_airflow_foundations
COPY setup.py setup.cfg README.md MANIFEST.in requirements.txt requirements-providers.txt requirements-ci.txt /opt/airflow/
#COPY tests /opt/airflow/tests
COPY gcp_airflow_foundations/__init__.py /opt/airflow/gcp_airflow_foundations/__init__.py
COPY gcp_airflow_foundations/version.py /opt/airflow/gcp_airflow_foundations/version.py

WORKDIR /opt/airflow

# USER root
RUN pip install  --upgrade pip

# - Install dependencies
RUN pip install  -e .[providers,test]

# USER airflow




