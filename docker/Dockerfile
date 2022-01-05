#ARG AIRFLOW_IMAGE_NAME="2.1.1-python3.8"

#FROM "apache/airflow:${AIRFLOW_IMAGE_NAME}"
ARG PYTHON_VERSION

FROM python:$PYTHON_VERSION

# Environment variabls
ENV AIRFLOW_VER='2.1.1'
ENV AIRFLOW_HOME=/opt/airflow


# Install Airflow and dependencies
COPY helpers/scripts/airflow-ci.sh .
RUN chmod +x ./airflow-ci.sh
RUN bash -c "./airflow-ci.sh '$AIRFLOW_VER'"

# - Install GCP util
#RUN curl -sSL https://sdk.cloud.google.com | bash
#ENV PATH $PATH:/home/airflow/google-cloud-sdk/bin

# - Copy a custom airflow config file
#COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# - install gcp_airflow_foundations package in EDITABLE mode.
# The gcp_airflow_foundations folder is later mounted into the container, so the latest version is picked up
RUN mkdir ./gcp_airflow_foundations

COPY setup.py .
COPY setup.cfg .
COPY README.md .
COPY MANIFEST.in .
COPY gcp_airflow_foundations/__init__.py ./gcp_airflow_foundations/__init__.py
COPY gcp_airflow_foundations/version.py ./gcp_airflow_foundations/version.py

# - Install CI/test dependencies
COPY docker/requirements-ci.txt requirements-ci.txt
RUN pip install --no-cache-dir -r requirements-ci.txt

# - Install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

#WORKDIR /opt/airflow
RUN pip install --upgrade pip
RUN pip install -e .
RUN pip freeze | grep gcp-airflow-foundations

# Initiaize Airflow DB
RUN airflow db init

RUN whoami