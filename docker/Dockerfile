ARG AIRFLOW_IMAGE_NAME="2.1.1-python3.8"

FROM "apache/airflow:${AIRFLOW_IMAGE_NAME}"

# - Install GCP util
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/home/airflow/google-cloud-sdk/bin

# - Copy a custom airflow config file
#COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# - install gcp_airflow_foundations package in EDITABLE mode.
# The gcp_airflow_foundations folder is later mounted into the container, so the latest version is picked up
RUN mkdir /opt/airflow/gcp_airflow_foundations

COPY setup.py /opt/airflow/setup.py
COPY setup.cfg /opt/airflow/setup.cfg
COPY README.md /opt/airflow/README.md
COPY MANIFEST.in /opt/airflow/MANIFEST.in
COPY gcp_airflow_foundations/__init__.py /opt/airflow/gcp_airflow_foundations/__init__.py
COPY gcp_airflow_foundations/version.py /opt/airflow/gcp_airflow_foundations/version.py

# - Install CI/test dependencies
COPY docker/requirements-ci.txt requirements-ci.txt
RUN pip install --no-cache-dir -r requirements-ci.txt

# - Install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/airflow
RUN pip install --upgrade pip
RUN pip install -e .


