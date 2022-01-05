ARG PYTHON_VERSION

FROM python:$PYTHON_VERSION

# Environment variabls
ENV AIRFLOW_VER='2.1.1'
ENV AIRFLOW_HOME=/opt/airflow

# Install Airflow and dependencies
COPY helpers/scripts/airflow-ci.sh .
RUN chmod +x ./airflow-ci.sh
RUN bash -c "./airflow-ci.sh '$AIRFLOW_VER'"

# - install gcp_airflow_foundations package in EDITABLE mode.
RUN mkdir /opt/gcp-airflow-foundations

COPY . /opt/gcp-airflow-foundations

# - Install CI/test dependencies
COPY docker/requirements-ci.txt requirements-ci.txt
RUN pip install --no-cache-dir -r requirements-ci.txt

# - Install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/gcp-airflow-foundations
RUN pip install --upgrade pip
RUN pip install -e .

# - Create distribution file
RUN pip install build wheel
RUN python3 setup.py sdist

# Initiaize Airflow DB
RUN airflow db init