ARG PYTHON_VERSION

FROM python:$PYTHON_VERSION

# Environment variabls
ENV AIRFLOW_VER='2.1.1'
ENV AIRFLOW_HOME=/opt/airflow


# Install Airflow and dependencies
COPY helpers/scripts/airflow-ci.sh .
RUN chmod +x ./airflow-ci.sh
RUN bash -c "./airflow-ci.sh '$AIRFLOW_VER'"

# Install Py packages
COPY docker/requirements-composer.txt .
RUN pip install --no-cache-dir -r requirements-composer.txt

COPY ./dist/gcp-airflow-foundations-0.2.5.tar.gz .
RUN pip install gcp-airflow-foundations-0.2.5.tar.gz

# Initiaize Airflow DB
RUN airflow db init