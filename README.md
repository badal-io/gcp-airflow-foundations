# gcp-airflow-foundations
[![PyPI version](https://badge.fury.io/py/gcp-airflow-foundations.svg)](https://badge.fury.io/py/gcp-airflow-foundations) [![Documentation Status](https://readthedocs.org/projects/gcp-airflow-foundations/badge/?version=latest)](https://gcp-airflow-foundations.readthedocs.io/en/latest/?badge=latest)

![airflow](./docs/_static/airflow_diagram.png)

Airflow is an awesome open source orchestration framework that is the go-to for building data ingestion pipelines on GCP (using Composer - a hosted AIrflow service). However, most companies using it face the same set of problems 
- Learning curve: Airflow requires python knowledge and has some gotchas that take time to learn. Further, writing Python DAGs for every single table that needs to get ingested becomes cumbersome. Most companies end up building utilities for creating DAGs out of configuration files to simplify DAG creation and to allow non-developers to configure ingestion
- Datalake and data pipelines design best practices: Airflow only provides the building blocks, users are still required to understand and implement the nuances of building a proper ingestion pipelines for the data lake/data warehouse platform they are using 
- Core reusability and best practice enforcement across the enterprise: Usually each team maintains its own Airflow source code and deployment

We have written an opinionated yet flexible ingestion framework for building an ingestion pipeline into data warehouse in BigQuery that supports the following features:

- Zero-code, config file based ingestion - anybody can start ingesting from the growing number of sources by just providing a simple configuration file. Zero python or Airflow knowledge is required.
- Modular and extendable - The core of the framework is a lightweight library. Ingestion sources are added as plugins. Adding a new source can be done by extending the provided base classes.
- Opinionated automatic creation of  ODS (Operational Data Store ) and HDS (Historical Data Store) in BigQuery while enforcing best practices such as schema migration, data quality validation, idempotency, partitioning, etc.
- Dataflow job support for ingesting large datasets from SQL sources and deploying jobs into a specific network or shared VPC.
- Support of advanced Airflow features for job prioritization such as slots and priorities.
- Integration with GCP data services such as DLP and Data Catalog [work in progress].
- Well tested - We maintain a rich suite of both unit and integration tests.

## Installing from PyPI
```bash
pip install 'gcp-airflow-foundations'
```

## Usage
See the [gcp-airflow-foundations documentation](https://gcp-airflow-foundations.readthedocs.io/en/latest/) for more details.
