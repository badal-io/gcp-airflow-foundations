# -*- coding: utf-8 -*-

VERSION = (1, 0, 2)
PRERELEASE = None
REVISION = None


def generate_version(version, prerelease=None, revision=None):
    version_parts = [".".join(map(str, version))]
    if prerelease is not None:
        version_parts.append(f"-{prerelease}")
    if revision is not None:
        version_parts.append(f".{revision}")
    return "".join(version_parts)


__title__ = "airflow_framework"
__description__ = "Opinionated framework based on Airflow 2.0 for building pipelines to ingest data into a BigQuery data warehouse"
__url__ = "https://github.com/badal-io/airflow-foundations"
__download_url__ = "https://github.com/badal-io/airflow-framework/archive/refs/tags/v1.0.2.tar.gz"
__version__ = generate_version(VERSION, prerelease=PRERELEASE, revision=REVISION)
__author__ = "Badal.io"
__author_email__ = "info@badal.io"
__license__ = "Apache 2.0"