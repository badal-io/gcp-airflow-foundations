# -*- coding: utf-8 -*-

VERSION = (0, 2, 5)
TAG = "v{}.{}.{}".format(0, 2, 5)
PRERELEASE = None
REVISION = None


def generate_version(version, prerelease=None, revision=None):
    version_parts = [".".join(map(str, version))]
    if prerelease is not None:
        version_parts.append(f"-{prerelease}")
    if revision is not None:
        version_parts.append(f".{revision}")
    return "".join(version_parts)


__title__ = "gcp-airflow-foundations"
__description__ = "Opinionated framework based on Airflow 2.0 for building pipelines to ingest data into a BigQuery data warehouse"
__url__ = "https://github.com/badal-io/gcp-airflow-foundations"
__download_url__ = f"https://github.com/badal-io/gcp-airflow-foundations/archive/refs/tags/{TAG}.tar.gz"
__version__ = generate_version(VERSION, prerelease=PRERELEASE, revision=REVISION)
__author__ = "Badal.io"
__author_email__ = "info@badal.io"
__license__ = "Apache 2.0"
