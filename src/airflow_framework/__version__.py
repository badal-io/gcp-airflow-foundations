# -*- coding: utf-8 -*-

VERSION = (0, 1, 0)
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
__description__ = "Airflow toolkit"
__url__ = "https://github.com/badal-io/airflow-foundations"
__version__ = generate_version(VERSION, prerelease=PRERELEASE, revision=REVISION)
__author__ = "Badal.io"
__author_email__ = "info@badal.io"
__license__ = "Apache 2.0"