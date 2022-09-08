import os
import sys

import setuptools
from setuptools import setup, find_packages

from pathlib import Path

here = os.path.abspath(os.path.dirname(__file__))
about = {}

requirements = ['SQLAlchemy==1.3.23',
                'apache-airflow-providers-apache-beam==4.0.0',
                'apache-airflow-providers-common-sql==1.0.0',
                'apache-airflow-providers-facebook==3.0.1',
                'apache-airflow-providers-salesforce==5.0.0',
                'dacite==1.5.1',
                'pydantic==1.8.2',
                'regex==2022.7.25'
                ]


this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

packages = [
    package
    for package in setuptools.PEP420PackageFinder.find()
    if package.startswith("gcp_airflow_foundations")
]

version = {}
with open(os.path.join(here, "gcp_airflow_foundations/version.py")) as fp:
    exec(fp.read(), version)
version = version["__version__"]


def main():
    metadata = dict(
        name="gcp-airflow-foundations",
        version=version,
        description="Opinionated framework based on Airflow 2.0 for building pipelines to ingest data into a BigQuery data warehouse",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/badal-io/gcp-airflow-foundations",
        author="Badal.io",
        author_email="info@badal.io",
        license="Apache 2.0",
        packages=packages,
        install_requires=requirements,
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "Topic :: Software Development :: Libraries",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
        ],
    )

    setup(**metadata)


if __name__ == "__main__":
    main()
