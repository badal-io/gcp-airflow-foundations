import os
import sys

import setuptools
from setuptools import setup, find_packages

from pathlib import Path

here = os.path.abspath(os.path.dirname(__file__))
about = {}

with open(os.path.join(here, "requirements.txt"), "r") as f:
    requirements = f.read().strip().split("\n")

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
