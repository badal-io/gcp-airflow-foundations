import os
import sys

from setuptools import setup, find_packages
from pathlib import Path

here = os.path.abspath(os.path.dirname(__file__))
about = {}

with open(os.path.join(here, "requirements.txt"), "r") as f:
	requirements = f.read().strip().split('\n')

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

TAG = "v{}.{}.{}".format(0, 2, 7)

def main():
    metadata = dict(
        name="gcp-airflow-foundations",
        version="0.2.7",
        description="Opinionated framework based on Airflow 2.0 for building pipelines to ingest data into a BigQuery data warehouse",
        long_description=long_description,
        long_description_content_type='text/markdown',
        url="https://github.com/badal-io/gcp-airflow-foundations",
        download_url=f"https://github.com/badal-io/gcp-airflow-foundations/archive/refs/tags/{TAG}.tar.gz",
        author="Badal.io",
        author_email="info@badal.io",
        license="Apache 2.0",
        packages=find_packages(where="gcp_airflow_foundations", exclude=("tests")),
        package_dir={"":"gcp_airflow_foundations"},
        install_requires=requirements,
        classifiers=[
            'Development Status :: 4 - Beta',      
            'Intended Audience :: Developers',      
            'Topic :: Software Development :: Libraries',
            'License :: OSI Approved :: Apache Software License',   
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
        ]
    )

    setup(**metadata)


if __name__ == "__main__":
    main()
