import os
import sys

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
about = {}

with open(os.path.join(here, "src", "airflow_framework", "__version__.py"), "r") as f:
    exec(f.read(), about)

requirements = [
"dacite==1.5.1",
"pydantic==1.7.2",
"pyyaml==5.3.1",
"wheel==0.36.2",
"black==19.10b0",
"pytest>=5.2.1",
"apache-airflow[gcp_api]==2.1.0",
"pytest-testconfig>=0.2.0",
"configparser>=3.5.0",
"pytest-mock>=3.3.1",
"pytest-airflow>=0.0.3",
"pytest-pythonpath>=0.7.3",
"pyclean>=2.0.0",
"flake8>=3.8.0",
"pre-commit>=1.18.3"
]


def main():
    metadata = dict(
        name=about["__title__"],
        version=about["__version__"],
        description=about["__description__"],
        url=about["__url__"],
        author=about["__author__"],
        author_email=about["__author_email__"],
        license=about["__license__"],
        packages=find_packages(where="src", exclude=("tests",)),
        package_dir={"":"src"},
        install_requires=requirements
    )

    setup(**metadata)


if __name__ == "__main__":
    main()