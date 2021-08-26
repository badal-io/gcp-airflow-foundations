import os
import sys

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
about = {}

with open(os.path.join(here, "src", "airflow_framework", "__version__.py"), "r") as f:
    exec(f.read(), about)

with open(os.path.join(here, "requirements.txt"), "r") as f:
	requirements = f.read().strip().split('\n')

def main():
    metadata = dict(
        name=about["__title__"],
        version=about["__version__"],
        description=about["__description__"],
        url=about["__url__"],
        author=about["__author__"],
        author_email=about["__author_email__"],
        license=about["__license__"],
        packages=find_packages(where="src", exclude=("tests")),
        package_dir={"":"src"},
        install_requires=requirements
    )

    setup(**metadata)


if __name__ == "__main__":
    main()