import os
import sys

from setuptools import setup, find_packages
from pathlib import Path

here = os.path.abspath(os.path.dirname(__file__))
about = {}

with open(os.path.join(here, "src", "gcp_airflow_foundations", "__version__.py"), "r") as f:
    exec(f.read(), about)

with open(os.path.join(here, "requirements.txt"), "r") as f:
	requirements = f.read().strip().split('\n')

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

def main():
    metadata = dict(
        name=about["__title__"],
        version=about["__version__"],
        description=about["__description__"],
        long_description=long_description,
        long_description_content_type='text/markdown',
        url=about["__url__"],
        download_url=about["__download_url__"],
        author=about["__author__"],
        author_email=about["__author_email__"],
        license=about["__license__"],
        packages=find_packages(where="src", exclude=("tests")),
        package_dir={"":"src"},
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
