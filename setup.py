import os
import sys
import yaml
import setuptools
from setuptools import setup, find_packages, Distribution


here = os.path.abspath(os.path.dirname(__file__))
about = {}
# Load extra dependencies from txt
with open(os.path.join(here, "requirements-providers.txt"), "r") as f:
    requirements_providers = f.read().strip().split("\n")
with open(os.path.join(here, "requirements-test.txt"), "r") as f:
    requirements_test = f.read().strip().split("\n")


def main() -> dict:
    if os.environ.get("PKG_NAME"):
        pkg = os.environ.get("PKG_NAME")

        # Framework configuration
        if pkg == "gcp_airflow_foundations":
            plug = yaml.safe_load(open("gcp_airflow_foundations_config/framework.yaml"))
            return _process_metadata(plug)

        # Facebook plugin configuration
        if pkg == "gcp_airflow_foundations_facebook":
            plug = yaml.safe_load(open("gcp_airflow_foundations_config/plugins_facebook.yaml"))
            return _process_metadata(plug)

        else:
            return None
    else:
        return None


def _process_metadata(plug) -> dict:
    try:
        packages = [
            package
            for package in setuptools.PEP420PackageFinder.find()
            if package.startswith(str(plug['package-startwith']))
        ]
        extras = {}
        for x in plug['extras']:
            with open(os.path.join(here, f"requirements-{x}.txt"), "r") as f:
                extra = f.read().strip().split("\n")
            extras.update({x: extra})
        metadata = dict(
            name=plug['name'],
            version=plug['versions'][0],
            description=plug['description'],
            long_description=plug['long-description'],
            packages=packages,
            install_requires=plug['dependencies'],
            extras_require=extras,
        )
        base_metadata = dict(
            long_description_content_type="text/markdown",
            author="Badal.io",
            author_email="info@badal.io",
            license="Apache 2.0",
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
        metadata.update(base_metadata)
    except Exception as e:
        print(e)
        metadata=dict(name="no_package")
        print(metadata)
    return metadata


if __name__ == "__main__":
    metadata = main()
    setup(**metadata)
