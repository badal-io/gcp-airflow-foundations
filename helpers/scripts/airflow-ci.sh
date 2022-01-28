tho#!/bin/bash
pip install -U pip

PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
