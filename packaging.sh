#!/bin/bash
echo "package_name: $1";

#we set variable from the command line
export PKG_NAME="$1"

echo $PKG_NAME
python setup.py bdist_wheel --universal

