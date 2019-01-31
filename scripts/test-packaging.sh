#!/bin/bash

set -e

rm -rf dist
python setup.py sdist

rm -rf test-packaging
mkdir test-packaging
cd test-packaging
virtualenv env
source env/bin/activate

pip install ../dist/*.tar.gz
