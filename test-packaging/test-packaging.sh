#!/bin/bash

set -e

rm -rf dist
python setup.py sdist

cd test-packaging
rm -rf env
virtualenv env
source env/bin/activate

pip install ../dist/*.tar.gz

python test.py
