#!/bin/bash

set -e

rm -r dist
python setup.py sdist

VENV=`pwd`/test-packaging-venv
rm -rf $VENV
virtualenv $VENV
source $VENV/bin/activate

pip install dist/*.tar.gz
