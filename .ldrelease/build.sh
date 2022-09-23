#!/bin/bash

set -ue

echo "Installing requirements"
pip install -r requirements.txt || { echo "installing requirements.txt failed" >&2; exit 1; }
pip install wheel || { echo "installing wheel failed" >&2; exit 1; }

echo "Running setup.py sdist bdist_wheel"
python setup.py sdist bdist_wheel || { echo "setup.py sdist bdist_wheel failed" >&2; exit 1; }
