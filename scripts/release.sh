#!/usr/bin/env bash
# This script updates the version for the ldclient library and releases it to PyPi
# It will only work if you have the proper credentials set up in ~/.pypirc

# It takes exactly one argument: the new version.
# It should be run from the root of this git repo like this:
#   ./scripts/release.sh 4.0.9

# When done you should commit and push the changes made.

set -uxe
echo "Starting python-server-sdk release."

VERSION=$1

#Update version in ldclient/version.py
echo "VERSION = \"${VERSION}\"" > ldclient/version.py

# Update version in setup.py
SETUP_PY_TEMP=./setup.py.tmp
sed "s/ldclient_version=.*/ldclient_version='${VERSION}'/g" setup.py > ${SETUP_PY_TEMP}
mv ${SETUP_PY_TEMP} setup.py

# Prepare distribution
python setup.py sdist

# Upload with Twine
pip install twine
python -m twine upload dist/*

echo "Done with python-server-sdk release"
