#!/usr/bin/env bash
# This script updates the version for the ldclient library. It does not actually perform a release.
# It takes exactly one argument: the new version.
# It should be run from the root of this git repo like this:
#   ./scripts/release.sh 4.0.9

set -uxe
echo "Starting python-client version update"

VERSION=$1

#Update version in ldclient/version.py
echo "VERSION = \"${VERSION}\"" > ldclient/version.py

# Update version in setup.py
SETUP_PY_TEMP=./setup.py.tmp
sed "s/ldclient_version=.*/ldclient_version='${VERSION}'/g" setup.py > ${SETUP_PY_TEMP}
mv ${SETUP_PY_TEMP} setup.py

echo "Done with python-client version update"
