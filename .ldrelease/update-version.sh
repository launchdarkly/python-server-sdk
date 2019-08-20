#!/bin/bash

# Update version in ldclient/version.py
echo "VERSION = \"${LD_RELEASE_VERSION}\"" > ldclient/version.py

# Update version in setup.py
SETUP_PY_TEMP=./setup.py.tmp
sed "s/ldclient_version=.*/ldclient_version='${LD_RELEASE_VERSION}'/g" setup.py > ${SETUP_PY_TEMP}
mv ${SETUP_PY_TEMP} setup.py
