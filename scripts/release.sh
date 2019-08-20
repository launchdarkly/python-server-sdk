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

# Update version in ldclient/version.py - setup.py references this constant
echo "VERSION = \"${VERSION}\"" > ldclient/version.py

# Prepare distribution
python setup.py sdist

# Upload with Twine
pip install twine
python -m twine upload dist/*

echo "Done with python-server-sdk release"
