#!/bin/bash
set -vxeuo pipefail

sudo apt-get install libsnappy-dev
# These packages are installed in the base environment but may be older
# versions. Explicitly upgrade them because they often create
# installation problems if out of date.
python -m pip install --upgrade pip setuptools wheel numpy
# Versioneer uses the most recent git tag to generate __version__, which appears
# in the published documentation.
git fetch --tags
python -m pip install .[all]
python -m pip install ./bluesky-tiled-plugins
python -m pip list
