#!/bin/bash

set -e

# removing old virtual environment, if any
pipenv --rm || true
rm -rf venv || true

# install develop and default dependencies
pipenv install --dev

# create venv link for convenience
ln -s $(pipenv --venv) venv

# install csv-generator and db-manager from source

source venv/bin/activate

cd ../csv-generator
pip install -e .

cd ../db-manager
pip install -e .
