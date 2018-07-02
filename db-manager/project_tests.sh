#!/bin/bash

set -e

pipenv install --dev --three
# enable once tests have been added
# pipenv run python -m pytest
