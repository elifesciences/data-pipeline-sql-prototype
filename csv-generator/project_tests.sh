#!/bin/bash

set -e

pipenv install --dev --three
pipenv run python -m pytest
