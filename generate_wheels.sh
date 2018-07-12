#!/usr/bin/env bash

cd csv-generator
python setup.py sdist bdist_wheel

cd ../db-manager/
python setup.py sdist bdist_wheel
