#!/bin/bash
# loads contents of the 'output' directory in project root
set -e
cd db-manager
source venv/bin/activate
DATA_PIPELINE_DATABASE_USER=root DATA_PIPELINE_DATABASE_PASSWORD= python -m db_manager import-data --source-dir ../output/
