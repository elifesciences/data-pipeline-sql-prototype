#!/bin/bash
set -ex
dropdb -U root elife_etl || echo "failed to drop db"
createdb -U root elife_etl
cd db-manager
source venv/bin/activate
DATA_PIPELINE_DATABASE_USER=root DATA_PIPELINE_DATABASE_PASSWORD= python -m db_manager create
