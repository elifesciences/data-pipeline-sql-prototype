import logging
import os

import pytest

from db_manager.database import (
    managed_connection_to,
    teardown_database,
    create_database
)


@pytest.fixture(autouse=True)
def configure_logging():
    logging.root.handlers = []
    logging.basicConfig(level='DEBUG')


def _get_test_database_connection_params():
    return dict(
        host=os.environ.get('DATA_PIPELINE_TEST_DATABASE_HOST', 'localhost'),
        port=os.environ.get('DATA_PIPELINE_TEST_DATABASE_PORT', '9432'),
        dbname=os.environ.get('DATA_PIPELINE_TEST_DATABASE_NAME', 'test'),
        user=os.environ.get('DATA_PIPELINE_TEST_DATABASE_USER', 'test'),
        password=os.environ.get('DATA_PIPELINE_TEST_DATABASE_PASSWORD', 'test')
    )


@pytest.fixture(scope='session')
def test_database():
    with managed_connection_to(**_get_test_database_connection_params()) as db:
        yield db


@pytest.fixture()
def empty_test_database(test_database):
    teardown_database(test_database)
    create_database(test_database)
    yield test_database
