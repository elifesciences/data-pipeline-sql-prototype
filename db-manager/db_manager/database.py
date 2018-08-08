import logging
import os
from contextlib import contextmanager

import psycopg2


LOGGER = logging.getLogger(__name__)


@contextmanager
def managed_connection():
    connect_str = "host={host} port={port} dbname={dbname} user={user} password={password}".format(
        host=os.environ.get('DATA_PIPELINE_DATABASE_HOST', 'localhost'),
        port=os.environ.get('DATA_PIPELINE_DATABASE_PORT', '5432'),
        dbname=os.environ.get('DATA_PIPELINE_DATABASE_NAME', 'elife_etl'),
        user=os.environ.get('DATA_PIPELINE_DATABASE_USER', 'elife_etl'),
        password=os.environ.get('DATA_PIPELINE_DATABASE_PASSWORD', 'elife_etl')
    )
    connection = psycopg2.connect(connect_str)
    yield connection
    connection.close()


def run_sql_script(connection, script_filename):
    with open(script_filename, 'r') as f:
        sql = f.read()
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()


def get_sql_script_path(name):
    return os.path.join(
        os.path.dirname(__file__),
        'sql_scripts',
        name
    )


def teardown_database(connection):
    LOGGER.info('tearing down database')
    run_sql_script(connection, get_sql_script_path('teardown.sql'))


def create_database(connection):
    LOGGER.info('creating database')
    run_sql_script(connection, get_sql_script_path('create.sql'))
