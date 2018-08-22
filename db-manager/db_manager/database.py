import logging
import os
from contextlib import contextmanager

import psycopg2

from .sql_scripts import SQL_SCRIPTS_DIRECTORY


LOGGER = logging.getLogger(__name__)


def get_database_connection_params():
    return dict(
        host=os.environ.get('DATA_PIPELINE_DATABASE_HOST', 'localhost'),
        port=os.environ.get('DATA_PIPELINE_DATABASE_PORT', '5432'),
        dbname=os.environ.get('DATA_PIPELINE_DATABASE_NAME', 'elife_etl'),
        user=os.environ.get('DATA_PIPELINE_DATABASE_USER', 'elife_etl'),
        password=os.environ.get('DATA_PIPELINE_DATABASE_PASSWORD', 'elife_etl')
    )


def get_connection_string(host, port, dbname, user, password):
    return "host={host} port={port} dbname={dbname} user={user} password={password}".format(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )


@contextmanager
def managed_connection_to(**kwargs):
    connection = psycopg2.connect(get_connection_string(**kwargs))
    yield connection
    connection.close()


def managed_connection():
    return managed_connection_to(
        **get_database_connection_params()
    )


def run_sql_script(connection, script_filename):
    with open(script_filename, 'r') as f:
        sql = f.read()
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()


def get_sql_script_path(name):
    return os.path.join(SQL_SCRIPTS_DIRECTORY, name)


def teardown_database(connection):
    LOGGER.info('tearing down database')
    run_sql_script(connection, get_sql_script_path('teardown.sql'))


def create_database(connection):
    LOGGER.info('creating database')
    run_sql_script(connection, get_sql_script_path('create.sql'))
