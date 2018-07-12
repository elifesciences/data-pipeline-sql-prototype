import logging
import os
from contextlib import contextmanager

import psycopg2


LOGGER = logging.getLogger(__name__)


@contextmanager
def managed_connection():
  connect_str = "host={host} port={port} dbname={dbname} user={user} password={password}".format(
    host=os.environ.get('DATABASE_HOST', 'localhost'),
    port=os.environ.get('DATABASE_PORT', '5432'),
    dbname=os.environ.get('DATABASE_NAME', 'elife_etl'),
    user=os.environ.get('DATABASE_USER', 'elife_etl'),
    password=os.environ.get('DATABASE_PASSWORD', 'elife_etl')
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


def teaddown_database(connection):
    LOGGER.info('tearing down database')
    run_sql_script(connection, os.path.join('sql_scripts', 'teardown.sql'))


def create_database(connection):
    LOGGER.info('creating database')
    run_sql_script(connection, os.path.join('sql_scripts', 'create.sql'))
