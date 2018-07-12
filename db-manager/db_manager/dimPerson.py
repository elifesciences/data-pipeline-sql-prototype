import csv
import logging

import psycopg2.extras

from . import DBManager
from . import dimPersonRole


LOGGING = logging.getLogger(__name__)


def stage_csv(conn, file_path):
  LOGGING.debug("StagingFile '{file}'".format(file = file_path))
  with open(file_path, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    # ToDo: Validation of column names and types
    with conn.cursor() as cur:
      psycopg2.extras.execute_values(
        cur,
        """
          INSERT INTO
            stg.dimPerson(
              source_file_name,
              source_file_creation,
              externalReference_Person,
              status,
              first_name,
              middle_name,
              last_name,
              profile_modify_date
            )
          VALUES %s
        """,
        reader,
        template="""
          (
            %(zip_name)s || '/' || %(xml_file_name)s,
            %(create_date)s,
            %(person_id)s,
            %(status)s,
            %(first_name)s,
            %(middle_name)s,
            %(last_name)s,
            %(profile_modify_date)s
          )
        """,
        page_size=1000
      )
      # ToDo: Logging of rows upload, time taken, etc
      conn.commit()

def cascadeActivations(conn, source):
  # to all children first
  if (source != 'dimPersonRole'):
    dimPersonRole.cascadeActivations(conn, 'dimPerson')

  # any necessary actions here
  pass

  # to all foreign key dependancies
  pass

def cascadeRetirements(conn, source):
  # to all foreign key dependancies first
  pass

  #any necessary actions here
  resolveStagingFKs(conn)
  pushDeActivations(conn)

  # to all children
  if (source != 'dimPersonRole'):
    dimPersonRole.cascadeRetirements(conn, 'dimPerson')

def resolveStagingFKs(conn):
  LOGGING.debug("resolveStagingFKs()")
  with conn.cursor() as cur:
    cur.execute("""
      UPDATE
        stg.dimPerson   s
      SET
        id = dp.id
      FROM
        dim.dimPerson   dp
      WHERE
        dp.externalReference = s.externalReference_Person
      ;
    """)
  conn.commit()

def registerInitialisations(conn, source, column_map):
  LOGGING.debug("registerInitialisations()")

  if ('source_file_name' not in column_map):
    column_map['source_file_name'] = "'<Initialise>'"

  if ('source_file_creation' not in column_map):
    column_map['source_file_creation'] = '-2'

  if ('profile_modify_date' not in column_map):
    column_map['profile_modify_date'] = '0'

  if ('status' not in column_map):
    column_map['status'] = """'Unknown(' || {externalReference_Person} || ')'""".format(**column_map)

  if ('first_name' not in column_map):
    column_map['first_name'] = """'Unknown(' || {externalReference_Person} || ')'""".format(**column_map)

  if ('middle_name' not in column_map):
    column_map['middle_name'] = """'Unknown(' || {externalReference_Person} || ')'""".format(**column_map)

  if ('last_name' not in column_map):
    column_map['last_name'] = """'Unknown(' || {externalReference_Person} || ')'""".format(**column_map)

  DBManager.registerInitialisations(
    conn            = conn,
    target          = 'stg.dimPerson',
    source          = source,
    allowed_columns = ['source_file_name', 'source_file_creation', 'externalReference_Person', 'status', 'first_name', 'middle_name', 'last_name', 'profile_modify_date'],
    column_map      = column_map,
    uniqueness      = 'source_file_name, source_file_creation, externalReference_Person'
  )

def pushDeActivations(conn):
  LOGGING.debug("pushDeActivations()")
  with conn.cursor() as cur:
    cur.execute("""
      INSERT INTO
        stg.dimPersonRole
        (
          source_file_name,
          source_file_creation,
          externalReference_Person,
          externalReference_Role,
          effective_from,
          is_active,
          _staging_mode
        )
      SELECT
        stg.source_file_name,
        stg.source_file_creation,
        stg.externalReference_Person,
        NULL,
        stg.profile_modify_date,
        FALSE,
        'U'
      FROM
      (
        SELECT DISTINCT source_file_name, source_file_creation, externalReference_Person, profile_modify_date
          FROM stg.dimPerson
      )
        stg
      ON CONFLICT
        (
          source_file_name,
          source_file_creation,
          externalReference_Person,
          externalReference_Role,
          effective_from
        )
          DO NOTHING
      ;
    """)
  conn.commit()

def applyChanges(conn, source):
  if (source is None):
    cascadeActivations(conn, 'dimPerson')
    cascadeRetirements(conn, 'dimPerson')

  ################################################################################
  # apply changes to "associates" here
  # - Any table to which this one has a foreign key
  #
  # apply my own changes
  #
  # apply changes to "children"
  ################################################################################

  # No "associates" at present

  LOGGING.debug("applyChanges()")
  _applyChanges(conn)

  children = ['dimPersonRole']
  if (source not in children):
    dimPersonRole.applyChanges(conn, 'dimPerson')

def _applyChanges(conn):
  with conn.cursor() as cur:
    cur.execute("""
      DELETE FROM
        dim.dimPerson   d
      USING
        stg.dimPerson   s
      WHERE
            s.id      = d.id
        AND s._staging_mode = 'D'
      ;
      
      INSERT INTO
        dim.dimPerson   AS d
          (
            externalReference,
            status,
            first_name,
            middle_name,
            last_name,
            profile_modify_date
          )
      SELECT DISTINCT ON (s.externalReference_Person)
        s.externalReference_Person,
        s.status,
        s.first_name,
        s.middle_name,
        s.last_name,
        s.profile_modify_date
      FROM
        stg.dimPerson   s
      WHERE
            (s._staging_mode = 'I' AND s.id IS NULL)
        OR  (s._staging_mode = 'U'                 )
      ORDER BY
        s.externalReference_Person,
        s.source_file_creation DESC,
        s.source_file_name DESC,
        s.profile_modify_date DESC
      ON CONFLICT
        (externalReference)
          DO UPDATE
            SET status              = EXCLUDED.status,
                first_name          = EXCLUDED.first_name,
                middle_name         = EXCLUDED.middle_name,
                last_name           = EXCLUDED.last_name,
                profile_modify_date = EXCLUDED.profile_modify_date
      ;
      
      DELETE FROM
        stg.dimPerson
      ;
    """)
  conn.commit()
