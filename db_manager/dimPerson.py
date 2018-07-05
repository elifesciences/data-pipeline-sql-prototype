import csv
import psycopg2.extras

from . import DBManager
from . import dimPersonRole

def stage_csv(conn, person, person_role):
  file_path = person
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
            %(profile_modify_date)s
          )
        """,
        page_size=1000
      )
      # ToDo: Logging of rows upload, time taken, etc
      conn.commit()
  print(1)
  pushDeActivations(conn)
  print(2)
  dimPersonRole.stage_csv(conn, person_role)
  print(3)
  prep(conn)
  print(4)

def prep(conn):
  resolveStagingFKs(conn)

def resolveStagingFKs(conn):
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

  if ('source_file_name' not in column_map):
    column_map['source_file_name'] = "'<Initialise>'"

  if ('source_file_creation' not in column_map):
    column_map['source_file_creation'] = '-2'

  if ('profile_modify_date' not in column_map):
    column_map['profile_modify_date'] = '0'

  DBManager.registerInitialisations(
    conn            = conn,
    target          = 'stg.dimPerson',
    source          = source,
    allowed_columns = ['source_file_name', 'source_file_creation', 'externalReference_Person', 'profile_modify_date'],
    column_map      = column_map,
    uniqueness      = 'source_file_name, source_file_creation, externalReference_Person'
  )

def pushDeActivations(conn):
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

def applyChanges(conn):
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
            profile_modify_date
          )
      SELECT DISTINCT ON (s.externalReference_Person)
        s.externalReference_Person,
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
            SET profile_modify_date = EXCLUDED.profile_modify_date
      ;
      
      DELETE FROM
        stg.dimPerson
      ;
    """)
  conn.commit()
  
  dimPersonRole.applyChanges(conn, _has_applied_parents=True)
