import csv
import psycopg2.extras

from . import DBManager

def stage_csv(conn, file_path):
  with open(file_path, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    # ToDo: Validation of column names and types
    with conn.cursor() as cur:
      psycopg2.extras.execute_values(
        cur,
        "INSERT INTO stg.dimPerson(externalReference_Person, name_full) VALUES %s",
        reader,
        template="(%(externalReference_Person)s, %(name_full)s)",
        page_size=1000
      )
      # ToDo: Logging of rows upload, time taken, etc
      conn.commit()
  prep(conn)

def prep(conn):
  resolveStagingFKs(conn)
  pushDeletes(conn)

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

  if ('name_full' not in column_map):
    column_map['name_full'] = """'Unknown (' || {externalReference_Person} || ')'""".format(**column_map)

  DBManager.registerInitialisations(
    conn            = conn,
    target          = 'stg.dimPerson',
    source          = source,
    allowed_columns = ['externalReference_Person', 'name_full'],
    column_map      = column_map,
    uniqueness      = 'externalReference_Person'
  )

def pushDeletes(conn):
  pass

def applyChanges(conn):
  with conn.cursor() as cur:
    cur.execute("""
      DELETE FROM
        dim.dimPerson   d
      USING
        stg.dimPerson   s
      WHERE
            s.id            = d.id
        AND s._staging_mode = 'D'
      ;
      
      INSERT INTO
        dim.dimPerson   AS d
          (
            externalReference,
            name_full
          )
      SELECT
        s.externalReference_Person,
        s.name_full
      FROM
        stg.dimPerson   s
      WHERE
            (s._staging_mode = 'I' AND s.id IS NULL)
        OR  (s._staging_mode = 'U'                 )
      ON CONFLICT
        (externalReference)
          DO UPDATE
            SET name_full = EXCLUDED.name_full
      ;
    """)
