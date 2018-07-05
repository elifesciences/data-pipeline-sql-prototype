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
        "INSERT INTO stg.dimRole(externalReference_Role, roleLabel) VALUES %s",
        reader,
        template="(%(externalReference_Role)s, %(roleLabel)s)",
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
        stg.dimRole   s
      SET
        id = d.id
      FROM
        dim.dimRole   d
      WHERE
        d.externalReference = s.externalReference_Role
      ;
    """)
  conn.commit()

def registerInitialisations(conn, source, column_map):

  if ('roleLabel' not in column_map):
    column_map['roleLabel'] = """{externalReference_Role}""".format(**column_map)

  DBManager.registerInitialisations(
    conn            = conn,
    target          = 'stg.dimRole',
    source          = source,
    allowed_columns = ['externalReference_Role', 'roleLabel'],
    column_map      = column_map,
    uniqueness      = 'externalReference_Role'
  )

def pushDeletes(conn):
  pass

def applyChanges(conn):
  with conn.cursor() as cur:
    cur.execute("""
      DELETE FROM
        dim.dimRole   d
      USING
        stg.dimRole   s
      WHERE
            s.id            = d.id
        AND s._staging_mode = 'D'
      ;
      
      INSERT INTO
        dim.dimRole   AS d
          (
            externalReference,
            roleLabel
          )
      SELECT
        s.externalReference_Role,
        s.roleLabel
      FROM
        stg.dimRole   s
      WHERE
            (s._staging_mode = 'I' AND s.id IS NULL)
        OR  (s._staging_mode = 'U'                 )
      ON CONFLICT
        (externalReference)
          DO UPDATE
            SET roleLabel = EXCLUDED.roleLabel
      ;
      
      DELETE FROM
        stg.dimRole
      ;
    """)
  conn.commit()
