import csv
import psycopg2.extras

from . import DBManager
from . import dimManuscriptVersion

def stage_csv(conn, manuscript, manuscriptVersion, manuscriptVersionHistory):
  file_path = manuscript
  with open(file_path, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    # ToDo: Validation of column names and types
    with conn.cursor() as cur:
      psycopg2.extras.execute_values(
        cur,
        "INSERT INTO stg.dimManuscript(externalReference_Manuscript, aDummyProperty) VALUES %s",
        reader,
        template="(%(externalReference_Manuscript)s, %(aDummyProperty)s)",
        page_size=1000
      )
      # ToDo: Logging of rows upload, time taken, etc
      conn.commit()
  dimManuscriptVersion.stage_csv(conn, manuscriptVersion, manuscriptVersionHistory)
  prep(conn)

def prep(conn):
  resolveStagingFKs(conn)
  pushDeletes(conn)

def resolveStagingFKs(conn):
  with conn.cursor() as cur:
    cur.execute("""
      UPDATE
        stg.dimManuscript   s
      SET
        id = dm.id
      FROM
        dim.dimManuscript   dm
      WHERE
        dm.externalReference = s.externalReference_Manuscript
      ;
    """)
  conn.commit()

def registerInitialisations(conn, source, column_map):
  DBManager.registerInitialisations(
    conn            = conn,
    target          = 'stg.dimManuscript',
    source          = source,
    allowed_columns = ['externalReference_Manuscript'],
    column_map      = column_map,
    uniqueness      = 'externalReference_Manuscript'
  )

def pushDeletes(conn):
  with conn.cursor() as cur:
    cur.execute("""
      INSERT INTO
        stg.dimManuscriptVersion
        (
          id,
          externalReference_Manuscript,
          externalReference_ManuscriptVersion,
          _staging_mode
        )
      SELECT
        dmv.id,
        dm.externalReference,
        dmv.externalReference,
        'D'
      FROM
      (
        SELECT DISTINCT id, externalReference_Manuscript AS externalReference
          FROM stg.dimManuscript
         WHERE _staging_mode <> 'I' 
           AND id IS NOT NULL
      )
        dm
      INNER JOIN
        dim.dimManuscriptVersion   dmv
          ON  dmv.manuscriptID = dm.id
      ON CONFLICT
        (externalReference_Manuscript, externalReference_ManuscriptVersion)
          DO NOTHING
      ;
    """)
  conn.commit()

def applyChanges(conn):
  with conn.cursor() as cur:
    cur.execute("""
      DELETE FROM
        dim.dimManuscript   d
      USING
        stg.dimManuscript   s
      WHERE
            s.id            = d.id
        AND s._staging_mode = 'D'
      ;
      
      INSERT INTO
        dim.dimManuscript   AS d
          (
            externalReference,
            aDummyProperty
          )
      SELECT DISTINCT
        s.externalReference_Manuscript,
        s.aDummyProperty
      FROM
        stg.dimManuscript   s
      WHERE
            (s._staging_mode = 'I' AND s.id IS NULL)
        OR  (s._staging_mode = 'U'                 )
      ON CONFLICT
        (externalReference)
          DO UPDATE
            SET aDummyProperty = EXCLUDED.aDummyProperty
      ;
    """)

  dimManuscriptVersion.applyChanges(conn, _has_applied_parents=True)
