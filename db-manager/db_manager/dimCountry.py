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
        """INSERT INTO stg.dimCountry(externalReference_Country, countryLabel) VALUES %s""",
        reader,
        template="(%(externalReference_Country)s, %(countryLabel)s)",
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
        stg.dimCountry   s
      SET
        id = dp.id
      FROM
        dim.dimCountry   dp
      WHERE
        dp.externalReference = s.externalReference_Country
      ;
    """)
  conn.commit()

def registerInitialisations(conn, source, column_map):

  if ('countryLabel' not in column_map):
    column_map['countryLabel'] = """{externalReference_Country}""".format(**column_map)

  DBManager.registerInitialisations(
    conn            = conn,
    target          = 'stg.dimCountry',
    source          = source,
    allowed_columns = ['externalReference_Country', 'countryLabel'],
    column_map      = column_map,
    uniqueness      = 'externalReference_Country'
  )

def pushDeletes(conn):
  pass

def applyChanges(conn):
  with conn.cursor() as cur:
    cur.execute("""
      DELETE FROM
        dim.dimStage   d
      USING
        stg.dimStage   s
      WHERE
            s.id            = d.id
        AND s._staging_mode = 'D'
      ;
      
      INSERT INTO
        dim.dimCountry   AS d
          (
            externalReference,
            countryLabel
          )
      SELECT
        s.externalReference_Country,
        s.countryLabel
      FROM
        stg.dimCountry   s
      WHERE
            (s._staging_mode = 'I' AND s.id IS NULL)
        OR  (s._staging_mode = 'U'                 )
      ON CONFLICT
        (externalReference)
          DO UPDATE
            SET countryLabel = EXCLUDED.countryLabel
      ;
      
      DELETE FROM
        stg.dimCountry
      ;
    """)
  conn.commit()