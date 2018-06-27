import csv
import psycopg2.extras

from . import DBManager
from . import dimManuscript
from . import dimManuscriptVersionHistory

def stage_csv(conn, manuscriptVersion, manuscriptVersionHistory):
  file_path = manuscriptVersion
  with open(file_path, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    # ToDo: Validation of column names and types
    with conn.cursor() as cur:
      psycopg2.extras.execute_values(
        cur,
        """
          INSERT INTO
            stg.dimManuscriptVersion(
              create_date,
              zip_name,
              externalReference_Manuscript,
              externalReference_ManuscriptVersion,
              decision,
              ms_type
            )
          VALUES
            %s""",
        reader,
        template="""(
          %(create_date)s,
          %(zip_name)s,
          %(xml_file_name)s,
          %(version_position_in_ms)s,
          %(decision)s,
          %(ms_type)s
        )""",
        page_size=1000
      )
      # ToDo: Logging of rows upload, time taken, etc
      conn.commit()
  dimManuscriptVersionHistory.stage_csv(conn, manuscriptVersionHistory)
  prep(conn)

def prep(conn):
  dimManuscript.registerInitialisations(
    conn,
    """
      (
        SELECT DISTINCT externalReference_Manuscript FROM stg.dimManuscriptVersion
      )
        {alias}
    """,
    {'externalReference_Manuscript': 'externalReference_Manuscript'}
  )

  resolveStagingFKs(conn)
  pushDeletes(conn)

def resolveStagingFKs(conn):
  with conn.cursor() as cur:
    cur.execute("""
      UPDATE
        stg.dimManuscriptVersion   s
      SET
        id = dmv.id
      FROM
        dim.dimManuscript          dm
      INNER JOIN
        dim.dimManuscriptVersion   dmv
          ON dmv.ManuscriptID = dm.id
      WHERE
             dm.externalReference = s.externalReference_Manuscript
        AND dmv.externalReference = s.externalReference_ManuscriptVersion
      ;
    """)
  conn.commit()

def registerInitialisations(conn, source, column_map):
  DBManager.registerInitialisations(
    conn            = conn,
    target          = 'stg.dimManuscriptVersion',
    source          = source,
    allowed_columns = ['externalReference_Manuscript', 'externalReference_ManuscriptVersion'],
    column_map      = column_map,
    uniqueness      = 'externalReference_Manuscript, externalReference_ManuscriptVersion'
  )

def pushDeletes(conn):
  with conn.cursor() as cur:
    cur.execute("""
      INSERT INTO
        stg.dimManuscriptVersionStageHistory
        (
          id,
          externalReference_Manuscript,
          externalReference_ManuscriptVersion,
          externalReference_ManuscriptVersionStage,
          _staging_mode
        )
      SELECT
        dmvh.id,
        dmv.externalReference_Manuscript,
        dmv.externalReference_ManuscriptVersion,
        dmvh.externalReference,
        'D'
      FROM
      (
        SELECT id, externalReference_Manuscript, externalReference_ManuscriptVersion
          FROM stg.dimManuscriptVersion
         WHERE _staging_mode <> 'I' 
           AND id IS NOT NULL
      )
        dmv
      INNER JOIN
        dim.dimManuscriptVersionStageHistory   dmvh
          ON  dmvh.manuscriptVersionID = dmv.id
      ON CONFLICT
        (externalReference_Manuscript, externalReference_ManuscriptVersion, externalReference_ManuscriptVersionStage)
          DO NOTHING
      ;
    """)
  conn.commit()

def applyChanges(conn, _has_applied_parents=False):
  if (not _has_applied_parents):
    dimManuscript.applyChanges(conn)
    return

  with conn.cursor() as cur:
    cur.execute("""
      DELETE FROM
        dim.dimManuscriptVersion   d
      USING
        stg.dimManuscriptVersion   s
      WHERE
            s.id            = d.id
        AND s._staging_mode = 'D'
      ;

      INSERT INTO
        dim.dimManuscriptVersion   AS d
          (
            manuscriptID,
            externalReference,
            decision,
            ms_type
          )
      SELECT
        m.id,
        s.externalReference_ManuscriptVersion,
        s.decision,
        s.ms_type
      FROM
        stg.dimManuscriptVersion   s
      LEFT JOIN
        dim.dimManuscript          m
          ON  m.externalReference = s.externalReference_Manuscript
      ON CONFLICT
        (manuscriptID, externalReference)
          DO UPDATE
            SET decision = EXCLUDED.decision,
                ms_type  = EXCLUDED.ms_type
      ;
      
      DELETE FROM
        stg.dimManuscriptVersion
      ;
    """)
  conn.commit()

  dimManuscriptVersionHistory.applyChanges(conn, _has_applied_parents=True)
