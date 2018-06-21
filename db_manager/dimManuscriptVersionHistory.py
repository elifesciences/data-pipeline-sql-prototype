import csv
import psycopg2.extras

from . import DBManager
from . import dimManuscriptVersion
from . import dimPerson
from . import dimStage

def stage_csv(conn, manuscriptVersionHistory):
  file_path = manuscriptVersionHistory
  with open(file_path, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    # ToDo: Validation of column names and types
    with conn.cursor() as cur:
      psycopg2.extras.execute_values(
        cur,
        """
          INSERT INTO
            stg.dimManuscriptVersionStageHistory(
              create_date,
              zip_name,
              externalReference_Manuscript,
              externalReference_ManuscriptVersion,
              externalReference_ManuscriptVersionStage,
              externalReference_Stage,
              externalReference_Person_Affective,
              externalReference_Person_TriggeredBy,
              epoch_startDate
            )
          VALUES
            %s""",
        reader,
        template="""(
          %(create_date)s,
          %(zip_name)s,
          %(xml_file_name)s,
          %(version_position_in_ms)s,
          %(stage_position_in_version)s,
          %(name)s,
          %(affective_person_id)s,
          %(triggered_by_person)s,
          %(start_date)s
        )""",
        page_size=1000
      )
      # ToDo: Logging of rows upload, time taken, etc
      conn.commit()
  prep(conn)

def prep(conn):
  dimManuscriptVersion.registerInitialisations(
    conn,
    """
      (
        SELECT DISTINCT
          externalReference_Manuscript,
          externalReference_ManuscriptVersion
      FROM
          stg.dimManuscriptVersionStageHistory
      )
        {alias}
    """,
    {
      'externalReference_Manuscript':        'externalReference_Manuscript',
      'externalReference_ManuscriptVersion': 'externalReference_ManuscriptVersion'
    }
  )

  dimPerson.registerInitialisations(
    conn,
    """
      (
        SELECT externalReference_Person_Affective   FROM stg.dimManuscriptVersionStageHistory
        UNION
        SELECT externalReference_Person_TriggeredBy FROM stg.dimManuscriptVersionStageHistory
      )
        {alias}(externalReference_Person)
    """,
    {'externalReference_Person': 'externalReference_Person'}
  )

  dimStage.registerInitialisations(
    conn,
    """
      (
        SELECT DISTINCT externalReference_Stage FROM stg.dimManuscriptVersionStageHistory
      )
        {alias}
    """,
    {'externalReference_Stage': 'externalReference_Stage'}
  )

  resolveStagingFKs(conn)

def resolveStagingFKs(conn):
  with conn.cursor() as cur:
    cur.execute("""
      UPDATE
        stg.dimManuscriptVersionStageHistory   s
      SET
        id = dmvsh.id
      FROM
        dim.dimManuscript                      dm
      INNER JOIN
        dim.dimManuscriptVersion               dmv
          ON dmv.ManuscriptID = dm.id
      INNER JOIN
        dim.dimManuscriptVersionStageHistory   dmvsh
          ON dmvsh.ManuscriptVersionID = dmv.id
      WHERE
               dm.externalReference = s.externalReference_Manuscript
        AND   dmv.externalReference = s.externalReference_ManuscriptVersion
        AND dmvsh.externalReference = s.externalReference_ManuscriptVersionStage
      ;
    """)
  conn.commit()

def registerInitialisations(conn, source, column_map):
  DBManager.registerInitialisations(
    conn            = conn,
    target          = 'stg.dimManuscriptVersionStageHistory',
    source          = source,
    allowed_columns = ['externalReference_Manuscript', 'externalReference_ManuscriptVersion', 'externalReference_ManuscriptVersionStage'],
    column_map      = column_map,
    uniqueness      = 'externalReference_Manuscript, externalReference_ManuscriptVersion, externalReference_ManuscriptVersionStage'
  )

def pushDeletes(conn):
  pass

def applyChanges(conn, _has_applied_parents=False):
  if (not _has_applied_parents):
    dimManuscriptVersion.applyChanges(conn)
    return

  dimPerson.applyChanges(conn)
  dimStage.applyChanges(conn)

  with conn.cursor() as cur:
    cur.execute("""
      INSERT INTO
        dim.dimManuscriptVersionStageHistory   AS d
          (
            manuscriptVersionID,
            externalReference,
            stageID,
            personID_affective,
            personID_triggeredBy,
            epoch_startDate
          )
      SELECT
        dmv.id,
        s.externalReference_ManuscriptVersionStage,
        ds.id,
        dp_a.id,
        dp_t.id,
        s.epoch_startDate
      FROM
        stg.dimManuscriptVersionStageHistory   s
      LEFT JOIN
        dim.dimManuscript                      dm
          ON  dm.externalReference  = s.externalReference_Manuscript
      LEFT JOIN
        dim.dimManuscriptVersion               dmv
          ON  dmv.externalReference = s.externalReference_ManuscriptVersion
          AND dmv.ManuscriptID      = dm.id
      LEFT JOIN
        dim.dimStage                           ds
          ON  ds.externalReference = s.externalReference_Stage
      LEFT JOIN
        dim.dimPerson                          dp_a
          ON  dp_a.externalReference = s.externalReference_Person_Affective
      LEFT JOIN
        dim.dimPerson                          dp_t
          ON  dp_t.externalReference = s.externalReference_Person_TriggeredBy
      ON CONFLICT
        (manuscriptVersionID, externalReference)
          DO UPDATE
            SET stageID               = EXCLUDED.stageID,
                personID_affective    = EXCLUDED.personID_affective,
                personID_triggeredBy  = EXCLUDED.personID_triggeredBy,
                epoch_startDate       = EXCLUDED.epoch_startDate
      ;
    """)
  conn.commit()
