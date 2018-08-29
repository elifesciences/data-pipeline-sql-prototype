import csv
import logging
from typing import Iterable

import psycopg2.extras

from . import DBManager
from . import dimManuscriptVersion
from . import dimPerson
from . import dimStage


LOGGING = logging.getLogger(__name__)


def stage_iterable(conn, iterable: Iterable[dict]):
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
            %s
            """,
            iterable,
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


def stage_csv(conn, file_path):
    LOGGING.debug("StagingFile '{file}'".format(file=file_path))
    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        stage_iterable(conn, reader)


def cascadeActivations(conn, source):
    # to all children first
    pass

    # any necessary actions here
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

    # to all foreign key dependancies
    dimStage.cascadeActivations(conn, 'dimManuscriptVersionHistory')
    dimPerson.cascadeActivations(conn, 'dimManuscriptVersionHistory')
    if (source != 'dimManuscriptVersion'):
        dimManuscriptVersion.cascadeActivations(
            conn, 'dimManuscriptVersionHistory'
        )


def cascadeRetirements(conn, source):
    # to all foreign key dependancies first
    dimStage.cascadeRetirements(conn, 'dimManuscriptVersionHistory')
    dimPerson.cascadeRetirements(conn, 'dimManuscriptVersionHistory')
    if (source != 'dimManuscriptVersion'):
        dimManuscriptVersion.cascadeRetirements(
            conn, 'dimManuscriptVersionHistory'
        )

    # any necessary actions here
    resolveStagingFKs(conn)

    # to all children
    pass


def resolveStagingFKs(conn):
    LOGGING.debug("resolveStagingFKs()")
    with conn.cursor() as cur:
        cur.execute(
            """
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
            """
        )
    conn.commit()


def registerInitialisations(conn, source, column_map):
    LOGGING.debug("registerInitialisations()")
    DBManager.registerInitialisations(
        conn=conn,
        target='stg.dimManuscriptVersionStageHistory',
        source=source,
        allowed_columns=['externalReference_Manuscript',
                         'externalReference_ManuscriptVersion', 'externalReference_ManuscriptVersionStage'],
        column_map=column_map,
        uniqueness='externalReference_Manuscript, externalReference_ManuscriptVersion, externalReference_ManuscriptVersionStage'
    )


def applyChanges(conn, source):
    if (source is None):
        cascadeActivations(conn, 'dimManuscriptVersionHistory')
        cascadeRetirements(conn, 'dimManuscriptVersionHistory')

    if (source != 'dimManuscriptVersion'):
        dimManuscriptVersion.applyChanges(conn, 'dimManuscriptVersionHistory')

    if (source != 'dimPerson'):
        dimPerson.applyChanges(conn, 'dimManuscriptVersionHistory')
    if (source != 'dimStage'):
        dimStage.applyChanges(conn, 'dimManuscriptVersionHistory')

    LOGGING.debug("applyChanges()")
    _applyChanges(conn)


def _applyChanges(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
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
            WHERE
                  (s._staging_mode = 'I' AND s.id IS NULL)
              OR  (s._staging_mode = 'U'                 )
            ON CONFLICT
              (manuscriptVersionID, externalReference)
                DO UPDATE
                  SET stageID               = EXCLUDED.stageID,
                      personID_affective    = EXCLUDED.personID_affective,
                      personID_triggeredBy  = EXCLUDED.personID_triggeredBy,
                      epoch_startDate       = EXCLUDED.epoch_startDate
            ;
            
            DELETE FROM
              stg.dimManuscriptVersionStageHistory
            ;
            """
        )
    conn.commit()
