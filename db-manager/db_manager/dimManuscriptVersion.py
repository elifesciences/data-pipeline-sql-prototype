import csv
import logging
from typing import Iterable

import psycopg2.extras

from . import DBManager
from . import dimManuscript
from . import dimManuscriptVersionHistory


LOGGING = logging.getLogger(__name__)


def stage_iterable(conn, iterable: Iterable[dict]):
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
            %s
            """,
            iterable,
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


def stage_csv(conn, file_path):
    LOGGING.debug("StagingFile '{file}'".format(file=file_path))
    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        # ToDo: Validation of column names and types
        stage_iterable(conn, reader)


def cascadeActivations(conn, source):
    # to all children first
    if (source != 'dimManuscriptVersionHistory'):
        dimManuscriptVersionHistory.cascadeActivations(
            conn, 'dimManuscriptVersion'
        )

    # any necessary actions here
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

    # to all foreign key dependancies
    if (source != 'dimManuscript'):
        dimManuscript.cascadeActivations(conn, 'dimManuscriptVersion')


def cascadeRetirements(conn, source):
    # to all foreign key dependancies first
    if (source != 'dimManuscript'):
        dimManuscript.cascadeRetirements(conn, 'dimManuscriptVersion')

    # any necessary actions here
    resolveStagingFKs(conn)
    pushDeletes(conn)

    # to all children
    if (source != 'dimManuscriptVersionHistory'):
        dimManuscriptVersionHistory.cascadeRetirements(
            conn, 'dimManuscriptVersion')


def resolveStagingFKs(conn):
    LOGGING.debug("resolveStagingFKs()")
    with conn.cursor() as cur:
        cur.execute(
            """
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
            """
        )
    conn.commit()


def registerInitialisations(conn, source, column_map):
    LOGGING.debug("registerInitialisations()")
    DBManager.registerInitialisations(
        conn=conn,
        target='stg.dimManuscriptVersion',
        source=source,
        allowed_columns=['externalReference_Manuscript',
                         'externalReference_ManuscriptVersion'],
        column_map=column_map,
        uniqueness='externalReference_Manuscript, externalReference_ManuscriptVersion'
    )


def pushDeletes(conn):
    LOGGING.debug("pushDeletes()")
    with conn.cursor() as cur:
        cur.execute(
            """
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
            """
        )
    conn.commit()


def applyChanges(conn, source):
    if (source is None):
        cascadeActivations(conn, 'dimManuscriptVersion')
        cascadeRetirements(conn, 'dimManuscriptVersion')

    if (source != 'dimManuscript'):
        dimManuscript.applyChanges(conn, 'dimManuscriptVersion')

    LOGGING.debug("applyChanges()")
    _applyChanges(conn)

    children = ['dimManuscriptVersionHistory']
    if (source not in children):
        dimManuscriptVersionHistory.applyChanges(conn, 'dimManuscriptVersion')


def _applyChanges(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
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
            WHERE
                  (s._staging_mode = 'I' AND s.id IS NULL)
              OR  (s._staging_mode = 'U'                 )
            ON CONFLICT
              (manuscriptID, externalReference)
                DO UPDATE
                  SET decision = EXCLUDED.decision,
                      ms_type  = EXCLUDED.ms_type
            ;
            
            DELETE FROM
              stg.dimManuscriptVersion
            ;
            """
        )
    conn.commit()
