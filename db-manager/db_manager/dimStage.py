import csv
import logging

import psycopg2.extras

from . import DBManager


LOGGING = logging.getLogger(__name__)


def stage_csv(conn, file_path):
    LOGGING.debug("StagingFile '{file}'".format(file=file_path))
    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        # ToDo: Validation of column names and types
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO stg.dimStage(externalReference_Stage, stageLabel) VALUES %s",
                reader,
                template="(%(externalReference_Stage)s, %(stageLabel)s)",
                page_size=1000
            )
            # ToDo: Logging of rows upload, time taken, etc
            conn.commit()


def cascadeActivations(conn, source):
    # to all children first
    pass

    # any necessary actions here
    pass

    # to all foreign key dependancies
    pass


def cascadeRetirements(conn, source):
    # to all foreign key dependancies first
    pass

    # any necessary actions here
    resolveStagingFKs(conn)

    # to all children
    pass


def resolveStagingFKs(conn):
    LOGGING.debug("ResolveStagingFKs()")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE
              stg.dimStage   s
            SET
              id = dp.id
            FROM
              dim.dimStage   dp
            WHERE
              dp.externalReference = s.externalReference_Stage
            ;
            """
        )
    conn.commit()


def registerInitialisations(conn, source, column_map):
    LOGGING.debug("registerInitialisations()")
    if ('stageLabel' not in column_map):
        column_map['stageLabel'] = """{externalReference_Stage}""".format(
            **column_map)
    DBManager.registerInitialisations(
        conn=conn,
        target='stg.dimStage',
        source=source,
        allowed_columns=['externalReference_Stage', 'stageLabel'],
        column_map=column_map,
        uniqueness='externalReference_Stage'
    )


def applyChanges(conn, source):
    if (source is None):
        cascadeActivations(conn, 'dimStage')
        cascadeRetirements(conn, 'dimStage')

    LOGGING.debug("ApplyChanges()")
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM
              dim.dimStage   d
            USING
              stg.dimStage   s
            WHERE
                  s.id            = d.id
              AND s._staging_mode = 'D'
            ;
            
            INSERT INTO
              dim.dimStage   AS d
                (
                  externalReference,
                  stageLabel
                )
            SELECT
              s.externalReference_Stage,
              s.stageLabel
            FROM
              stg.dimStage   s
            WHERE
                  (s._staging_mode = 'I' AND s.id IS NULL)
              OR  (s._staging_mode = 'U'                 )
            ON CONFLICT
              (externalReference)
                DO UPDATE
                  SET stageLabel = EXCLUDED.stageLabel
            ;
            
            DELETE FROM
              stg.dimStage
            ;
            """
        )
    conn.commit()
