import csv
import logging

import psycopg2.extras

from . import DBManager
from . import dimCountry
from . import dimManuscriptVersion


LOGGING = logging.getLogger(__name__)


def stage_csv(conn, file_path):
    LOGGING.debug("StagingFile '{file}'".format(file=file_path))
    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        # ToDo: Validation of column names and types
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO
                  stg.dimManuscript(
                    create_date,
                    externalReference_Manuscript,
                    msid,
                    externalReference_country,
                    doi
                  )
                VALUES
                %s
                """,
                reader,
                template="""(
                  %(create_date)s,
                  %(source_file_name)s,
                  %(msid)s,
                  %(country)s,
                  %(doi)s
                )""",
                page_size=1000
            )
            # ToDo: Logging of rows upload, time taken, etc
            conn.commit()


def cascadeActivations(conn, source):
    # to all children first
    if (source != 'dimManuscriptVersion'):
        dimManuscriptVersion.cascadeActivations(conn, 'dimManuscript')

    # any necessary actions here
    dimCountry.registerInitialisations(
        conn,
        """
          (
            SELECT DISTINCT externalReference_Country FROM stg.dimManuscript
          )
            {alias}
        """,
        {'externalReference_Country': 'externalReference_Country'}
    )

    # to all foreign key dependancies
    dimCountry.cascadeActivations(conn, 'dimManuscript')


def cascadeRetirements(conn, source):
    # to all foreign key dependancies first
    dimCountry.cascadeRetirements(conn, 'dimManuscript')

    # any necessary actions here
    resolveStagingFKs(conn)
    pushDeletes(conn)

    # to all children
    if (source != 'dimManuscriptVersion'):
        dimManuscriptVersion.cascadeRetirements(conn, 'dimManuscript')


def resolveStagingFKs(conn):
    LOGGING.debug("resolveStagingFKs()")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE
              stg.dimManuscript   s
            SET
              id = dm.id
            FROM
              dim.dimManuscript   dm
            WHERE
              dm.externalReference = s.externalReference_Manuscript
            ;
            """
        )
    conn.commit()


def registerInitialisations(conn, source, column_map):
    LOGGING.debug("registerInitialisations()")
    DBManager.registerInitialisations(
        conn=conn,
        target='stg.dimManuscript',
        source=source,
        allowed_columns=['externalReference_Manuscript'],
        column_map=column_map,
        uniqueness='externalReference_Manuscript'
    )


def pushDeletes(conn):
    LOGGING.debug("pushDeletes()")
    with conn.cursor() as cur:
        cur.execute(
            """
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
            """
        )
    conn.commit()


def applyChanges(conn, source):
    if (source is None):
        cascadeActivations(conn, 'dimManuscript')
        cascadeRetirements(conn, 'dimManuscript')

    if (source != 'dimCountry'):
        dimCountry.applyChanges(conn, 'dimManuscript')

    LOGGING.debug("applyChanges()")
    _applyChanges(conn)

    if (source != 'dimManuscriptVersion'):
        dimManuscriptVersion.applyChanges(conn, 'dimManuscript')


def _applyChanges(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
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
                  msid,
                  country_id,
                  doi
                )
            SELECT DISTINCT
              s.externalReference_Manuscript,
              s.msid,
              c.id,
              s.doi
            FROM
              stg.dimManuscript   s
            INNER JOIN
              dim.dimCountry      c
                ON  c.externalReference = s.externalReference_Country
            WHERE
                  (s._staging_mode = 'I' AND s.id IS NULL)
              OR  (s._staging_mode = 'U'                 )
            ON CONFLICT
              (externalReference)
                DO UPDATE
                  SET msid       = EXCLUDED.msid,
                      country_id = EXCLUDED.country_id,
                      doi        = EXCLUDED.doi
            ;
            
            DELETE FROM
              stg.dimManuscript
            ;
            """
        )
    conn.commit()
