import csv
import psycopg2.extras

from . import DBManager
from . import dimRole
from . import dimPerson

def stage_csv(conn, file_path):
  with open(file_path, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    # ToDo: Validation of column names and types
    with conn.cursor() as cur:
      psycopg2.extras.execute_values(
        cur,
        """
          INSERT INTO
            stg.dimPersonRole(
              source_file_name,
              source_file_creation,
              externalReference_Person,
              externalReference_Role,
              effective_from
            )
          VALUES %s
        """,
        reader,
        template="""(
            %(zip_name)s || '/' || %(xml_file_name)s,
            %(create_date)s,
            %(person_id)s,
            %(role)s,
            %(profile_modify_date)s
        )""",
        page_size=1000
      )
      # ToDo: Logging of rows upload, time taken, etc
      conn.commit()
  prep(conn)

def prep(conn):
  dimRole.registerInitialisations(
    conn,
    """
      (
        SELECT DISTINCT externalReference_Role FROM stg.dimPersonRole WHERE externalReference_Role IS NOT NULL
      )
        {alias}
    """,
    {'externalReference_Role': 'externalReference_Role'}
  )
  dimPerson.registerInitialisations(
    conn,
    """
      (
        SELECT DISTINCT externalReference_Person FROM stg.dimPersonRole
      )
        {alias}
    """,
    {'externalReference_Person': 'externalReference_Person'}
  )
  applyOrderOfPrecedence(conn)
  stageDeactivations(conn)
  resolveStagingFKs(conn)
  stageInitsTruncsAndUpdates(conn)

def applyOrderOfPrecedence(conn):
  with conn.cursor() as cur:
    cur.execute("""
      WITH
        collapsed
      AS
      (
        SELECT DISTINCT
          externalreference_person,
          source_file_creation,
          source_file_name,
          effective_from
        FROM
          stg.dimpersonrole
      ),
        running_lowest
      AS
      (
        SELECT
          *,
          MIN(effective_from)
            OVER (PARTITION BY externalreference_person
                      ORDER BY source_file_creation DESC,    -- Newer files have precedence (as if applied over the top of older files)
                               source_file_name DESC,        -- Tie breaker : Same Person, Multiple Files, Same Creation Date - Behave as if applied in name order
                               effective_from DESC           -- Where one file contains a sequence of changes, apply the changes in chronological order
                 )
                   AS lowest_effective_from
        FROM
          collapsed
      ),
        lagged
      AS
      (
        SELECT
          *,
          LAG(effective_from, 1, 2147483647)
            OVER (PARTITION BY externalreference_person
                      ORDER BY source_file_creation DESC,    -- Newer files have precedence (as if applied over the top of older files)
                               source_file_name DESC,        -- Tie breaker : Same Person, Multiple Files, Same Creation Date - Behave as if applied in name order
                               effective_from DESC           -- Where one file contains a sequence of changes, apply the changes in chronological order
                 )
                   AS next_effective_from
        FROM
          running_lowest
        WHERE
          effective_from = lowest_effective_from             -- If the effective_from is higher than the lowest encountered so far, it's from an older file so can be ignored
      )
      UPDATE
        stg.dimpersonrole
      SET
        effective_stop = lagged.next_effective_from
      FROM
        lagged
      WHERE
            lagged.effective_from           < lagged.next_effective_from                     -- Prevent zero duration periods
        AND lagged.externalreference_person = stg.dimpersonrole.externalreference_person
        AND lagged.source_file_name         = stg.dimpersonrole.source_file_name
        AND lagged.source_file_creation     = stg.dimpersonrole.source_file_creation
        AND lagged.effective_from           = stg.dimpersonrole.effective_from
      ;
    """)
  conn.commit()

def stageDeactivations(conn):
  with conn.cursor() as cur:
    cur.execute("""
      INSERT INTO
        stg.dimPersonRole (
          source_file_name,
          source_file_creation,
          externalreference_person,
          externalreference_role,
          effective_from,
          effective_stop,
          is_active,
          _staging_mode
        )
      SELECT
        '<De-Activate>',
        -1,
        stg.externalreference_person,
        dr.externalreference,
        stg.effective_from,
        stg.effective_stop,
        FALSE,
        'U'
      FROM
      (
        SELECT
          externalreference_person,
          effective_from,
          effective_stop
        FROM
          stg.dimPersonRole
        WHERE
          effective_stop IS NOT NULL  -- staging row not superceded by another staging row
        GROUP BY
          externalreference_person,
          effective_from,
          effective_stop
      )
        stg
      INNER JOIN
        dim.dimPerson       AS dp
          ON dp.externalReference = stg.externalreference_person
      INNER JOIN
      (
        SELECT
          personID,
          roleID
        FROM
          dim.dimPersonRole
        GROUP BY
          personID,
          roleID
      )
        dpr
          ON dpr.personID = dp.id
      INNER JOIN
        dim.dimRole         AS dr
          ON dr.id = dpr.roleID
      LEFT JOIN
        stg.dimPersonRole   AS chk
          ON  chk.externalreference_person = dp.externalreference
          AND chk.externalreference_role   = dr.externalreference
          AND chk.effective_from           = stg.effective_from
          AND chk.effective_stop          IS NOT NULL
      WHERE
        chk.externalreference_person IS NULL
      ;
    """)
  conn.commit()

def resolveStagingFKs(conn):
  with conn.cursor() as cur:
    cur.execute("""
      UPDATE
        stg.dimPersonRole   AS stg
      SET
        personID               = dpr.personID,
        roleID                 = dpr.RoleID,
        current_effective_from = dpr.effective_from,
        current_effective_stop = dpr.effective_stop
      FROM
        dim.dimPersonRole   AS dpr
      INNER JOIN
        dim.dimPerson       AS dp
          ON dp.id = dpr.personID
      INNER JOIN
        dim.dimRole         AS dr
          ON dr.id = dpr.roleID
      WHERE
            stg.externalReference_Person  = dp.externalReference
        AND stg.externalReference_Role    = dr.externalReference
        AND stg.effective_from           >= dpr.effective_from
        AND stg.effective_from           <  dpr.effective_stop
      ;
    """)
  conn.commit()

def stageInitsTruncsAndUpdates(conn):
  with conn.cursor() as cur:
    cur.execute("""
      INSERT INTO
        stg.dimPersonRole (
          source_file_name,
          source_file_creation,
          externalreference_person,
          externalreference_role,
          effective_from,
          effective_stop,
          is_active,
          _staging_mode
        )
      (
        --------------------------------------------------------------------------------
        --------------------------------------------------------------------------------
        -- Entities that need initialising
        --------------------------------------------------------------------------------
        --------------------------------------------------------------------------------
        SELECT
          '<Initialise>',
          -2,
          externalreference_person,
          externalreference_role,
          0,
          MIN(effective_from),
          FALSE,
          'I'
        FROM
          stg.dimPersonRole
        WHERE
              personID       IS     NULL  -- New Entity in need of initialisation
          AND effective_stop IS NOT NULL  -- Row not superceded by another staging row
          AND externalreference_role IS NOT NULL -- Is not a dummy record [see dimPerson.pushDeActivations()]
        GROUP BY
          externalreference_person,
          externalreference_role
        HAVING
          MIN(effective_from) >  0  -- Prevent zero length periods
        --------------------------------------------------------------------------------
      )
      ---------
      UNION ALL
      ---------
      (
        --------------------------------------------------------------------------------
        --------------------------------------------------------------------------------
        -- Associations then need updating (new association overlaps existing start)
        --------------------------------------------------------------------------------
        --------------------------------------------------------------------------------
        SELECT
          DISTINCT ON (
            stg.externalreference_person,
            stg.externalreference_role,
            dim.effective_from
          )
          '<'
      	|| CASE WHEN dim.effective_from < stg.effective_from THEN 'Truncate'
      	        WHEN dim.effective_stop > stg.effective_stop THEN 'Truncate'
      	                                                     ELSE '' END
      	|| CASE WHEN dim.effective_from < stg.effective_from THEN ''
      	                                                     ELSE 'Update' END
      	|| '>',
          -3,
          stg.externalreference_person,
          stg.externalreference_role,
          dim.effective_from,
          CASE WHEN dim.effective_from < stg.effective_from THEN stg.effective_from
               WHEN dim.effective_stop > stg.effective_stop THEN stg.effective_stop
                                                            ELSE dim.effective_stop END,
          CASE WHEN dim.effective_from < stg.effective_from THEN dim.is_active
                                                            ELSE stg.is_active END,
          'U'
        FROM
          stg.dimPersonRole   AS stg
        INNER JOIN
          dim.dimPersonRole   AS dim
            ON  dim.personID       = stg.personID
            AND dim.roleID         = stg.roleID
            AND dim.effective_from < stg.effective_stop
            AND dim.effective_stop > stg.effective_from
        ORDER BY
          stg.externalreference_person,
          stg.externalreference_role,
          dim.effective_from,
          stg.effective_from
        --------------------------------------------------------------------------------
      );
    """)
  conn.commit()

def applyChanges(conn, _has_applied_parents=False):
  if (not _has_applied_parents):
    dimPerson.applyChanges(conn)
    return

  dimRole.applyChanges(conn)

  with conn.cursor() as cur:
    cur.execute("""
      DELETE FROM
        dim.dimPersonRole   d
      USING
        stg.dimPersonRole   s
      WHERE
            s.PersonID      = d.PersonID
        AND s.RoleID        = d.RoleID
        AND s._staging_mode = 'D'
      ;
      
      INSERT INTO
        dim.dimPersonRole (
          PersonID,
          RoleID,
          effective_from,
          effective_stop,
          is_active
        )
      SELECT
        dp.id,
        dr.id,
        stg.effective_from,
        CASE WHEN stg.current_effective_stop < stg.effective_stop THEN stg.current_effective_stop ELSE stg.effective_stop END,
        stg.is_active
      FROM
        stg.dimPersonRole   stg
      INNER JOIN
        dim.DimPerson       dp
          ON dp.externalReference = stg.externalReference_Person
      INNER JOIN
        dim.DimRole         dr
          ON dr.externalReference = stg.externalReference_Role
      WHERE
        stg.effective_stop IS NOT NULL
      ON CONFLICT
        (
          personID,
          roleID,
          effective_from
        )
          DO UPDATE
            SET effective_stop = EXCLUDED.effective_stop,
                is_active      = EXCLUDED.is_active
      ;
      
      DELETE FROM
        stg.dimPersonRole
      ;
    """)
  conn.commit()
