#!/usr/bin/python3

import os

import psycopg2

import db_manager.dimManuscript
import db_manager.dimManuscriptVersion
import db_manager.dimManuscriptVersionHistory
import db_manager.dimCountry
import db_manager.dimPerson

import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-40s %(levelname)-8s %(message)s',
                    datefmt='%H:%M:%S',
                    filename='/tmp/dbmanager.log',
                    filemode='w')

LOGGER = logging.getLogger(__name__)

connect_str = "dbname={dbname} user={user} host={host} port={port} password={password}".format(
  host=os.environ.get('DATABASE_HOST', 'localhost'),
  port=os.environ.get('DATABASE_PORT', '5432'),
  dbname=os.environ.get('DATABASE_NAME', 'elife_etl'),
  user=os.environ.get('DATABASE_USER', 'elife_etl'),
  password=os.environ.get('DATABASE_PASSWORD', 'elife_etl')
)
conn = psycopg2.connect(connect_str)

sqlTearDown = open("sql_scripts/teardown.sql", "r").read()
sqlCreate   = open("sql_scripts/create.sql",   "r").read()

with conn.cursor() as c:
  c.execute(sqlTearDown)
  c.execute(sqlCreate)

conn.commit()

###
### Example staging csv's and applying changes
###

################################################################################
# A Manuscript is composed of it's versions and VersionHistory too.
#
# This means that those must all be staged before applying changes
# (which will be cascaded down to the child entities)
#
# Applying changes to a manuscript without loading the Version information is
# equivalent to explicitly stating that a manuscript has no Versions.
################################################################################

LOGGER.info("NEW FILES...")

db_manager.dimManuscript.stage_csv(
  conn,
  "dummy_csv/1526868166_manuscripts.csv"
)

db_manager.dimManuscriptVersion.stage_csv(
  conn,
  "dummy_csv/1526868166_versions.csv"
)

db_manager.dimManuscriptVersionHistory.stage_csv(
  conn,
  "dummy_csv/1526868166_stages.csv"
)

db_manager.dimManuscript.applyChanges(conn, None)



### relabel countries with different externalReferences, to have the same label

LOGGER.info("NEW FILES...")

db_manager.dimCountry.stage_csv(conn, "dummy_csv/1526868166_country_relabel.csv")

db_manager.dimCountry.applyChanges(conn, None)



### load persons

LOGGER.info("NEW FILES...")

db_manager.dimPerson.stage_csv(
    conn,
    "dummy_csv/1500000000_persons.csv"
)

db_manager.dimPersonRole.stage_csv(
    conn,
    "dummy_csv/1500000000_person_roles.csv"
)

db_manager.dimPerson.applyChanges(conn, None)



LOGGER.info("NEW FILES...")

db_manager.dimPerson.stage_csv(
    conn,
    "dummy_csv/1500000000_persons.csv"
)

db_manager.dimPersonRole.stage_csv(
    conn,
    "dummy_csv/1500000000_person_roles.csv"
)

db_manager.dimPerson.applyChanges(conn, None)



LOGGER.info("NEW FILES...")

db_manager.dimPerson.stage_csv(
    conn,
    "dummy_csv/1600000000_persons.csv"
)

db_manager.dimPersonRole.stage_csv(
    conn,
    "dummy_csv/1600000000_person_roles.csv"
)

db_manager.dimPerson.applyChanges(conn, None)
