#!/usr/bin/python3

import os

import psycopg2
import db_manager.dimManuscript 
import db_manager.dimCountry

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


### Example staging csv's and applying changes

db_manager.dimManuscript.stage_csv(
    conn,
    manuscript               = "dummy_csv/1526868166_manuscripts.csv",
    manuscriptVersion        = "dummy_csv/1526868166_versions.csv",
    manuscriptVersionHistory = "dummy_csv/1526868166_stages.csv"
)

db_manager.dimManuscript.applyChanges(conn)


db_manager.dimCountry.stage_csv(conn, "dummy_csv/1526868166_country_relabel.csv")

db_manager.dimCountry.applyChanges(conn)

