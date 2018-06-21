#!/usr/bin/python3

import psycopg2
import db_manager.dimManuscript 

connect_str = "dbname=elife_ejp user=elife_etl host=localhost port=5432 password=elife_etl"
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