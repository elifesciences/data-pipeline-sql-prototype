#!/usr/bin/python3

import psycopg2
import db_manager.dimManuscript 
import db_manager.dimCountry
import db_manager.dimPerson



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



### relabel countries with different externalReferences, to have the same label

db_manager.dimCountry.stage_csv(conn, "dummy_csv/1526868166_country_relabel.csv")

db_manager.dimCountry.applyChanges(conn)



### load persons

db_manager.dimPerson.stage_csv(
    conn,
    person                   = "dummy_csv/1500000000_persons.csv",
    person_role              = "dummy_csv/1500000000_persons_roles.csv"
)

db_manager.dimPerson.applyChanges(conn)


db_manager.dimPerson.stage_csv(
    conn,
    person                   = "dummy_csv/1600000000_persons.csv",
    person_role              = "dummy_csv/1600000000_persons_roles.csv"
)

db_manager.dimPerson.applyChanges(conn)
