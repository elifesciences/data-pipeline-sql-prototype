
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- CLEANUP
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS stg.dimManuscriptVersionStageHistory;
DROP TABLE IF EXISTS stg.dimManuscriptVersion;
DROP TABLE IF EXISTS stg.dimManuscript;
DROP TABLE IF EXISTS stg.dimPerson;
DROP TABLE IF EXISTS stg.dimStage;

DROP TABLE IF EXISTS dim.dimManuscriptVersionStageHistory;
DROP TABLE IF EXISTS dim.dimManuscriptVersion;
DROP TABLE IF EXISTS dim.dimManuscript;
DROP TABLE IF EXISTS dim.dimPerson;
DROP TABLE IF EXISTS dim.dimStage;

DROP SCHEMA IF EXISTS stg;
DROP SCHEMA IF EXISTS dim;



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- SCHEMA
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE SCHEMA stg;
CREATE SCHEMA dim;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- MANUSCRIPT
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE stg.dimManuscript (
    id                                        INT           DEFAULT(NULL),
    externalReference_Manuscript              VARCHAR(80)   NOT NULL,
    aDummyProperty                            VARCHAR(80)   DEFAULT('Unknown'),
    _staging_mode                             CHAR          DEFAULT('U')
);

CREATE TABLE dim.dimManuscript (
    id                                        INT           GENERATED ALWAYS AS IDENTITY,
    externalReference                         VARCHAR(80)   NOT NULL,
    aDummyProperty                            VARCHAR(80),
    PRIMARY KEY (id),
    UNIQUE(externalReference)
);



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- MANUSCRIPT VERSION
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE stg.dimManuscriptVersion (
    id                                        INT           DEFAULT(NULL),
    externalReference_Manuscript              VARCHAR(80)   NOT NULL,
    externalReference_ManuscriptVersion       VARCHAR(80)   NOT NULL,
    sequence_number                           INT           DEFAULT(NULL),
    aDummyProperty                            VARCHAR(80)   DEFAULT('Unknown'),
    _staging_mode                             CHAR          DEFAULT('U')
);

CREATE TABLE dim.dimManuscriptVersion (
    id                                        INT           GENERATED ALWAYS AS IDENTITY,
    manuscriptID                              INT           NOT NULL,
    externalReference                         VARCHAR(80)   NOT NULL,
    sequence_number                           INT           DEFAULT(NULL),
    aDummyProperty                            VARCHAR(80),
    PRIMARY KEY (id),
    UNIQUE (manuscriptID, externalReference)
);



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- MANUSCRIPT VERSION STAGE_HISTORY
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE stg.dimManuscriptVersionStageHistory (
    id                                        INT           DEFAULT(NULL),
    externalReference_Manuscript              VARCHAR(80)   NOT NULL,
    externalReference_ManuscriptVersion       VARCHAR(80)   NOT NULL,
    externalReference_ManuscriptVersionStage  VARCHAR(80)   NOT NULL,
    sequence_number                           INT           DEFAULT(NULL),
    externalReference_Stage                   VARCHAR(80)   NOT NULL,
    externalReference_Person_Affective        VARCHAR(80)   NOT NULL,
    externalReference_Person_TriggeredBy      VARCHAR(80)   NOT NULL,
    epoch_startDate                           INT           DEFAULT(-1),
    _staging_mode                             CHAR          DEFAULT('U')
);

CREATE TABLE dim.dimManuscriptVersionStageHistory (
    id                                        INT           GENERATED ALWAYS AS IDENTITY,
    manuscriptVersionID                       INT           NOT NULL,
    externalReference                         VARCHAR(80)   NOT NULL,
    sequence_number                           INT           DEFAULT(NULL),
    stageID                                   INT           NOT NULL,
    personID_Affective                        INT           NOT NULL,
    personID_TriggeredBy                      INT           NOT NULL,
    epoch_startDate                           INT           NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (manuscriptVersionID, externalReference)
);



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Stage
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE stg.dimStage (
    id                                        INT           DEFAULT(NULL),
    externalReference_Stage                   VARCHAR(80)   NOT NULL,
    stageLabel                                VARCHAR(80)   DEFAULT(NULL),
    _staging_mode                             CHAR          DEFAULT('U')
);

CREATE TABLE dim.dimStage (
    id                                        INT           GENERATED ALWAYS AS IDENTITY,
    externalReference                         VARCHAR(80)   NOT NULL,
    stageLabel                                VARCHAR(80)   NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (externalReference)
);



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Person
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE stg.dimPerson (
    id                                        INT           DEFAULT(NULL),
    externalReference_Person                  VARCHAR(80)   NOT NULL,
    name_full                                 VARCHAR(80)   DEFAULT(NULL),
    _staging_mode                             CHAR          DEFAULT('U')
);

CREATE TABLE dim.dimPerson (
    id                                        INT           GENERATED ALWAYS AS IDENTITY,
    externalReference                         VARCHAR(80)   NOT NULL,
    name_full                                 VARCHAR(80),
    PRIMARY KEY (id),
    UNIQUE (externalReference)
);



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- STAGE DUMMY DATA
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
INSERT INTO stg.dimManuscript
  (externalReference_Manuscript)
VALUES
  ('dd-mm-yyyy-type-eLife-12345'),
  ('dd-mm-yyyy-type-eLife-67890')
;

INSERT INTO stg.dimManuscriptVersion
  (externalReference_Manuscript, externalReference_ManuscriptVersion, sequence_number)
VALUES
  ('dd-mm-yyyy-type-eLife-12345', '0', 1),
  ('dd-mm-yyyy-type-eLife-12345', '1', 2),
  ('dd-mm-yyyy-type-eLife-67890', '0', 1),
  ('dd-mm-yyyy-type-eLife-67890', '1', 2),
  ('dd-mm-yyyy-type-eLife-67890', '2', 3)
;

INSERT INTO stg.dimManuscriptVersionStageHistory
  (
    externalReference_Manuscript,
    externalReference_ManuscriptVersion,
    externalReference_ManuscriptVersionStage,
    sequence_number,
    externalReference_Stage,
    externalReference_Person_Affective,
    externalReference_Person_TriggeredBy,
    epoch_startDate
  )
VALUES
  ('dd-mm-yyyy-type-eLife-12345', '0', '1', 1, 'xxx', '12345', '99999', 123456),
  ('dd-mm-yyyy-type-eLife-12345', '0', '2', 2, 'yyy', '23456', '12345', 234567),
  ('dd-mm-yyyy-type-eLife-12345', '0', '3', 3, 'zzz', '12345', '23456', 345678),

  ('dd-mm-yyyy-type-eLife-12345', '1', '1', 1, 'xxx', '23456', '99999', 123456),
  ('dd-mm-yyyy-type-eLife-12345', '1', '2', 2, 'zzz', '34567', '23456', 234567),
  
  ('dd-mm-yyyy-type-eLife-67890', '0', '1', 1, 'yyy', '34567', '99999', 123456),

  ('dd-mm-yyyy-type-eLife-67890', '1', '1', 1, 'yyy', '12345', '99999', 123456),
  ('dd-mm-yyyy-type-eLife-67890', '1', '2', 2, 'zzz', '12345', '12345', 234567),

  ('dd-mm-yyyy-type-eLife-67890', '2', '1', 1, 'aaa', '12345', '99999', 123456),
  ('dd-mm-yyyy-type-eLife-67890', '2', '2', 2, 'bbb', '34567', '12345', 234567),
  ('dd-mm-yyyy-type-eLife-67890', '2', '3', 3, 'ccc', '56789', '34567', 345678)
;





--------------------------------------------------------------------------------
-- ManuscriptVersionStageHistory_Manager
--    -> Prepare()
--         -> PushInitialisationOfUnseenPersons()
--         -> PushInitialisationOfUnseenStages()
--         -> PushInitialisationOfUnseenManuscriptVersions()
--         -> ResolveFKs()
--         -> PullDeletes()
--------------------------------------------------------------------------------

INSERT INTO
  stg.dimPerson
  (
    externalReference_Person,
    name_full,
    _staging_mode
  )
SELECT DISTINCT
  externalReference_Person,
  'Unknown (' || externalReference_Person || ')',
  'I'
FROM
(
  SELECT externalReference_Person_Affective   FROM stg.dimManuscriptVersionStageHistory
  UNION
  SELECT externalReference_Person_TriggeredBY FROM stg.dimManuscriptVersionStageHistory
)
  s(externalReference_Person)
WHERE
  NOT EXISTS (
    SELECT *
      FROM stg.dimPerson
     WHERE externalReference_Person = s.externalReference_Person
  )
;

INSERT INTO
  stg.dimStage
  (
    externalReference_Stage,
    stageLabel,
    _staging_mode
  )
SELECT DISTINCT
  externalReference_Stage,
  'Unknown (' || externalReference_Stage || ')',
  'I'
FROM
  stg.dimManuscriptVersionStageHistory   s
WHERE
  NOT EXISTS (
    SELECT *
      FROM stg.dimStage
     WHERE externalReference_Stage = s.externalReference_Stage
  )
;

INSERT INTO
  stg.dimManuscriptVersion
  (
    externalReference_Manuscript,
    externalReference_ManuscriptVersion,
    _staging_mode
  )
SELECT DISTINCT
  externalReference_Manuscript,
  externalReference_ManuscriptVersion,
  'I'
FROM
  stg.dimManuscriptVersionStageHistory   s
WHERE
  NOT EXISTS (
    SELECT *
      FROM stg.dimManuscriptVersion
     WHERE externalReference_Manuscript        = s.externalReference_Manuscript
       AND externalReference_ManuscriptVersion = s.externalReference_ManuscriptVersion
  )
;

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
  dmvsh.id,
  dmv.externalReference_Manuscript,
  dmv.externalReference_ManuscriptVersion,
  dmvsh.externalReference,
  'D'
FROM
(
  SELECT DISTINCT id, externalReference_Manuscript, externalReference_ManuscriptVersion
    FROM stg.dimManuscriptVersion
   WHERE _staging_mode <> 'I' 
     AND id IS NOT NULL
)
  dmv
INNER JOIN
  dim.dimManuscriptVersionStageHistory   dmvsh
    ON  dmvsh.manuscriptVersionID = dmv.id
WHERE
  NOT EXISTS (
    SELECT *
      FROM stg.dimManuscriptVersionStageHistory
     WHERE externalReference_Manuscript             = dmv.externalReference_Manuscript
	   AND externalReference_ManuscriptVersion      = dmv.externalReference_ManuscriptVersion
       AND externalReference_ManuscriptVersionStage = dmvsh.externalReference
  )
;




--------------------------------------------------------------------------------
-- ManuscriptVersion_Manager
--    -> Prepare()
--         -> PushInitialisationOfUnseenManuscripts()
--         -> ResolveFKs()
--         -> PullDeletes()
--------------------------------------------------------------------------------

INSERT INTO
  stg.dimManuscript
  (
    externalReference_Manuscript,
    _staging_mode
  )
SELECT DISTINCT
  externalReference_Manuscript,
  'I'
FROM
  stg.dimManuscriptVersion   s
WHERE
  NOT EXISTS (
    SELECT *
      FROM stg.dimManuscript
     WHERE externalReference_Manuscript = s.externalReference_Manuscript
  )
;

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
WHERE
  NOT EXISTS (
    SELECT *
      FROM stg.dimManuscriptVersion
     WHERE externalReference_Manuscript        = dm.externalReference
	   AND externalReference_ManuscriptVersion = dmv.externalReference
  )
;



--------------------------------------------------------------------------------
-- Manuscript_Manager
--    -> Prepare()
--         -> ResolveFKs()
--------------------------------------------------------------------------------

UPDATE
  stg.dimManuscript   s
SET
  id = dm.id
FROM
  dim.dimManuscript   dm
WHERE
  dm.externalReference = s.externalReference_Manuscript
;



--------------------------------------------------------------------------------
-- Manuscript_Manager
--    -> ApplyChanges()
--------------------------------------------------------------------------------

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
      aDummyProperty
    )
SELECT DISTINCT
  s.externalReference_Manuscript,
  s.aDummyProperty
FROM
  stg.dimManuscript   s
WHERE
      (s._staging_mode = 'I' AND s.id IS NULL)
  OR  (s._staging_mode = 'U'                 )
ON CONFLICT
  (externalReference)
    DO UPDATE
      SET aDummyProperty = EXCLUDED.aDummyProperty
;



--------------------------------------------------------------------------------
-- ManuscriptVersion_Manager
--    -> ApplyChanges()
--------------------------------------------------------------------------------

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
      sequence_number,
      aDummyProperty
    )
SELECT DISTINCT
  m.id,
  s.externalReference_ManuscriptVersion,
  s.sequence_number,
  s.aDummyProperty
FROM
  stg.dimManuscriptVersion   s
LEFT JOIN
  dim.dimManuscript          m
    ON  m.externalReference = s.externalReference_Manuscript
ON CONFLICT
  (manuscriptID, externalReference)
    DO UPDATE
      SET sequence_number = EXCLUDED.sequence_number,
          aDummyProperty  = EXCLUDED.aDummyProperty
;



--------------------------------------------------------------------------------
-- Stage_Manager
--    -> ApplyChanges()
--------------------------------------------------------------------------------

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
SELECT DISTINCT
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



--------------------------------------------------------------------------------
-- Stage_Manager
--    -> ApplyChanges()
--------------------------------------------------------------------------------

DELETE FROM
  dim.dimPerson   d
USING
  stg.dimPerson   s
WHERE
      s.id            = d.id
  AND s._staging_mode = 'D'
;

INSERT INTO
  dim.dimPerson   AS d
    (
      externalReference,
      name_full
    )
SELECT DISTINCT
  s.externalReference_Person,
  s.name_full
FROM
  stg.dimPerson   s
WHERE
      (s._staging_mode = 'I' AND s.id IS NULL)
  OR  (s._staging_mode = 'U'                 )
ON CONFLICT
  (externalReference)
    DO UPDATE
      SET name_full = EXCLUDED.name_full
;



--------------------------------------------------------------------------------
-- ManuscriptVersionStageHistory_Manager
--    -> ApplyChanges()
--------------------------------------------------------------------------------

DELETE FROM
  dim.dimManuscriptVersionStageHistory   d
USING
  stg.dimManuscriptVersionStageHistory   s
WHERE
      s.id            = d.id
  AND s._staging_mode = 'D'
;

INSERT INTO
  dim.dimManuscriptVersionStageHistory   AS d
    (
      manuscriptVersionID,
      externalReference,
      sequence_number,
      stageID,
      personID_affective,
      personID_triggeredBy,
      epoch_startDate
    )
SELECT DISTINCT
  dmv.id,
  s.externalReference_ManuscriptVersionStage,
  s.sequence_number,
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
      SET sequence_number       = EXCLUDED.sequence_number,
          stageID               = EXCLUDED.stageID,
          personID_affective    = EXCLUDED.personID_affective,
          personID_triggeredBy  = EXCLUDED.personID_triggeredBy,
          epoch_startDate       = EXCLUDED.epoch_startDate
;
