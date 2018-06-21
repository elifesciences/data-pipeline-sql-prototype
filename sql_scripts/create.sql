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
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Manuscript)
);

CREATE TABLE dim.dimManuscript (
    id                                        SERIAL,
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
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Manuscript, externalReference_ManuscriptVersion)
);

CREATE TABLE dim.dimManuscriptVersion (
    id                                        SERIAL,
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
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Manuscript, externalReference_ManuscriptVersion, externalReference_ManuscriptVersionStage)
);

CREATE TABLE dim.dimManuscriptVersionStageHistory (
    id                                        SERIAL,
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
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Stage)
);

CREATE TABLE dim.dimStage (
    id                                        SERIAL,
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
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Person)
);

CREATE TABLE dim.dimPerson (
    id                                        SERIAL,
    externalReference                         VARCHAR(80)   NOT NULL,
    name_full                                 VARCHAR(80),
    PRIMARY KEY (id),
    UNIQUE (externalReference)
);

