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
    create_date                               INT               NULL,
    zip_name                                  VARCHAR(80)       NULL,
    externalReference_Manuscript              VARCHAR(80)   NOT NULL,
    msid                                      INT               NULL,
    externalReference_Country                 VARCHAR(80)   DEFAULT('Unknown'),
    doi                                       VARCHAR(80)   DEFAULT(''),
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Manuscript)
);

CREATE TABLE dim.dimManuscript (
    id                                        SERIAL,
    externalReference                         VARCHAR(80)   NOT NULL,
    msid                                      INT               NULL,
    country_id                                INT           NOT NULL,
    doi                                       VARCHAR(80)   NOT NULL,
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
    create_date                               INT               NULL,
    zip_name                                  VARCHAR(80)       NULL,
    externalReference_Manuscript              VARCHAR(80)   NOT NULL,
    externalReference_ManuscriptVersion       INT           NOT NULL,
    decision                                  TEXT          DEFAULT('<None Specified>'),
    ms_type                                   VARCHAR(80)       NULL,
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Manuscript, externalReference_ManuscriptVersion)
);

CREATE TABLE dim.dimManuscriptVersion (
    id                                        SERIAL,
    manuscriptID                              INT           NOT NULL,
    externalReference                         INT           NOT NULL,
    decision                                  TEXT          NOT NULL,
    ms_type                                   VARCHAR(80)       NULL,
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
    create_date                               INT               NULL,
    zip_name                                  VARCHAR(80)       NULL,
    externalReference_Manuscript              VARCHAR(80)   NOT NULL,
    externalReference_ManuscriptVersion       INT           NOT NULL,
    externalReference_ManuscriptVersionStage  INT           NOT NULL,
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
    externalReference                         INT           NOT NULL,
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



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Country
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE stg.dimCountry (
    id                                        INT           DEFAULT(NULL),
    externalReference_Country                 VARCHAR(80)   NOT NULL,
    countryLabel                              VARCHAR(80)   DEFAULT(NULL),
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Country)
);

CREATE TABLE dim.dimCountry (
    id                                        SERIAL,
    externalReference                         VARCHAR(80)   NOT NULL,
    countryLabel                              VARCHAR(80),
    PRIMARY KEY (id),
    UNIQUE (externalReference)
);

