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
    zip_name                                  VARCHAR(255)      NULL,
    externalReference_Manuscript              VARCHAR(255)  NOT NULL,
    msid                                      VARCHAR(255)      NULL,
    externalReference_Country                 VARCHAR(255)  DEFAULT('Unknown'),
    doi                                       VARCHAR(255)  DEFAULT(''),
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Manuscript)
);

CREATE TABLE dim.dimManuscript (
    id                                        SERIAL,
    externalReference                         VARCHAR(255)  NOT NULL,
    msid                                      VARCHAR(255)      NULL,
    country_id                                INT           NOT NULL,
    doi                                       VARCHAR(255)  NOT NULL,
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
    zip_name                                  VARCHAR(255)      NULL,
    externalReference_Manuscript              VARCHAR(255)  NOT NULL,
    externalReference_ManuscriptVersion       INT           NOT NULL,
    decision                                  TEXT          DEFAULT('<None Specified>'),
    ms_type                                   VARCHAR(255)      NULL,
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Manuscript, externalReference_ManuscriptVersion)
);

CREATE TABLE dim.dimManuscriptVersion (
    id                                        SERIAL,
    manuscriptID                              INT           NOT NULL,
    externalReference                         INT           NOT NULL,
    decision                                  TEXT          NOT NULL,
    ms_type                                   VARCHAR(255)      NULL,
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
    zip_name                                  VARCHAR(255)      NULL,
    externalReference_Manuscript              VARCHAR(255)  NOT NULL,
    externalReference_ManuscriptVersion       INT           NOT NULL,
    externalReference_ManuscriptVersionStage  INT           NOT NULL,
    externalReference_Stage                   VARCHAR(255)  NOT NULL,
    externalReference_Person_Affective        VARCHAR(255)  NOT NULL,
    externalReference_Person_TriggeredBy      VARCHAR(255)  NOT NULL,
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
    externalReference_Stage                   VARCHAR(255)  NOT NULL,
    stageLabel                                VARCHAR(255)  DEFAULT(NULL),
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Stage)
);

CREATE TABLE dim.dimStage (
    id                                        SERIAL,
    externalReference                         VARCHAR(255)  NOT NULL,
    stageLabel                                VARCHAR(255)  NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (externalReference)
);



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Person
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE stg.dimPerson (
    id                                        INT,
    source_file_name                          VARCHAR(255)  NOT NULL,
    source_file_creation                      INT           NOT NULL,
    externalReference_Person                  VARCHAR(255)  NOT NULL,
    status                                    VARCHAR(255)      NULL,
    first_name                                VARCHAR(255)      NULL,
    middle_name                               VARCHAR(255)      NULL,
    last_name                                 VARCHAR(255)      NULL,
    profile_modify_date                       INT           DEFAULT(0),
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(source_file_name, source_file_creation, externalReference_Person)
);

CREATE TABLE dim.dimPerson (
    id                                        SERIAL,
    externalReference                         VARCHAR(255)  NOT NULL,
    status                                    VARCHAR(255)      NULL,
    first_name                                VARCHAR(255)      NULL,
    middle_name                               VARCHAR(255)      NULL,
    last_name                                 VARCHAR(255)      NULL,
    profile_modify_date                       INT,
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
    externalReference_Country                 VARCHAR(255)  NOT NULL,
    countryLabel                              VARCHAR(255)  DEFAULT(NULL),
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Country)
);

CREATE TABLE dim.dimCountry (
    id                                        SERIAL,
    externalReference                         VARCHAR(255)  NOT NULL,
    countryLabel                              VARCHAR(255),
    PRIMARY KEY (id),
    UNIQUE (externalReference)
);


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Role
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE stg.dimRole (
    id                                        INT           DEFAULT(NULL),
    externalReference_Role                    VARCHAR(255)  NOT NULL,
    roleLabel                                 VARCHAR(255)  DEFAULT(NULL),
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE(externalReference_Role)
);

CREATE TABLE dim.dimRole (
    id                                        SERIAL,
    externalReference                         VARCHAR(255)  NOT NULL,
    roleLabel                                 VARCHAR(255),
    PRIMARY KEY (id),
    UNIQUE (externalReference)
);


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- PersonRole
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE stg.dimPersonRole (
    id                                        SERIAL,
	source_file_name                          VARCHAR(255)  NOT NULL,
	source_file_creation                      INT           NOT NULL,
    externalreference_person                  VARCHAR(255)  NOT NULL,
    externalreference_role                    VARCHAR(255)       NULL,
	effective_from                            INT           NOT NULL,
	effective_stop                            INT               NULL,
	current_effective_from                    INT               NULL,
	current_effective_stop                    INT               NULL,
	personID                                  INT               NULL,
	roleID                                    INT               NULL,
	is_active                                 BOOL          DEFAULT(TRUE),
    _staging_mode                             CHAR          DEFAULT('U'),
    UNIQUE (
		externalReference_person,
		externalReference_role,
		source_file_name,
		source_file_creation,
		effective_from
	)
);

CREATE TABLE dim.dimPersonRole (
    id                                        SERIAL,
	personID                                  INT           NOT NULL,
	roleID                                    INT           NOT NULL,
	effective_from                            INT           NOT NULL,
	effective_stop                            INT           NOT NULL,
	is_active                                 BOOL          NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (
		personID,
		roleID,
		effective_from
	)
);

