from db_manager.dimManuscriptVersionHistory import (
  stage_iterable,
  applyChanges
)

from .database_utils import fetch_table_as_dict, fetch_table_column, fetch_table_as_map


DATE_1 = 123
ZIP_NAME_1 = 'test.zip'
XML_NAME_1 = 'test.xml'
VERSION_POS_1 = 1
STAGING_POS_1 = 1
STAGE_NAME_1 = 'Stage 1'
STAGE_NAME_2 = 'Stage 2'
PERSON_ID_1 = '11111'
PERSON_ID_2 = '22222'
DECISION_1 = 'Decision 1'
DECISION_2 = 'Decision 2'
MANUSCRIPT_TYPE_1 = 'Research Article'


STAGING_MANUSCRIPT_STAGE_1 = {
    'create_date': str(DATE_1),
    'source_file_name': XML_NAME_1,
    'version_position_in_ms': str(VERSION_POS_1),
    'stage_position_in_version': str(STAGING_POS_1),
    'name': STAGE_NAME_1,
    'affective_person_id': PERSON_ID_1,
    'triggered_by_person': PERSON_ID_1,
    'start_date': str(DATE_1)
}


STAGED_MANUSCRIPT_STAGE_1 = {
    'create_date': DATE_1,
    'externalReference_Manuscript': XML_NAME_1,
    'externalReference_ManuscriptVersion': VERSION_POS_1,
    'externalReference_ManuscriptVersionStage': STAGING_POS_1,
    'externalReference_Stage': STAGE_NAME_1,
    'externalReference_Person_Affective': PERSON_ID_1,
    'externalReference_Person_TriggeredBy': PERSON_ID_1,
    'epoch_startDate': DATE_1
}


DIM_MANUSCRIPT_STAGE_EX_IDS_1 = {
    'externalReference': STAGING_POS_1, # is that correct?
    'epoch_startDate': DATE_1
}


def fetch_stage_id_by_name(connection) -> dict:
    return fetch_table_as_map(
        connection,
        'dim.dimStage',
        key_column='externalReference',
        value_column='id'
    )


def fetch_person_id_by_external_ref(connection) -> dict:
    return fetch_table_as_map(
        connection,
        'dim.dimPerson',
        key_column='externalReference',
        value_column='id'
    )



class TestDimManuscriptVersionHistory:
    class TestStageIterable:
        def test_should_not_fail_with_empty_list(self, empty_test_database):
            stage_iterable(empty_test_database, [])
            result = fetch_table_as_dict(
                empty_test_database,
                'stg.dimManuscriptVersionStageHistory'
            )
            assert result == []


        def test_should_stage_one_stage_entry(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_STAGE_1])
            result = fetch_table_as_dict(
                empty_test_database,
                'stg.dimManuscriptVersionStageHistory',
                sorted(STAGED_MANUSCRIPT_STAGE_1.keys())
            )
            assert result == [STAGED_MANUSCRIPT_STAGE_1]


    class TestApplyChanges:
        def test_should_apply_one_manuscript_version(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_STAGE_1])
            applyChanges(empty_test_database, None)
            result = fetch_table_as_dict(
                empty_test_database,
                'dim.dimManuscriptVersionStageHistory',
                sorted(DIM_MANUSCRIPT_STAGE_EX_IDS_1.keys())
            )
            assert result == [DIM_MANUSCRIPT_STAGE_EX_IDS_1]


        def test_should_create_manuscript(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_STAGE_1])
            applyChanges(empty_test_database, None)
            result = fetch_table_as_dict(
                empty_test_database,
                'dim.dimManuscript',
                ['externalReference']
            )
            assert result == [{
                'externalReference': XML_NAME_1
            }]


        def test_should_create_manuscript_version(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_STAGE_1])
            applyChanges(empty_test_database, None)
            result = fetch_table_as_dict(
                empty_test_database,
                'dim.dimManuscriptVersion',
                ['externalReference']
            )
            assert result == [{
                'externalReference': VERSION_POS_1
            }]


        def test_should_create_stage_and_resolve_id(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_STAGE_1])
            applyChanges(empty_test_database, None)

            stage_id_by_name = fetch_stage_id_by_name(empty_test_database)
            assert stage_id_by_name.keys() == {STAGE_NAME_1}

            manuscript_stage_ids = fetch_table_column(
                empty_test_database,
                'dim.dimManuscriptVersionStageHistory',
                'stageID'
            )
            assert manuscript_stage_ids == [stage_id_by_name[STAGE_NAME_1]]


        def test_should_create_persons_and_resolve_id(self, empty_test_database):
            stage_iterable(empty_test_database, [{
                **STAGING_MANUSCRIPT_STAGE_1,
                'affective_person_id': PERSON_ID_1,
                'triggered_by_person': PERSON_ID_2
            }])
            applyChanges(empty_test_database, None)

            person_id_by_external_ref = fetch_person_id_by_external_ref(empty_test_database)
            assert person_id_by_external_ref.keys() == {PERSON_ID_1, PERSON_ID_2}

            stage_person_ids = fetch_table_as_dict(
                empty_test_database,
                'dim.dimManuscriptVersionStageHistory',
                ['personID_affective', 'personID_triggeredBy']
            )
            assert [
                (x['personID_affective'], x['personID_triggeredBy'])
                for x in stage_person_ids
            ] == [(
                person_id_by_external_ref[PERSON_ID_1],
                person_id_by_external_ref[PERSON_ID_2]
            )]


        def test_should_update_stage_id(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_STAGE_1])
            applyChanges(empty_test_database, None)

            stage_iterable(empty_test_database, [{
                **STAGING_MANUSCRIPT_STAGE_1,
                'name': STAGE_NAME_2
            }])
            applyChanges(empty_test_database, None)

            stage_id_by_name = fetch_stage_id_by_name(empty_test_database)
            manuscript_stage_ids = fetch_table_column(
                empty_test_database,
                'dim.dimManuscriptVersionStageHistory',
                'stageID'
            )
            assert manuscript_stage_ids == [stage_id_by_name[STAGE_NAME_2]]


        def test_should_update_person_ids(self, empty_test_database):
            stage_iterable(empty_test_database, [{
                **STAGING_MANUSCRIPT_STAGE_1,
                'affective_person_id': PERSON_ID_1,
                'triggered_by_person': PERSON_ID_2
            }])
            applyChanges(empty_test_database, None)

            stage_iterable(empty_test_database, [{
                **STAGING_MANUSCRIPT_STAGE_1,
                'affective_person_id': PERSON_ID_2,
                'triggered_by_person': PERSON_ID_1
            }])
            applyChanges(empty_test_database, None)

            person_id_by_external_ref = fetch_person_id_by_external_ref(empty_test_database)
            stage_person_ids = fetch_table_as_dict(
                empty_test_database,
                'dim.dimManuscriptVersionStageHistory',
                ['personID_affective', 'personID_triggeredBy']
            )
            assert [
                (x['personID_affective'], x['personID_triggeredBy'])
                for x in stage_person_ids
            ] == [(
                person_id_by_external_ref[PERSON_ID_2],
                person_id_by_external_ref[PERSON_ID_1]
            )]
