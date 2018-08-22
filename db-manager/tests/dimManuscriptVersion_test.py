from db_manager.dimManuscriptVersion import (
  stage_iterable,
  applyChanges
)

from .database_utils import fetch_table_as_dict


DATE_1 = 123
ZIP_NAME_1 = 'test.zip'
XML_NAME_1 = 'test.xml'
VERSION_POS_1 = 1
DECISION_1 = 'Decision 1'
DECISION_2 = 'Decision 2'
MANUSCRIPT_TYPE_1 = 'Research Article'


STAGING_MANUSCRIPT_VERSION_1 = {
    'create_date': str(DATE_1),
    'zip_name': ZIP_NAME_1,
    'xml_file_name': XML_NAME_1,
    'version_position_in_ms': VERSION_POS_1,
    'decision': DECISION_1,
    'ms_type': MANUSCRIPT_TYPE_1
}


STAGED_MANUSCRIPT_VERSION_1 = {
    'create_date': DATE_1,
    'zip_name': ZIP_NAME_1,
    'externalReference_Manuscript': XML_NAME_1,
    'externalReference_ManuscriptVersion': VERSION_POS_1,
    'decision': DECISION_1,
    'ms_type': MANUSCRIPT_TYPE_1
}


DIM_MANUSCRIPT_VERSION_1 = {
    'externalReference': VERSION_POS_1, # is that correct?
    'decision': DECISION_1,
    'ms_type': MANUSCRIPT_TYPE_1
}


class TestDimManuscriptVersion:
    class TestStageIterable:
        def test_should_not_fail_with_empty_list(self, empty_test_database):
            stage_iterable(empty_test_database, [])
            result = fetch_table_as_dict(
                empty_test_database,
                'stg.dimManuscriptVersion'
            )
            assert result == []


        def test_should_stage_one_manuscript_version(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_VERSION_1])
            result = fetch_table_as_dict(
                empty_test_database,
                'stg.dimManuscriptVersion',
                sorted(STAGED_MANUSCRIPT_VERSION_1.keys())
            )
            assert result == [STAGED_MANUSCRIPT_VERSION_1]


    class TestApplyChanges:
        def test_should_apply_one_manuscript_version(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_VERSION_1])
            applyChanges(empty_test_database, None)
            result = fetch_table_as_dict(
                empty_test_database,
                'dim.dimManuscriptVersion',
                sorted(DIM_MANUSCRIPT_VERSION_1.keys())
            )
            assert result == [DIM_MANUSCRIPT_VERSION_1]


        def test_should_create_manuscript(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_VERSION_1])
            applyChanges(empty_test_database, None)
            result = fetch_table_as_dict(
                empty_test_database,
                'dim.dimManuscript',
                ['externalReference']
            )
            assert result == [{
                'externalReference': XML_NAME_1
            }]


        def test_should_update_manuscript_decision(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_MANUSCRIPT_VERSION_1])
            applyChanges(empty_test_database, None)

            stage_iterable(empty_test_database, [{
                **STAGING_MANUSCRIPT_VERSION_1,
                'decision': DECISION_2
            }])
            applyChanges(empty_test_database, None)

            result = fetch_table_as_dict(
                empty_test_database,
                'dim.dimManuscriptVersion',
                sorted(DIM_MANUSCRIPT_VERSION_1.keys())
            )
            assert result == [{
                **DIM_MANUSCRIPT_VERSION_1,
                'decision': DECISION_2
            }]
