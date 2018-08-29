from db_manager.dimCountry import (
  stage_iterable,
  applyChanges
)

from .database_utils import fetch_table_as_dict


COUNTRY_CODE_1 = 'C1'
COUNTRY_LABEL_1 = 'Country 1'
COUNTRY_LABEL_2 = 'Country 2'


STAGING_COUNTRY_1 = {
    'externalReference_Country': COUNTRY_CODE_1,
    'countryLabel': COUNTRY_LABEL_1
}


STAGED_COUNTRY_1 = STAGING_COUNTRY_1


DIM_COUNTRY_1 = {
    'externalReference': COUNTRY_CODE_1,
    'countryLabel': COUNTRY_LABEL_1
}


class TestDimCountry:
    class TestStageIterable:
        def test_should_not_fail_with_empty_list(self, empty_test_database):
            stage_iterable(empty_test_database, [])
            result = fetch_table_as_dict(
                empty_test_database,
                'stg.dimCountry'
            )
            assert result == []


        def test_should_stage_one_country(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_COUNTRY_1])
            result = fetch_table_as_dict(
                empty_test_database,
                'stg.dimCountry',
                sorted(STAGED_COUNTRY_1.keys())
            )
            assert result == [STAGED_COUNTRY_1]


    class TestApplyChanges:
        def test_should_apply_one_country(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_COUNTRY_1])
            applyChanges(empty_test_database, None)
            result = fetch_table_as_dict(
                empty_test_database,
                'dim.dimCountry',
                sorted(DIM_COUNTRY_1.keys())
            )
            assert result == [DIM_COUNTRY_1]


        def test_should_update_country_label(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_COUNTRY_1])
            applyChanges(empty_test_database, None)

            stage_iterable(empty_test_database, [{
                **STAGING_COUNTRY_1,
                'countryLabel': COUNTRY_LABEL_2
            }])
            applyChanges(empty_test_database, None)

            result = fetch_table_as_dict(
                empty_test_database,
                'dim.dimCountry',
                sorted(DIM_COUNTRY_1.keys())
            )
            assert result == [{
                **DIM_COUNTRY_1,
                'countryLabel': COUNTRY_LABEL_2
            }]
