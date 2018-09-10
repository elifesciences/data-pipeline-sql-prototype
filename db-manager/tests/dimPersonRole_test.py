import logging
from typing import Dict, List

from db_manager.dimPersonRole import (
  stage_iterable,
  applyChanges
)

from .database_utils import fetch_table_column, fetch_table_as_dict

from .test_utils import (
    reverse_dict,
    fetch_person_id_by_external_ref,
    fetch_person_role_id_by_external_ref
)


LOGGER = logging.getLogger(__name__)

CREATED_DATE_1 = 123
MODIFIED_DATE_1 = 222
MODIFIED_DATE_2 = 223
XML_NAME_1 = 'test1.xml'
XML_NAME_2 = 'test2.xml'
PERSON_ID_1 = 'person1'
ROLE_1 = 'role1'
ROLE_2 = 'role2'


STAGING_PERSON_ROLE_1 = {
    'create_date': str(CREATED_DATE_1),
    'source_file_name': XML_NAME_1,
    'person_id': PERSON_ID_1,
    'role': ROLE_1,
    'profile_modify_date': str(MODIFIED_DATE_1)
}


STAGED_PERSON_ROLE_1 = {
    'source_file_creation': CREATED_DATE_1,
    'source_file_name': XML_NAME_1,
    'externalReference_Person': PERSON_ID_1,
    'externalReference_Role': ROLE_1,
    'effective_from': MODIFIED_DATE_1
}


DIM_PERSON_ROLE_EXCL_IDS_1 = {
    'effective_from': MODIFIED_DATE_1,
    # 'effective_stop': None,
    'is_active': True
}


DIM_PERSON_ROLE_1 = {
    **DIM_PERSON_ROLE_EXCL_IDS_1,
    'PersonID': PERSON_ID_1,
    'RoleID': ROLE_1
}


def map_person_id(
    dim_person_role: dict,
    person_id_map: Dict[str, str]) -> dict:

    return {
        **dim_person_role,
        'PersonID': person_id_map[dim_person_role['PersonID']]
    }


def map_person_ids(
    dim_person_roles: List[dict],
    person_id_map: Dict[str, str]) -> List[dict]:

    return [
        map_person_id(dim_person_role, person_id_map)
        for dim_person_role in dim_person_roles
    ]


def map_person_role_id(
    dim_person_role: dict,
    person_role_id_map: Dict[str, str]) -> dict:

    return {
        **dim_person_role,
        'RoleID': person_role_id_map[dim_person_role['RoleID']]
    }


def map_person_role_ids(
    dim_person_roles: List[dict],
    person_role_id_map: Dict[str, str]) -> List[dict]:

    return [
        map_person_role_id(dim_person_role, person_role_id_map)
        for dim_person_role in dim_person_roles
    ]


def filter_active(dim_person_roles: List[dict]) -> List[dict]:
    return [
        dim_person_role
        for dim_person_role in dim_person_roles
        if dim_person_role['is_active']
    ]


def fetch_person_role_with_reversed_ids(connection, keys) -> dict:
    person_id_map = fetch_person_id_by_external_ref(connection)
    rev_person_id_map = reverse_dict(person_id_map)

    person_role_id_map = fetch_person_role_id_by_external_ref(connection)
    rev_person_role_id_map = reverse_dict(person_role_id_map)

    return map_person_role_ids(
        map_person_ids(fetch_table_as_dict(
            connection,
            'dim.dimPersonRole',
            keys
        ), rev_person_id_map),
        rev_person_role_id_map
    )


class TestDimPersonRole:
    class TestStageIterable:
        def test_should_not_fail_with_empty_list(self, empty_test_database):
            stage_iterable(empty_test_database, [])
            result = fetch_table_as_dict(
                empty_test_database,
                'stg.dimPersonRole'
            )
            assert result == []


        def test_should_stage_one_person_role(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_PERSON_ROLE_1])
            result = fetch_table_as_dict(
                empty_test_database,
                'stg.dimPersonRole',
                sorted(STAGED_PERSON_ROLE_1.keys())
            )
            assert result == [STAGED_PERSON_ROLE_1]


        def test_should_ignore_duplicate_person_roles(self, empty_test_database):
            stage_iterable(empty_test_database, [
                STAGING_PERSON_ROLE_1,
                STAGING_PERSON_ROLE_1
            ])
            result = fetch_table_as_dict(
                empty_test_database,
                'stg.dimPersonRole',
                sorted(STAGED_PERSON_ROLE_1.keys())
            )
            assert result == [STAGED_PERSON_ROLE_1]


    class TestApplyChanges:
        def test_should_apply_one_person_role(self, empty_test_database):
            stage_iterable(empty_test_database, [STAGING_PERSON_ROLE_1])
            applyChanges(empty_test_database, None)

            result = filter_active(fetch_person_role_with_reversed_ids(
                empty_test_database,
                sorted(DIM_PERSON_ROLE_1.keys())
            ))
            LOGGER.debug('expected: %s', DIM_PERSON_ROLE_1)
            LOGGER.debug('result: %s', result)
            assert result == [DIM_PERSON_ROLE_1]


        def test_should_update_person_role_with_same_modfied_date(self, empty_test_database):
            stage_iterable(empty_test_database, [{
                **STAGING_PERSON_ROLE_1,
                'role': ROLE_1
            }])
            applyChanges(empty_test_database, None)

            stage_iterable(empty_test_database, [{
                **STAGING_PERSON_ROLE_1,
                'role': ROLE_2
            }])
            applyChanges(empty_test_database, None)

            expected = {
                **DIM_PERSON_ROLE_1,
                'RoleID': ROLE_2
            }
            result = filter_active(fetch_person_role_with_reversed_ids(
                empty_test_database,
                sorted(DIM_PERSON_ROLE_1.keys())
            ))
            LOGGER.debug('expected: %s', expected)
            LOGGER.debug('result: %s', result)
            assert result == [expected]


        def test_should_update_person_role_with_different_modfied_date(self, empty_test_database):
            stage_iterable(empty_test_database, [{
                **STAGING_PERSON_ROLE_1,
                'role': ROLE_1,
                'profile_modify_date': str(MODIFIED_DATE_1)
            }])
            applyChanges(empty_test_database, None)

            stage_iterable(empty_test_database, [{
                **STAGING_PERSON_ROLE_1,
                'role': ROLE_2,
                'profile_modify_date': str(MODIFIED_DATE_2)
            }])
            applyChanges(empty_test_database, None)

            expected = {
                **DIM_PERSON_ROLE_1,
                'effective_from': MODIFIED_DATE_2,
                'RoleID': ROLE_2
            }
            result = filter_active(fetch_person_role_with_reversed_ids(
                empty_test_database,
                sorted(DIM_PERSON_ROLE_1.keys())
            ))
            LOGGER.debug('expected: %s', expected)
            LOGGER.debug('result: %s', result)
            assert result == [expected]


        def test_should_update_person_role_with_different_source_file_name(self, empty_test_database):
            stage_iterable(empty_test_database, [{
                **STAGING_PERSON_ROLE_1,
                'role': ROLE_1,
                'source_file_name': XML_NAME_1
            }])
            applyChanges(empty_test_database, None)

            stage_iterable(empty_test_database, [{
                **STAGING_PERSON_ROLE_1,
                'role': ROLE_2,
                'source_file_name': XML_NAME_2
            }])
            applyChanges(empty_test_database, None)

            expected = {
                **DIM_PERSON_ROLE_1,
                'RoleID': ROLE_2
            }
            result = filter_active(fetch_person_role_with_reversed_ids(
                empty_test_database,
                sorted(DIM_PERSON_ROLE_1.keys())
            ))
            LOGGER.debug('expected: %s', expected)
            LOGGER.debug('result: %s', result)
            assert result == [expected]
