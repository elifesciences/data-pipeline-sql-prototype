from .database_utils import fetch_table_as_map


def reverse_dict(d: dict) -> dict:
    return {v: k for k, v in d.items()}


def fetch_person_id_by_external_ref(connection) -> dict:
    return fetch_table_as_map(
        connection,
        'dim.dimPerson',
        key_column='externalReference',
        value_column='id'
    )


def fetch_person_role_id_by_external_ref(connection) -> dict:
    return fetch_table_as_map(
        connection,
        'dim.dimRole',
        key_column='externalReference',
        value_column='id'
    )
