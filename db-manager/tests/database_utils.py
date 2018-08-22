import logging

from typing import Iterable, List, Tuple


LOGGING = logging.getLogger(__name__)


def get_query_results(connection, query) -> List[Tuple]:
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def to_dict_list(iterable: Iterable[Tuple], keys: List[str]) -> List[dict]:
    return [{
        k: v
        for k, v in zip(keys, row)
    } for row in iterable]


def fetch_table_as_dict(connection, table_name: str, columns: List[str] = None) -> List[dict]:
    query = 'select %s from %s' % (
        '*' if columns is None else ','.join(columns),
        table_name
    )
    LOGGING.debug('query: %s', query)
    with connection.cursor() as cursor:
        cursor.execute(query)
        result_columns = (
            [desc[0] for desc in cursor.description]
            if columns is None
            else columns
        )
        return to_dict_list(cursor.fetchall(), result_columns)


def fetch_table_column(connection, table_name: str, column: str) -> list:
    result = fetch_table_as_dict(connection, table_name, [column])
    return [d[column] for d in result]


def fetch_table_as_map(connection, table_name: str, key_column: str, value_column: str) -> dict:
    result = fetch_table_as_dict(connection, table_name, [key_column, value_column])
    return {
        d[key_column]: d[value_column]
        for d in result
    }
