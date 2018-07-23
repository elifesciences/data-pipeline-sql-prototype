import pytest

from csv_generator.reviewer_sharing_name import (
    convert_row,
    extract_create_date,
    extract_version_position
)


@pytest.mark.parametrize("input_str,epoch", [
    ("...identity_revealled_2018_07_13_eLife.csv", 1531440000),
    ("...identity_revealled_2017_08_13_eLife.csv", 1502582400),
    ("...some_invalid_value", None),
])
def test_can_extract_create_date(input_str: str, epoch: int):
    assert extract_create_date(input_str) == epoch


@pytest.mark.parametrize("input_str,position", [
    ("01-02-2017-SR-eLife-10002R2", 2),
    ("01-02-2017-SR-eLife-10002R1", 1),
    ("01-02-2017-SR-eLife-10002", 0),
])
def test_can_extract_version_position(input_str: str, position: int):
    assert extract_version_position(input_str) == position


def test_can_convert_row():
    input_row = [
        "01-02-2017-SR-eLife-10001R1",
        "104651",
        "1",
        "",
        "2018-01-02 09:09:09.090"
    ]
    create_date = 1531440000
    source_file = 'some_file_2018_07_13.csv'

    expected = [1531440000, 'some_file_2018_07_13.csv', '10001', 1, '104651', 1]
    assert convert_row(input_row, source_file, create_date) == expected
