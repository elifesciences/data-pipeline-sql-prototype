import pytest

from csv_generator.consumers.utils import convert_ms_type, timestamp_to_epoch


@pytest.mark.parametrize("full_type,short_type", [
    ("Research Article", "RA"),
    ("Initial Submission Research Article", "IS/ISRA"),
    ("Initial Submission Feature Article", "ISFA"),
])
def test_can_convert_convert_ms_types(full_type: str, short_type: str):
    assert convert_ms_type(full_type) == short_type


@pytest.mark.parametrize("ts_str,epoch", [
    ("16th Apr 18  00:00:00", 1523836800),
    ("2018-05-21 02:02:46", 1526868166),
    ("29th Dec 17  15:16:34", 1514560594),
])
def test_can_convert_known_timestamps_to_epoch(ts_str: str, epoch: str):
    assert timestamp_to_epoch(ts_str) == epoch
