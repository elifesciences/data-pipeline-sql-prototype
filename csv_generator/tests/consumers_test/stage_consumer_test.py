from unittest.mock import call, MagicMock, patch

from bs4 import BeautifulSoup

from consumers.stage_consumer import StageXMLConsumer


def test_can_create_consumer(stage_consumer: StageXMLConsumer):
    assert stage_consumer


def test_can_get_stages(stage_consumer: StageXMLConsumer,
                        stages_xml: str):
    soup = BeautifulSoup(stages_xml, 'lxml-xml')
    assert len(stage_consumer.get_stages(soup)) == 6


@patch('consumers.stage_consumer.StageXMLConsumer._write_row')
def test_can_process_data(mock_write_row: MagicMock,
                          stage_consumer: StageXMLConsumer,
                          stages_xml: str):
    expected = [
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 0, 0,
              'Preliminary Manuscript Data Submitted', '7733', '7733', 1525303967]),
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 0, 1,
              'Author Approved Converted Files', '7733', '7733', 1525306626]),
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 0, 2,
              'Senior Editor Assigned', '7733', '7733', 1525306626]),
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 0, 3,
              'Initial QC Started', '7733', '7733', 1525306626]),
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 1, 0,
              'Preliminary Manuscript Data Submitted', '7733', '7733', 1525303967]),
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 1, 1,
              'Author Approved Converted Files', '7733', '7733', 1525306626])
    ]

    soup = BeautifulSoup(stages_xml, 'lxml-xml')
    stage_consumer.process(soup, 'foobar.xml')

    assert mock_write_row.call_count == 6
    assert mock_write_row.call_args_list == expected
