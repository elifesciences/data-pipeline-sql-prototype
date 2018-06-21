from unittest.mock import call, MagicMock, patch

from bs4 import BeautifulSoup

from consumers.manuscript_consumer import ManuscriptXMLConsumer


def test_can_create_consumer(manuscript_consumer: ManuscriptXMLConsumer):
    assert manuscript_consumer


def test_can_get_msid(manuscript_consumer: ManuscriptXMLConsumer,
                      manuscript_xml: str):
    soup = BeautifulSoup(manuscript_xml, 'lxml-xml')
    assert manuscript_consumer.get_msid(soup) == '33099'


def test_will_handle_no_msid(manuscript_consumer: ManuscriptXMLConsumer):
    soup = BeautifulSoup('<xml><foo>bar</foo></xml>', 'lxml-xml')
    assert manuscript_consumer.get_msid(soup) == ''


@patch('consumers.manuscript_consumer.ManuscriptXMLConsumer._write_row')
def test_can_process_data(mock_write_row: MagicMock,
                          manuscript_consumer: ManuscriptXMLConsumer,
                          manuscript_xml: str):
    expected = ['1526868166', 'test_file.zip', 'foobar.xml',
                '33099', 'United States', '10.7554/eLife.33099']

    soup = BeautifulSoup(manuscript_xml, 'lxml-xml')
    manuscript_consumer.process(soup, 'foobar.xml')

    assert mock_write_row.called_once()
    assert mock_write_row.call_args == call(expected)
