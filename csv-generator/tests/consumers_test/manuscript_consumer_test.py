from unittest.mock import call, MagicMock, patch

from lxml import etree

from csv_generator.consumers.manuscript_consumer import ManuscriptXMLConsumer


def test_can_create_consumer(manuscript_consumer: ManuscriptXMLConsumer):
    assert manuscript_consumer


def test_can_get_msid(manuscript_consumer: ManuscriptXMLConsumer,
                      manuscript_xml: str):
    root = etree.fromstring(manuscript_xml)
    manuscript = root.find('manuscript')
    assert manuscript_consumer.get_msid(manuscript) == '33099'


def test_can_get_msid_on_appeal(manuscript_consumer: ManuscriptXMLConsumer,
                                manuscript_appeal_xml: str):
    root = etree.fromstring(manuscript_appeal_xml)
    manuscript = root.find('manuscript')
    assert manuscript_consumer.get_msid(manuscript) == '12345'


def test_will_handle_no_msid(manuscript_consumer: ManuscriptXMLConsumer):
    root = etree.fromstring('<xml><foo>bar</foo></xml>')
    assert manuscript_consumer.get_msid(root) == ''


@patch('csv_generator.consumers.manuscript_consumer.ManuscriptXMLConsumer._write_row')
def test_can_process_data(mock_write_row: MagicMock,
                          manuscript_consumer: ManuscriptXMLConsumer,
                          manuscript_xml: str):
    expected = ['1526868166', 'test_file.zip', 'foobar.xml',
                '33099', 'United States', '10.7554/eLife.33099']

    root = etree.fromstring(manuscript_xml)
    manuscript_consumer.process(root, 'foobar.xml')

    assert mock_write_row.called_once()
    assert mock_write_row.call_args == call(expected)
