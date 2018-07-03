from unittest.mock import call, MagicMock, patch

from lxml import etree

from csv_generator.consumers.version_consumer import VersionXMLConsumer


def test_can_create_consumer(version_consumer: VersionXMLConsumer):
    assert version_consumer


def test_can_get_versions(version_consumer: VersionXMLConsumer,
                          versions_xml: str):
    root = etree.fromstring(versions_xml)
    manuscript = root.find('manuscript')
    assert len(version_consumer.get_versions(manuscript)) == 2


@patch('csv_generator.consumers.version_consumer.VersionXMLConsumer._write_row')
def test_can_process_data(mock_write_row: MagicMock,
                          version_consumer: VersionXMLConsumer,
                          versions_xml: str):
    expected = [
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 0,
              'Revise Full Submission', 'Research Article']),
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 1,
              'Accept Full Submission', 'Research Article'])
    ]

    root = etree.fromstring(versions_xml)
    version_consumer.process(root, 'foobar.xml')

    assert mock_write_row.call_count == 2
    assert mock_write_row.call_args_list == expected
