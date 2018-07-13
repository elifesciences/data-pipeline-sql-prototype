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


def test_can_get_reviewing_editor_person_id(version_consumer: VersionXMLConsumer,
                                            versions_xml: str):
    root = etree.fromstring(versions_xml)
    manuscript = root.find('manuscript')
    version = version_consumer.get_versions(manuscript)[0]

    editor_id = version_consumer.get_reviewing_editor_id(version)

    assert editor_id == '1132'


def test_can_get_senior_editor_person_id(version_consumer: VersionXMLConsumer,
                                         versions_xml: str):
    root = etree.fromstring(versions_xml)
    manuscript = root.find('manuscript')
    version = version_consumer.get_versions(manuscript)[0]

    editor_id = version_consumer.get_senior_editor_id(version)

    assert editor_id == '1122'


def test_will_return_empty_string_if_no_reviewing_editor(version_consumer: VersionXMLConsumer):
    root = etree.fromstring('<xml><foo>bar</foo></xml>')
    assert version_consumer.get_reviewing_editor_id(root) == ''


def test_will_return_empty_string_if_no_senior_editor(version_consumer: VersionXMLConsumer):
    root = etree.fromstring('<xml><foo>bar</foo></xml>')
    assert version_consumer.get_senior_editor_id(root) == ''


@patch('csv_generator.consumers.version_consumer.VersionXMLConsumer._write_row')
def test_can_process_data(mock_write_row: MagicMock,
                          version_consumer: VersionXMLConsumer,
                          versions_xml: str):
    expected = [
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 0,
              'Revise Full Submission', 'Research Article', '1122', '1132']),
        call(['1526868166', 'test_file.zip', 'foobar.xml', '33099', 1,
              'Accept Full Submission', 'Research Article', '1109', '1132'])
    ]

    root = etree.fromstring(versions_xml)
    version_consumer.process(root, 'foobar.xml')

    assert mock_write_row.call_count == 2
    assert mock_write_row.call_args_list == expected
