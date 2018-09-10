from unittest.mock import call, MagicMock, patch

from lxml import etree

from csv_generator.consumers.person_consumer import PersonXMLConsumer


def test_can_create_consumer(person_consumer: PersonXMLConsumer):
    assert person_consumer


def test_can_get_people(person_consumer: PersonXMLConsumer, persons_xml: str):
    root = etree.fromstring(persons_xml)
    assert len(person_consumer.get_people(root)) == 2


def test_will_handle_not_finding_people(person_consumer: PersonXMLConsumer):
    root = etree.fromstring('<xml><foo>bar</foo></xml>')
    assert len(person_consumer.get_people(root)) == 0


@patch('csv_generator.consumers.person_consumer.PersonXMLConsumer._write_row')
def test_can_process_data(mock_write_row: MagicMock,
                          person_consumer: PersonXMLConsumer,
                          persons_xml: str):
    expected = ['1526868166', 'foobar.xml', '1009',
                'Active', 'Prof.', 'User', 'S.', 'Two', 'user2@fake.com', '', 1526549451]

    root = etree.fromstring(persons_xml)
    person_consumer.process(root, 'foobar.xml')

    assert mock_write_row.called_once()
    assert mock_write_row.call_args[0][0] == expected
