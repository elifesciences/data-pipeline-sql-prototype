from unittest.mock import call, MagicMock, patch

from lxml import etree

from csv_generator.consumers.person_address_consumer import PersonAddressXMLConsumer


def test_can_create_consumer(person_address_consumer: PersonAddressXMLConsumer):
    assert person_address_consumer


def test_can_get_addresses(person_address_consumer: PersonAddressXMLConsumer,
                           persons_xml: str):
    root = etree.fromstring(persons_xml)
    people = person_address_consumer.get_people(root)
    assert len(person_address_consumer.get_addresses(people[0])) == 1


def test_will_handle_not_finding_addresses(person_address_consumer: PersonAddressXMLConsumer):
    root = etree.fromstring('<xml><foo>bar</foo></xml>')
    assert len(person_address_consumer.get_addresses(root)) == 0


@patch('csv_generator.consumers.person_address_consumer.PersonAddressXMLConsumer._write_row')
def test_can_process_data(mock_write_row: MagicMock,
                          person_address_consumer: PersonAddressXMLConsumer,
                          persons_xml: str):
    expected = ['1526868166', 'foobar.xml', '1009',
                1340025741, None, 'Primary Work', 'Switzerland', 'Lausanne',
                '', '', 'Department of Plant Molecular Biology', None, '']

    root = etree.fromstring(persons_xml)
    person_address_consumer.process(root, 'foobar.xml')

    assert mock_write_row.called_once()
    assert mock_write_row.call_args == call(expected)
