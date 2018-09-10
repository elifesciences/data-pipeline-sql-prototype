from unittest.mock import call, MagicMock, patch

from lxml import etree

from csv_generator.consumers.person_role_consumer import PersonRoleXMLConsumer


def test_can_create_consumer(person_role_consumer: PersonRoleXMLConsumer):
    assert person_role_consumer


def test_can_get_roles(person_role_consumer: PersonRoleXMLConsumer,
                       persons_xml: str):
    root = etree.fromstring(persons_xml)
    people = person_role_consumer.get_people(root)
    assert len(person_role_consumer.get_roles(people[0])) == 2


def test_will_handle_not_finding_roles(person_role_consumer: PersonRoleXMLConsumer):
    root = etree.fromstring('<xml><foo>bar</foo></xml>')
    assert len(person_role_consumer.get_roles(root)) == 0


@patch('csv_generator.consumers.person_role_consumer.PersonRoleXMLConsumer._write_row')
def test_can_process_data(mock_write_row: MagicMock,
                          person_role_consumer: PersonRoleXMLConsumer,
                          persons_xml: str):
    expected = [
        '1526868166', 'foobar.xml', '1009', 1526549451, 'Senior Editor'
    ]

    root = etree.fromstring(persons_xml)
    person_role_consumer.process(root, 'foobar.xml')

    assert mock_write_row.called_once()
    assert mock_write_row.call_args[0][0] == expected
