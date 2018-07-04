import logging

from .person_consumer import PersonXMLConsumer

LOGGER = logging.getLogger(__name__)


class PersonRoleXMLConsumer(PersonXMLConsumer):
    base_file_name = 'person_roles.csv'
    headers = [
        'create_date',
        'zip_name',
        'xml_file_name',
        'person_id',
        'role',
    ]

    @staticmethod
    def get_roles(element: 'lxml.etree.ElementTree') -> str:
        return element.findall('roles/role')

    def process(self, element: 'lxml.etree.ElementTree', xml_file_name: str) -> None:
        """

        :param element: class: `lxml.etree.ElementTree`
        :param xml_file_name: str
        :return:
        """

        for person in self.get_people(element):
            person_id = self.get_contents(person, 'person-id')

            for role in self.get_roles(person):
                role_type = self.get_contents(role, 'role-type')
                self._write_row([self.create_date, self.zip_file_name, xml_file_name, person_id, role_type])
