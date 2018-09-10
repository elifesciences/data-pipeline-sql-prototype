import logging

from .person_consumer import PersonXMLConsumer
from .utils import timestamp_to_epoch

LOGGER = logging.getLogger(__name__)


class PersonRoleXMLConsumer(PersonXMLConsumer):
    base_file_name = 'person_roles.csv'
    headers = [
        'create_date',
        'source_file_name',
        'person_id',
        'profile_modify_date',
        'role'
    ]

    @staticmethod
    def get_roles(element: 'lxml.etree.ElementTree') -> str:
        return element.findall('roles/role')

    def process(self, element: 'lxml.etree.ElementTree', source_file_name: str) -> None:
        """

        :param element: class: `lxml.etree.ElementTree`
        :param source_file_name: str
        :return:
        """

        for person in self.get_people(element):
            person_id = self.get_contents(person, 'person-id')
            profile_modify_date = timestamp_to_epoch(
                self.get_contents(person, 'profile-modify-date')
            )

            for role in self.get_roles(person):
                role_type = self.get_contents(role, 'role-type')
                self._write_row([
                    self.create_date, source_file_name,
                    person_id, profile_modify_date, role_type
                ])
