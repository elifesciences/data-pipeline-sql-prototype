import logging

from .consumer import BaseXMLConsumer
from .utils import timestamp_to_epoch


LOGGER = logging.getLogger(__name__)


class PersonXMLConsumer(BaseXMLConsumer):
    base_file_name = 'persons.csv'
    headers = [
        'create_date',
        'zip_name',
        'xml_file_name',
        'person_id',
        'status',
        'first-name',
        'middle-name',
        'last-name',
        'email',
        'secondary_email',
        'profile-modify-date'
    ]

    person_elements = [
        'person-id',
        'status',
        'first-name',
        'middle-name',
        'last-name',
        'email',
        'secondary_email'
    ]

    date_elements = ['profile-modify-date']

    @staticmethod
    def get_people(ele: 'lxml.etree.ElementTree') -> str:
        return ele.findall('people/person')

    def process(self, element: 'lxml.etree.ElementTree', xml_file_name: str) -> None:
        """

        :param element: class: `lxml.etree.ElementTree`
        :param xml_file_name: str
        :return:
        """

        for person in self.get_people(element):
            person_values = []

            for person_element in self.person_elements:
                value = self.get_contents(person, person_element)

                if person_element in self.date_elements:
                    value = timestamp_to_epoch(value)

                person_values.append(value)

            self._write_row([self.create_date, self.zip_file_name, xml_file_name] + person_values)
