import logging

from consumers.person_consumer import PersonXMLConsumer
from consumers.utils import timestamp_to_epoch


LOGGER = logging.getLogger(__name__)


class PersonAddressXMLConsumer(PersonXMLConsumer):
    base_file_name = 'person_addresses.csv'
    headers = [
        'create_date',
        'zip_name',
        'xml_file_name',
        'person_id',
        'address_start_date',
        'address_end_date',
        'address_type',
        'address_country',
        'address_city',
        'address_state_province',
        'address_zip_postal_code',
        'address_department',
        'address_street_address_1',
        'address_street_address_2'
    ]

    address_elements = [
        'address-start-date',
        'address-end-date',
        'address-type',
        'address-country',
        'address-city',
        'address-state-province',
        'address-zip-postal-code',
        'address-department',
        'address-street-address_1',
        'address-street-address-2'
    ]

    date_elements = [
        'address-start-date',
        'address-end-date'
    ]

    @staticmethod
    def get_addresses(ele: 'lxml.etree.ElementTree') -> str:
        return ele.findall('addresses/address')

    def process(self, element: 'lxml.etree.ElementTree', xml_file_name: str) -> None:
        """

        :param element: class: `lxml.etree.ElementTree`
        :param xml_file_name: str
        :return:
        """

        for person in self.get_people(element):

            person_id = self.get_contents(person, 'person-id')

            for address in self.get_addresses(person):
                address_values = []

                for address_element in self.address_elements:
                    value = self.get_contents(address, address_element)

                    if address_element in self.date_elements:
                        value = timestamp_to_epoch(value)

                    address_values.append(value)

                self._write_row([self.create_date, self.zip_file_name, xml_file_name, person_id] + address_values)
