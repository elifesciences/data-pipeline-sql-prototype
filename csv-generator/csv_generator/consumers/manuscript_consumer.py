import logging
import re

from .consumer import BaseXMLConsumer


LOGGER = logging.getLogger(__name__)


class ManuscriptXMLConsumer(BaseXMLConsumer):
    base_file_name = 'manuscripts.csv'
    headers = ['create_date',
               'zip_name',
               'xml_file_name',
               'msid',
               'country',
               'doi']

    msid_strip_values = [
        '-Appeal',
        r'[R]\d+'
    ]

    def _clean_msid(self, id_contents: str) -> str:
        """Will strip out any values matched from `msid_strip_values`.

        :param id_contents: str
        :return: str
        """
        contents = id_contents
        for strip_val in self.msid_strip_values:
            contents = re.sub(strip_val, '', contents)

        return contents

    def get_msid(self, element: 'lxml.etree.ElementTree', xml_file_name: str = None) -> str:
        try:
            contents = element.findtext('version/manuscript-number')
            msid = self._clean_msid(contents).split('-')[-1]
        except (AttributeError, IndexError, TypeError):
            msid = ''
        if not msid or not msid.isdigit():
            LOGGER.info('manuscript id "%s" invalid, ignoring %s', msid, xml_file_name)
        return msid

    def process(self, element: 'lxml.etree.ElementTree', xml_file_name: str) -> None:
        """

        :param element: class: lxml.etree.ElementTree
        :param xml_file_name:
        :return:
        """
        manuscript = element.find('manuscript')

        if manuscript is None:
            LOGGER.info('no manuscript element found in %s', xml_file_name)
            return

        msid = self.get_msid(manuscript, xml_file_name=xml_file_name)

        if not msid:
            return

        country = self.get_contents(manuscript, 'country')
        doi = self.get_contents(manuscript, 'production-data/production-data-doi')
        self._write_row([self.create_date, self.zip_file_name, xml_file_name, msid, country, doi])
