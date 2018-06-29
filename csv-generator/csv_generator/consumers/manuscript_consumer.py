import logging

from bs4 import BeautifulSoup

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

    @staticmethod
    def get_msid(soup: BeautifulSoup, xml_file_name: str = None) -> str:
        try:
            msid = soup.find('manuscript-number').contents[0].split('-')[-1]
        except (AttributeError, IndexError):
            msid = ''
        if not msid or not msid.isdigit():
            LOGGER.info('manuscript id "%s" invalid, ignoring %s', msid, xml_file_name)
        return msid

    def process(self, soup: BeautifulSoup, xml_file_name: str) -> None:
        """

        :param soup:
        :param xml_file_name:
        :return:
        """
        if not soup.find('manuscript'):
            LOGGER.info('no manuscript element found in %s', xml_file_name)
            return
        msid = self.get_msid(soup, xml_file_name=xml_file_name)
        if not msid:
            return
        country = self.get_contents(soup, 'country')
        doi = self.get_contents(soup, 'production-data-doi')
        self._write_row([self.create_date, self.zip_file_name, xml_file_name, msid, country, doi])
