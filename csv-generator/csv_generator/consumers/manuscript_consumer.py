import logging

from .consumer import BaseXMLConsumer
from .utils import clean_msid

LOGGER = logging.getLogger(__name__)


class ManuscriptXMLConsumer(BaseXMLConsumer):
    base_file_name = 'manuscripts.csv'
    headers = ['create_date',
               'source_file_name',
               'msid',
               'country',
               'doi']

    @staticmethod
    def get_msid(element: 'lxml.etree.ElementTree', source_file_name: str = None) -> str:
        try:
            contents = element.findtext('version/manuscript-number')
            msid = clean_msid(contents).split('-')[-1]
        except (AttributeError, IndexError, TypeError):
            msid = ''
        if not msid or not msid.isdigit():
            LOGGER.info('manuscript id "%s" invalid, ignoring %s', msid, source_file_name)
        return msid

    def process(self, element: 'lxml.etree.ElementTree', source_file_name: str) -> None:
        """

        :param element: class: lxml.etree.ElementTree
        :param source_file_name:
        :return:
        """
        manuscript = element.find('manuscript')

        if manuscript is None:
            LOGGER.info('no manuscript element found in %s', source_file_name)
            return

        msid = self.get_msid(manuscript, source_file_name)

        if not msid:
            return

        country = self.get_contents(manuscript, 'country')
        doi = self.get_contents(manuscript, 'production-data/production-data-doi')
        self._write_row([self.create_date, source_file_name, msid, country, doi])
