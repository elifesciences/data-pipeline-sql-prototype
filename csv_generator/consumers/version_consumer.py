from bs4 import BeautifulSoup

from consumers.manuscript_consumer import ManuscriptXMLConsumer
from consumers.utils import convert_ms_type


class VersionXMLConsumer(ManuscriptXMLConsumer):
    base_file_name = 'versions.csv'
    headers = ['create_date',
               'zip_name',
               'xml_file_name',
               'msid',
               'version_position_in_ms',
               'decision',
               'ms_type']

    @staticmethod
    def get_versions(soup: BeautifulSoup) -> str:
        return soup.find_all('version')

    def process(self, soup: BeautifulSoup, xml_file_name: str) -> None:
        """Parse target`BeautifulSoup` object, extract required data and
        write data row to output_file.

        :param soup:
        :param xml_file_name:
        :return:
        """
        msid = self.get_msid(soup)

        versions = self.get_versions(soup)

        for index, version in enumerate(versions):
            decision = self.get_contents(version, 'decision')
            ms_type = self.get_contents(version, 'manuscript-type')
            self._write_row([self.create_date, self.zip_file_name, xml_file_name,
                             msid, index, decision, convert_ms_type(ms_type)])
