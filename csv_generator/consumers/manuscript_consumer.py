from bs4 import BeautifulSoup

from consumers.consumer import BaseXMLConsumer


class ManuscriptXMLConsumer(BaseXMLConsumer):
    base_file_name = 'manuscripts.csv'
    headers = ['create_date',
               'zip_name',
               'xml_file_name',
               'msid',
               'country',
               'doi']

    @staticmethod
    def get_msid(soup: BeautifulSoup) -> str:
        try:
            return soup.find('manuscript-number').contents[0].split('-')[-1]
        except (AttributeError, IndexError):
            return ''

    def process(self, soup: BeautifulSoup, xml_file_name: str) -> None:
        """

        :param soup:
        :param xml_file_name:
        :return:
        """
        msid = self.get_msid(soup)
        country = self.get_contents(soup, 'country')
        doi = self.get_contents(soup, 'production-data-doi')
        self._write_row([self.create_date, self.zip_file_name, xml_file_name, msid, country, doi])
