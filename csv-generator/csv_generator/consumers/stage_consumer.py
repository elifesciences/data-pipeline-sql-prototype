from bs4 import BeautifulSoup

from .version_consumer import VersionXMLConsumer
from .utils import timestamp_to_epoch


class StageXMLConsumer(VersionXMLConsumer):
    base_file_name = 'stages.csv'
    headers = ['create_date',
               'zip_name',
               'xml_file_name',
               'msid',
               'version_position_in_ms',
               'stage_position_in_version',
               'name',
               'affective_person_id',
               'triggered_by_person',
               'start_date']

    stage_elements = ['stage-name',
                      'stage-affective-person-id',
                      'stage-triggered-by-person-id',
                      'start-date']

    date_elements = ['start-date']

    @staticmethod
    def get_stages(soup: BeautifulSoup) -> str:
        return soup.find_all('stage')

    def process(self, soup: BeautifulSoup, xml_file_name: str) -> None:
        """Parse target`BeautifulSoup` object, extract required data and
        write data row to output_file.

        :param soup:
        :param xml_file_name:
        :return:
        """
        msid = self.get_msid(soup, xml_file_name=xml_file_name)
        if not msid:
            return

        versions = self.get_versions(soup)

        for version_index, version in enumerate(versions):
            for stage_index, stage in enumerate(self.get_stages(version)):
                stage_values = []
                for element in self.stage_elements:
                    value = self.get_contents(stage, element)

                    if element in self.date_elements:
                        value = timestamp_to_epoch(value)

                    stage_values.append(value)

                self._write_row([self.create_date, self.zip_file_name,
                                 xml_file_name, msid, version_index, stage_index] + stage_values)
