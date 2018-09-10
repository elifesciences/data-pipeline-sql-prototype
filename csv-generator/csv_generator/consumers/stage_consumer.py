from .version_consumer import VersionXMLConsumer
from .utils import timestamp_to_epoch


class StageXMLConsumer(VersionXMLConsumer):
    base_file_name = 'stages.csv'
    headers = ['create_date',
               'source_file_name',
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
    def get_stages(element: 'lxml.etree.ElementTree') -> str:
        return element.findall('history/stage')

    def process(self, element: 'lxml.etree.ElementTree', source_file_name: str) -> None:
        """Parse target`BeautifulSoup` object, extract required data and
        write data row to output_file.

        :param ele: class: `lxml.etree.ElementTree`
        :param source_file_name: str
        :return:
        """

        manuscript = element.find('manuscript')

        msid = self.get_msid(manuscript, source_file_name)
        if not msid:
            return

        versions = self.get_versions(manuscript)

        for version_index, version in enumerate(versions):
            for stage_index, stage in enumerate(self.get_stages(version)):
                stage_values = []
                for stage_element in self.stage_elements:
                    value = self.get_contents(stage, stage_element)

                    if stage_element in self.date_elements:
                        value = timestamp_to_epoch(value)

                    stage_values.append(value)

                self._write_row([self.create_date,
                                 source_file_name, msid, version_index, stage_index] + stage_values)
