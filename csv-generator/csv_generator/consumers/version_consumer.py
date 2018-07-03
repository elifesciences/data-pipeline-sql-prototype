from .manuscript_consumer import ManuscriptXMLConsumer


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
    def get_versions(element: 'lxml.etree.ElementTree') -> str:
        return element.findall('version')

    def process(self, element: 'lxml.etree.ElementTree', xml_file_name: str) -> None:
        """Parse target `lxml.etree.ElementTree` object, extract required data and
        write data row to output_file.

        :param element: class: `lxml.etree.ElementTree`
        :param xml_file_name: str
        :return:
        """
        manuscript = element.find('manuscript')

        msid = self.get_msid(manuscript, xml_file_name=xml_file_name)

        if not msid:
            return

        versions = self.get_versions(manuscript)

        for index, version in enumerate(versions):
            decision = self.get_contents(version, 'decision')
            ms_type = self.get_contents(version, 'manuscript-type')
            self._write_row([self.create_date, self.zip_file_name, xml_file_name,
                             msid, index, decision, ms_type])
