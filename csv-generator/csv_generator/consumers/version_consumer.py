from .manuscript_consumer import ManuscriptXMLConsumer


class VersionXMLConsumer(ManuscriptXMLConsumer):
    base_file_name = 'versions.csv'
    headers = [
        'create_date',
        'zip_name',
        'xml_file_name',
        'msid',
        'version_position_in_ms',
        'decision',
        'ms_type',
        'senior_editor_person_id',
        'reviewing_editor_person_id',
    ]

    def get_reviewing_editor_id(self, element: 'lxml.etree.ElementTree') -> str:
        """Retrieves the first `editor`'s `editor-person-id` value.

        Example input element:

        <version>
            ...
            <editors>
                <editor>
                    <editor-assigned-date>20th May 18  18:15:30</editor-assigned-date>
                    <editor-decision></editor-decision>
                    <editor-decision-date></editor-decision-date>
                    <editor-decision-due-date></editor-decision-due-date>
                    <editor-person-id>1102</editor-person-id>
                </editor>
            </editors>
        </version>

        :param element:
        :return: str
        """
        try:
            editor = element.findall('editors/editor')[0]
            return self.get_contents(editor, 'editor-person-id')
        except IndexError:
            return ''

    def get_senior_editor_id(self, element: 'lxml.etree.ElementTree') -> str:
        """Retrieves the first `senior-editor`'s `senior-editor-person-id` value.

        Example input element:

        <version>
            ...
            <senior-editors>
                <senior-editor>
                  <senior-editor-assigned-date>14th May 18  14:20:16</senior-editor-assigned-date>
                    <senior-editor-person-id>1109</senior-editor-person-id>
                </senior-editor>
            </senior-editors>
        </version>

        :param element:
        :return: str
        """
        try:
            editor = element.findall('senior-editors/senior-editor')[0]
            return self.get_contents(editor, 'senior-editor-person-id')
        except IndexError:
            return ''

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
            self._write_row(
                [
                    self.create_date,
                    self.zip_file_name,
                    xml_file_name,
                    msid,
                    index,
                    self.get_contents(version, 'decision'),
                    self.get_contents(version, 'manuscript-type'),
                    self.get_senior_editor_id(version),
                    self.get_reviewing_editor_id(version)
                ]
            )
