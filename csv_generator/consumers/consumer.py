import csv
import os
from typing import List


class BaseXMLConsumer:
    delimiter = ','
    headers = []
    base_file_name = 'base_consumer.csv'
    _output_file = ''

    def __init__(self, create_date: str, zip_file_name: str, output_dir: str = '') -> None:
        self.create_date = create_date
        self.zip_file_name = zip_file_name
        self.output_dir = output_dir
        self._delete_old_output_file()
        self._write_row(self.headers)

    def _delete_old_output_file(self):
        if os.path.isfile(self.output_file):
            os.remove(self.output_file)

    @staticmethod
    def get_contents(ele: 'lxml.etree.ElementTree', target_ele: str) -> str:
        try:
            return ele.findtext(target_ele)
        except (AttributeError, IndexError):
            return ''

    @property
    def output_file(self) -> str:
        if not self._output_file:
            self._output_file = os.path.join(self.output_dir, '{0}_{1}'.format(self.create_date, self.base_file_name))
        return self._output_file

    def process(self, ele: 'lxml.etree.ElementTree', xml_file_name: str) -> None:
        """Parse target `lxml.etree.ElementTree` object, extract required data and
        write data row to output_file.

        :param ele: class: `lxml.etree.ElementTree`
        :param xml_file_name:
        :return:
        """
        pass

    def _write_row(self, row_data: List[str]) -> None:
        """

        :param row_data:
        :return:
        """
        with open(self.output_file, 'a', newline='') as output_file:
            writer = csv.writer(output_file, delimiter=self.delimiter)
            writer.writerow(row_data)
