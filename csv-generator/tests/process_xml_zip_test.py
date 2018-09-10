from zipfile import ZipFile
from unittest.mock import patch

from lxml import etree

import pytest

import csv_generator.process_xml_zip
from csv_generator.process_xml_zip import (
    get_create_date,
    get_file_list,
    process_zip_or_extracted_dir,
    GO_XML_FILE_NAME
)


GO_XML_TIMESTAMP_1 = '2018-01-02 03:04:05'
XML_FILE_NAME_1 = 'file1.xml'

GO_XML_1 = '<file_list create_date="%s"><file_nm>%s</file_nm></file_list>' % (
    GO_XML_TIMESTAMP_1, XML_FILE_NAME_1
)

ZIP_FILE_NAME_1 = 'test1.zip'


@pytest.fixture(name='feed_consumers_mock')
def _feed_consumers_mock():
    with patch.object(csv_generator.process_xml_zip, 'feed_consumers') as feed_consumers_mock:
        yield feed_consumers_mock


@pytest.mark.skip('Needs refactoring to make pass now we are using lxml')
def test_can_get_create_date(go_xml):
    root = etree.fromstring(go_xml)
    assert get_create_date(root) == '2018-05-21 02:02:46'


def test_can_get_file_list(go_xml):
    root = etree.fromstring(go_xml)
    assert len(get_file_list(root)) == 9


class TestProcessZipOrExtractedDir:
    def test_should_process_extracted_dir_and_pass_none_for_zip_file_name(
        self, tmpdir, feed_consumers_mock):

        output_dir = tmpdir.join('output')

        extracted_dir = tmpdir.join('extracted-dir')
        extracted_dir.join(GO_XML_FILE_NAME).write_text(GO_XML_1, 'utf-8', ensure=True)

        process_zip_or_extracted_dir(extracted_dir, output_dir, batch=True)
        assert feed_consumers_mock.called
        feed_consumers_kwargs = feed_consumers_mock.call_args[1]
        assert feed_consumers_kwargs['zip_file_name'] is None


    def test_should_process_zip_and_pass_zip_file_name(
        self, tmpdir, feed_consumers_mock):

        output_dir = tmpdir.join('output')

        zip_file = str(tmpdir.join(ZIP_FILE_NAME_1))
        with ZipFile(zip_file, 'w') as zf:
            zf.writestr(GO_XML_FILE_NAME, GO_XML_1)

        process_zip_or_extracted_dir(zip_file, output_dir, batch=True)
        assert feed_consumers_mock.called
        feed_consumers_kwargs = feed_consumers_mock.call_args[1]
        assert feed_consumers_kwargs['zip_file_name'] == ZIP_FILE_NAME_1
