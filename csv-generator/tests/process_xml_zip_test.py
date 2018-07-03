from lxml import etree

import pytest

from csv_generator.process_xml_zip import get_create_date, get_file_list


@pytest.mark.skip('Needs refactoring to make pass now we are using lxml')
def test_can_get_create_date(go_xml):
    root = etree.fromstring(go_xml)
    assert get_create_date(root) == '2018-05-21 02:02:46'


def test_can_get_file_list(go_xml):
    root = etree.fromstring(go_xml)
    assert len(get_file_list(root)) == 9
