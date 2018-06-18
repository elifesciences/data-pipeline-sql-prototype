from bs4 import BeautifulSoup

from csv_generator.process_xml_zip import get_create_date, get_file_list


def test_can_get_create_date(go_xml):
    soup = BeautifulSoup(go_xml, 'lxml-xml')
    assert get_create_date(soup) == '2018-05-21 02:02:46'


def test_can_get_file_list(go_xml):
    soup = BeautifulSoup(go_xml, 'lxml-xml')
    assert len(get_file_list(soup)) == 9
