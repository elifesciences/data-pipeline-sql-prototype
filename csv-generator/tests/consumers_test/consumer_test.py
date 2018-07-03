from bs4 import BeautifulSoup
import pytest

from csv_generator.consumers.consumer import BaseXMLConsumer


@pytest.mark.parametrize("element,expected_value", [
    ("country", "United States"),
    ("production-data-doi", "10.7554/eLife.33099"),
])
def test_can_get_contents(element: str,
                          expected_value: str,
                          base_consumer: BaseXMLConsumer,
                          manuscript_xml: str):

    soup = BeautifulSoup(manuscript_xml, 'lxml-xml')
    assert base_consumer.get_contents(soup, element) == expected_value


def test_will_handle_no_contents(base_consumer: BaseXMLConsumer):
    soup = BeautifulSoup('<xml><foo>bar</foo></xml>', 'lxml-xml')
    assert base_consumer.get_contents(soup, 'invalid') == ''


def test_can_generate_output_file_path(base_consumer: BaseXMLConsumer):
    assert base_consumer.output_file == 'test_output/1526868166_base_consumer.csv'
