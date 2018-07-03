from lxml import etree
import pytest

from csv_generator.consumers.consumer import BaseXMLConsumer


@pytest.mark.parametrize("element,expected_value", [
    ("manuscript/country", "United States"),
    ("manuscript/production-data/production-data-doi", "10.7554/eLife.33099"),
])
def test_can_get_contents(element: str,
                          expected_value: str,
                          base_consumer: BaseXMLConsumer,
                          manuscript_xml: str):

    soup = etree.fromstring(manuscript_xml)
    assert base_consumer.get_contents(soup, element) == expected_value


def test_will_handle_no_contents(base_consumer: BaseXMLConsumer):
    soup = etree.fromstring('<xml><foo>bar</foo></xml>')
    assert not base_consumer.get_contents(soup, 'invalid')


def test_can_generate_output_file_path(base_consumer: BaseXMLConsumer):
    assert base_consumer.output_file == 'test_output/1526868166_base_consumer.csv'
