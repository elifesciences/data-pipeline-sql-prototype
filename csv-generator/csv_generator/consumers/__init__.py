import logging
import os
from typing import List

from lxml import etree

from .consumer import BaseXMLConsumer
from .manuscript_consumer import ManuscriptXMLConsumer
from .person_consumer import PersonXMLConsumer
from .person_address_consumer import PersonAddressXMLConsumer
from .person_role_consumer import PersonRoleXMLConsumer
from .stage_consumer import StageXMLConsumer
from .version_consumer import VersionXMLConsumer


LOGGER = logging.getLogger(__name__)

CONSUMERS = {
    'manuscript': ManuscriptXMLConsumer,
    'person': PersonXMLConsumer,
    'person_address': PersonAddressXMLConsumer,
    'person_role': PersonRoleXMLConsumer,
    'stage': StageXMLConsumer,
    'version': VersionXMLConsumer,
}


def feed_consumers(file_list: List[str], output_dir: str, create_date: str,
                   zip_dir: str, zip_file_name: str) -> None:
    """Feed each consumer `lxml.etree.ElementTree` objects for processing.

    :param file_list: list
    :param output_dir: str
    :param create_date: str
    :param zip_dir: str
    :param zip_file_name: str
    :return:
    """

    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    consumers = _init_consumers(create_date, output_dir, zip_file_name)

    for data_file in file_list:
        LOGGER.debug('consuming: ', data_file)
        root = etree.parse(os.path.join(zip_dir, data_file))

        for consumer in consumers:
            consumer.process(root, xml_file_name=data_file)


def _init_consumers(create_date: str, output_dir: str, zip_file_name: str) -> List[BaseXMLConsumer]:
    """Initialize consumer classes with required values.

    :param create_date: str
    :param output_dir: str
    :param zip_file_name: str
    :return: list
    """
    return [CONSUMERS[consumer](create_date, zip_file_name, output_dir) for consumer in CONSUMERS]
