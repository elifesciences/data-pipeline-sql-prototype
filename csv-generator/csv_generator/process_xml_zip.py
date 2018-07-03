import argparse
import logging
import os
from typing import List
from tempfile import TemporaryDirectory
import zipfile

from lxml import etree

from tqdm import tqdm

from .consumers import feed_consumers, make_soup
from .consumers.utils import timestamp_to_epoch


LOGGER = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = '../output'
GO_XML_FILE_NAME = 'go.xml'


def extract_zip_file(source_zip: str, output_dir: str) -> None:
    """Extract `source_zip` to `output_dir`

    :param source_zip: str
    :param output_dir: str
    :return:
    """
    zip_file = zipfile.ZipFile(source_zip)

    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    zip_file.extractall(path=output_dir)


def get_create_date(ele: 'lxml.etree.ElementTree') -> str:
    """Return `create_date` from ele.

    :param ele: class: `lxml.etree.ElementTree`
    :return: str
    """
    return ele.getroot().get('create_date')


def get_file_list(ele: 'lxml.etree.ElementTree') -> List[str]:
    """Return `file_nm` list from ele.

    :param ele: class: `lxml.etree.ElementTree`
    :return: list
    """
    return [name.text for name in ele.findall('file_nm')]


def process_extracted_dir(source_zip, extracted_dir, output_dir, batch=False):
    go_ele = etree.parse(os.path.join(extracted_dir, GO_XML_FILE_NAME))

    create_date = timestamp_to_epoch(get_create_date(go_ele))

    feed_consumers(
        file_list=tqdm(get_file_list(go_ele), leave=False, disable=batch),
        output_dir=output_dir,
        create_date=create_date,
        zip_dir=extracted_dir,
        zip_file_name=source_zip.split(os.path.sep)[-1]
    )


def process_zip(source_zip, output_dir, batch=False):
    with TemporaryDirectory(suffix='-zip-output') as zip_output_dir:
        LOGGER.info('zip_output_dir: %s', zip_output_dir)

        extract_zip_file(source_zip=source_zip, output_dir=zip_output_dir)

        process_extracted_dir(
            source_zip, zip_output_dir, output_dir,
            batch=batch
        )


def process_zip_or_extracted_dir(source_zip_or_extracted_dir, output_dir, batch=False):
    if os.path.isdir(source_zip_or_extracted_dir):
        extracted_dir = source_zip_or_extracted_dir
        if not os.path.exists(os.path.join(source_zip_or_extracted_dir, GO_XML_FILE_NAME)):
            raise RuntimeError('source-zip is a directory but does not contain %s' % (
                GO_XML_FILE_NAME
            ))
        process_extracted_dir(
            source_zip=extracted_dir,
            extracted_dir=extracted_dir,
            output_dir=output_dir,
            batch=batch
        )
    else:
        source_zip = source_zip_or_extracted_dir
        process_zip(source_zip, output_dir, batch=batch)


def main():
    parser = argparse.ArgumentParser(description='process_zip')

    parser.add_argument('--source-zip', type=str, help='Source zip file e.g. some-file.zip')
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR,
                        type=str, help='CSV output directory. Defaults to {}'.format(DEFAULT_OUTPUT_DIR))
    parser.add_argument(
        '--batch',
        action='store_true',
        help='Enable batch mode (output for logging rather than human consumption)'
    )

    args = parser.parse_args()

    source_zip = args.source_zip
    output_dir = args.output_dir

    LOGGER.info('processing: %s', source_zip)

    process_zip_or_extracted_dir(source_zip, output_dir, batch=args.batch)

    LOGGER.info('complete')


if __name__ == '__main__':
    logging.basicConfig(level='INFO')

    main()
