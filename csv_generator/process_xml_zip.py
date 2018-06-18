import argparse
import os
import shutil
from typing import List
import zipfile

from bs4 import BeautifulSoup

from consumers import feed_consumers, make_soup
from consumers.utils import timestamp_to_epoch


DEFAULT_ZIP_OUTPUT_DIR = '../zip_output'
DEFAULT_OUTPUT_DIR = '../output'
GO_XML_FILE_NAME = 'go.xml'


def clean_up_zip(target_dir: str) -> None:
    """Delete target zip output directory.

    :param target_dir: str
    :return:
    """
    try:
        shutil.rmtree(target_dir)
    except FileNotFoundError:
        pass


def extract_zip_file(target_zip: str, output_dir: str) -> None:
    """Extract `target_zip` to `output_dir`

    :param target_zip: str
    :param output_dir: str
    :return:
    """
    zip_file = zipfile.ZipFile(target_zip)

    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    zip_file.extractall(path=output_dir)


def get_create_date(soup: BeautifulSoup) -> str:
    """Return `create_date` from soup.

    :param soup:
    :return: class: `BeautifulSoup`
    """
    return soup.find('file_list')['create_date']


def get_file_list(soup: BeautifulSoup) -> List[str]:
    """Return `file_nm` from soup.

    :param soup:
    :return: list
    """
    return [name.contents[0] for name in soup.find_all('file_nm')]


def main():
    parser = argparse.ArgumentParser(description='process_zip')

    parser.add_argument('--target-zip', type=str, help='Target zip file e.g. some-file.zip')
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR,
                        type=str, help='CSV output directory. Defaults to {}'.format(DEFAULT_OUTPUT_DIR))

    args = parser.parse_args()

    target_zip = args.target_zip
    output_dir = args.output_dir

    print('processing: ', target_zip)

    extract_zip_file(target_zip=target_zip, output_dir=DEFAULT_ZIP_OUTPUT_DIR)

    go_soup = make_soup(os.path.join(DEFAULT_ZIP_OUTPUT_DIR, GO_XML_FILE_NAME))

    create_date = timestamp_to_epoch(get_create_date(go_soup))

    feed_consumers(file_list=get_file_list(go_soup),
                   output_dir=output_dir,
                   create_date=create_date,
                   zip_dir=DEFAULT_ZIP_OUTPUT_DIR,
                   zip_file_name=target_zip.split(os.path.sep)[-1])

    clean_up_zip(target_dir=DEFAULT_ZIP_OUTPUT_DIR)

    print('complete')


if __name__ == '__main__':
    main()
