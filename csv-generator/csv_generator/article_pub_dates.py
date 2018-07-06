import argparse
import csv
import logging
import os
from tempfile import TemporaryDirectory

import requests
from tqdm import tqdm

from consumers.utils import timestamp_to_epoch


LOGGER = logging.getLogger(__name__)

CSV_TEMP_FILE = 'temp.csv'
CSV_OUTPUT_FILE = 'published_article_index.csv'
DEFAULT_OUTPUT_DIR = '../output'
HEADERS = ['msid', 'poa_published_date', 'vor_published_date']
URL = 'https://observer.elifesciences.org/report/published-research-article-index.csv'


def _get_article_pub_index() -> str:
    response = requests.get(URL)
    response.raise_for_status()
    return response.content.decode('utf-8')


def _create_output_csv(input_csv_file: str, output_file: str) -> None:
    """Read from an `input_csv_file`, add headers and
    convert any cell timestamp values, then export to
    `output_file` csv.

    :param input_csv_file: str
    :param output_file: str
    :return:
    """
    with open(input_csv_file) as csv_file:
        reader = csv.reader(csv_file)

        with open(output_file, 'w') as output_csv:
            writer = csv.writer(output_csv, delimiter=',')
            writer.writerow(HEADERS)

            for row in tqdm(reader, desc='Processing csv row'):
                writer.writerow([row[0], timestamp_to_epoch(row[1]), timestamp_to_epoch(row[2])])


def create_pub_dates_csv(output_dir: str) -> None:
    with TemporaryDirectory() as temp_dir:
        csv_file_path = os.path.join(temp_dir, CSV_TEMP_FILE)

        with open(csv_file_path, 'w') as csv_file:
            csv_file.write(_get_article_pub_index())

        _create_output_csv(csv_file_path, os.path.join(output_dir, CSV_OUTPUT_FILE))


def main():
    parser = argparse.ArgumentParser(description='article_pub_dates')

    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR,
                        type=str, help='CSV output directory. Defaults to {}'.format(DEFAULT_OUTPUT_DIR))

    args = parser.parse_args()
    output_dir = args.output_dir

    os.makedirs(output_dir, exist_ok=True)

    create_pub_dates_csv(output_dir)

    LOGGER.info('complete')


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    main()

