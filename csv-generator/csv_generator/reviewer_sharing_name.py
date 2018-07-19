import argparse
import csv
import logging
import os
from itertools import islice
import re
from typing import List

from tqdm import tqdm

from .consumers.utils import timestamp_to_epoch, clean_msid

LOGGER = logging.getLogger(__name__)

CSV_TEMP_FILE = 'temp.csv'
CSV_OUTPUT_FILE = 'reviewer_identity_revealed.csv'
DEFAULT_OUTPUT_DIR = '../output'
HEADERS = [
    'create_date',
    'source_file_name',
    'msid',
    'version_position_in_ms',
    'reviewer_person_id',
    'share_name'
]
ROWS_TO_SKIP = 4


def extract_create_date(input_data: str) -> int:
    """Extracts create date from file name and
    returns its epoch value.

    example:

    >>> extract_create_date('...identity_revealled_2018_07_13_eLife.csv')
    1531440000

    :param input_data: str
    :return: int
    """
    date_str = ''
    result = re.search(r'(\d{4}_\d{2}_\d{2})', input_data)
    if result:
        date_str = result.group()

    return timestamp_to_epoch(date_str)


def extract_version_position(input_data: str) -> int:
    """Extracts version position from a Manuscript Tracking No.

    example:

    >>> extract_version_position('01-02-2017-SR-eLife-10002R2')
    2
    >>> extract_version_position('01-02-2017-SR-eLife-10002R1')
    1
    >>> extract_version_position('01-02-2017-SR-eLife-10002')
    0

    :param input_data: str
    :return: int
    """
    position = 0
    result = re.search(r'(R\d{1,2})', input_data)
    if result:
        position = int(result.group().replace('R', ''))

    return position


def convert_row(row: List[str], source_file: str, create_date: int) -> List[str]:
    """Converts existing `row`s values to required formats.

    example input row:

    ["01-02-2017-SR-eLife-10001R1","104651","1","","2018-01-02 09:09:09.090"]

    output:

    [1531440000, ...identity_revealled_2018_07_13_eLife.csv, 10001, 1, 104651 ,1]

    :param row: list
    :param source_file: str
    :param create_date: int
    :return: list
    """
    return [
        create_date,
        source_file,
        clean_msid(row[0]).split('-')[-1],
        extract_version_position(row[0]),
        row[1],
        {'1': 1}.get(row[2], 0)
    ]


def create_output_csv(source_file: str, output_file: str, create_date: int) -> None:
    """Read from an `source_file`, add headers and
    convert any cell timestamp values, then export to
    `output_file` csv.

    :param source_file: str
    :param output_file: str
    :param create_date: int
    :return:
    """

    with open(source_file) as csv_file:
        reader = csv.reader(csv_file)

        with open(output_file, 'w') as output_csv:
            writer = csv.writer(output_csv, delimiter=',')
            writer.writerow(HEADERS)

            for row in tqdm(islice(reader, ROWS_TO_SKIP, None), desc='Processing csv row'):
                writer.writerow(convert_row(row, source_file, create_date))


def main():
    parser = argparse.ArgumentParser(description='reviewer_sharing_name')

    parser.add_argument('--source-file', type=str, help='Source file e.g. some-file.csv')
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR,
                        type=str, help='CSV output directory. Defaults to {}'.format(DEFAULT_OUTPUT_DIR))

    args = parser.parse_args()
    source_file = args.source_file
    output_dir = args.output_dir

    os.makedirs(output_dir, exist_ok=True)

    create_date = extract_create_date(source_file)
    create_output_csv(source_file, os.path.join(output_dir, CSV_OUTPUT_FILE), create_date)

    LOGGER.info('complete')


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    main()
