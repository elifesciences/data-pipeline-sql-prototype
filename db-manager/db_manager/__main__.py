import argparse
import logging
import re
import os
from typing import List, Tuple, Iterable, Any
from contextlib import contextmanager
from itertools import groupby

import psycopg2
from natsort import natsorted

from . import dimCountry, dimPerson, dimManuscript


LOGGER = logging.getLogger(__name__)


class FileTypePatterns:
    COUNTRY = 'country'
    MANUSCRIPT = 'manuscripts'
    MANUSCRIPT_VERSION = 'versions'
    MANUSCRIPT_STAGE = 'stages'
    PERSON = 'persons'
    PERSON_ROLE = 'person_roles'


class SubCommands:
    TEARDOWN = 'teardown'
    CREATE = 'create'
    IMPORT_DATA = 'import-data'


TIMESTAMP_PATTERN = r'(\d+).*'


StagingModule = Any
StagingParams = dict
StagingInstruction = Tuple[StagingModule, StagingParams]


@contextmanager
def managed_connection():
  connect_str = "host={host} port={port} dbname={dbname} user={user} password={password}".format(
    host=os.environ.get('DATABASE_HOST', 'localhost'),
    port=os.environ.get('DATABASE_PORT', '5432'),
    dbname=os.environ.get('DATABASE_NAME', 'elife_etl'),
    user=os.environ.get('DATABASE_USER', 'elife_etl'),
    password=os.environ.get('DATABASE_PASSWORD', 'elife_etl')
  )
  connection = psycopg2.connect(connect_str)
  yield connection
  connection.close()


def run_sql_script(connection, script_filename):
    with open(script_filename, 'r') as f:
        sql = f.read()
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()


def teaddown_database(connection):
    LOGGER.info('tearing down database')
    run_sql_script(connection, os.path.join('sql_scripts', 'teardown.sql'))


def create_database(connection):
    LOGGER.info('creating database')
    run_sql_script(connection, os.path.join('sql_scripts', 'create.sql'))


def find_filenames_to_process(source_dir):
    return [
      os.path.join(source_dir, filename)
      for filename in os.listdir(source_dir)
      if filename.endswith('.csv')
    ]


def filename_pattern_matches(filename, pattern):
    return pattern in os.path.basename(filename)


def filter_filenames_by_pattern(filenames, pattern):
    return [
        filename
        for filename in filenames
        if filename_pattern_matches(filename, pattern)
    ]


def group_filenames_to_staging_instruction(filenames: List[str]) -> Iterable[StagingInstruction]:
    country_filenames = filter_filenames_by_pattern(filenames, FileTypePatterns.COUNTRY)
    manuscript_filenames = filter_filenames_by_pattern(filenames, FileTypePatterns.MANUSCRIPT)
    manuscript_version_filenames = filter_filenames_by_pattern(
        filenames, FileTypePatterns.MANUSCRIPT_VERSION
    )
    manuscript_stage_filenames = filter_filenames_by_pattern(
        filenames, FileTypePatterns.MANUSCRIPT_STAGE
    )
    person_filenames = filter_filenames_by_pattern(filenames, FileTypePatterns.PERSON)
    person_role_filenames = filter_filenames_by_pattern(
        filenames, FileTypePatterns.PERSON_ROLE
    )

    for filename in country_filenames:
        yield (
            dimCountry,
            {
                'file_path': filename
            }
        )

    for person_filename, person_role_filename in zip(
        person_filenames, person_role_filenames
    ):
        yield (
            dimPerson,
            {
                'person': person_filename,
                'person_role': person_role_filename
            }
        )

    for manuscript_filename, manuscript_version_filename, manuscript_stage_filename in zip(
        manuscript_filenames, manuscript_version_filenames, manuscript_stage_filenames
    ):
        yield (
            dimManuscript,
            {
                'manuscript': manuscript_filename,
                'manuscriptVersion': manuscript_version_filename,
                'manuscriptVersionHistory': manuscript_stage_filename,
            }
        )


def sort_filenames(filenames):
    return natsorted(filenames)


def extract_timestamp_from_filename(filename):
    m = re.match(TIMESTAMP_PATTERN, os.path.basename(filename))
    if not m:
        raise ValueError('unrecognised filename pattern, missing timestamp: %s' % filename)
    return m.group(1)


def sort_and_group_filenames(filenames):
    groupby_key = extract_timestamp_from_filename
    return groupby(sort_filenames(filenames), key=groupby_key)


def filenames_to_staging_module(filenames: List[str]) -> Iterable[StagingInstruction]:
    filenames = sort_filenames(filenames)
    yield from group_filenames_to_staging_instruction(filenames)


def filenames_to_grouped_staging_instruction(
    filenames: List[str]) -> Iterable[Tuple[str, Iterable[StagingInstruction]]]:

    for group_key, grouped_filenames in sort_and_group_filenames(filenames):
        grouped_filenames = list(grouped_filenames)
        LOGGER.debug('group %s, filenames: %s', group_key, grouped_filenames)
        yield group_key, list(group_filenames_to_staging_instruction(grouped_filenames))


def process_staging_instructions_group(
    connection, staging_instructions: Iterable[StagingInstruction], is_batch_mode: bool):

    LOGGER.debug('processing staging_instructions: %s', staging_instructions)
    staging_modules = []
    for staging_module, staging_parameters in staging_instructions:
        LOGGER.info('staging: %s %s', staging_module.__name__, staging_parameters)
        staging_module.stage_csv(connection, **staging_parameters)
        staging_modules.append(staging_module)
    for staging_module in staging_modules:
        staging_module.applyChanges(connection)


def process_filenames(connection, filenames: List[str], is_batch_mode: bool):
    LOGGER.debug('filenames: %s', filenames)
    grouped_staging_instructions = filenames_to_grouped_staging_instruction(filenames)
    for group_key, staging_instructions in grouped_staging_instructions:
        LOGGER.info('processing staging group: %s', group_key)
        process_staging_instructions_group(
            connection, staging_instructions, is_batch_mode=is_batch_mode
        )


def process_source_dir(connection, source_dir: str, is_batch_mode: bool):
    LOGGER.info('importing data from: %s', source_dir)
    filenames = find_filenames_to_process(source_dir)
    process_filenames(connection, filenames, is_batch_mode=is_batch_mode)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description='DB Manager')

    subparsers = parser.add_subparsers(
        dest='command',
        help='sub-commands'
    )

    subparsers.add_parser(
        SubCommands.TEARDOWN,
        help='teardown database'
    )

    subparsers.add_parser(
        SubCommands.CREATE,
        help='create database'
    )

    import_sub_command = subparsers.add_parser(
        SubCommands.IMPORT_DATA,
        help='stage and import data'
    )
    import_sub_command.add_argument(
        '--source-dir', type=str, required=True,
        help='Directory containing CSV files to process'
    )

    parser.add_argument(
        '--batch',
        action='store_true',
        help='Enable batch mode (output for logging rather than human consumption)'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging to console'
    )

    return parser.parse_args()


def main(argv=None):
    args = parse_args(argv)

    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    LOGGER.debug('args: %s', args)

    with managed_connection() as connection:
        if args.command == SubCommands.TEARDOWN:
            teaddown_database(connection)
        elif args.command == SubCommands.CREATE:
            create_database(connection)
        elif args.command == SubCommands.IMPORT_DATA:
            process_source_dir(
                connection,
                args.source_dir,
                is_batch_mode=args.batch
            )
        else:
            # this should never happen
            raise AssertionError('unknown sub command: %s' % args.command)

    LOGGER.info('done')


if __name__ == '__main__':
    logging.basicConfig(level='INFO')

    main()
