import argparse
import logging

from .database import managed_connection, teardown_database, create_database
from .processing import process_source_dir


LOGGER = logging.getLogger(__name__)


class SubCommands:
    TEARDOWN = 'teardown'
    CREATE = 'create'
    IMPORT_DATA = 'import-data'


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
            teardown_database(connection)
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
