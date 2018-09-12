import logging
import re
import os
from typing import List, Tuple, Iterable, Any
from itertools import groupby
from natsort import natsorted
from . import (
    utils,
    dimCountry2,
    dimPerson2,
    dimPersonRole2,
    dimManuscript2,
    dimManuscriptVersion2,
    dimManuscriptVersionHistory2,

    dimRole2,
    dimStage2,
)


LOGGER = logging.getLogger(__name__)

PERSON, COUNTRY, ROLE, STAGE = 'PERSON', 'COUNTRY', 'ROLE', 'STAGE'
MANUSCRIPT, PERSON_ROLE = 'MANUSCRIPT', 'PERSON_ROLE'
MANUSCRIPT_VERSION = 'MANUSCRIPT_VERSION'
MANUSCRIPT_STAGE = 'MANUSCRIPT_VERSION_STAGE'

ALL_KEYS = INSERTION_ORDER = [
    PERSON, COUNTRY, ROLE, STAGE, # leaves, no dependencies
    MANUSCRIPT, PERSON_ROLE, # depend on above
    MANUSCRIPT_VERSION, # depend on above
    MANUSCRIPT_STAGE, # etc
]

ALL_STAGING_KEYS = [
    COUNTRY, PERSON, PERSON_ROLE, MANUSCRIPT, MANUSCRIPT_VERSION, MANUSCRIPT_STAGE
]

STAGING_MODULE_BY_NAME = {
    ROLE: dimRole2,
    STAGE: dimStage2,
    
    COUNTRY: dimCountry2,
    PERSON: dimPerson2,
    PERSON_ROLE: dimPersonRole2,
    MANUSCRIPT: dimManuscript2,
    MANUSCRIPT_VERSION: dimManuscriptVersion2,
    MANUSCRIPT_STAGE: dimManuscriptVersionHistory2
}

FILE_PATTERN_BY_NAME = {
    COUNTRY: 'country',
    PERSON: 'persons',
    PERSON_ROLE: 'person_roles',
    MANUSCRIPT: 'manuscripts',
    MANUSCRIPT_VERSION: 'versions',
    MANUSCRIPT_STAGE: 'stages'
}


TIMESTAMP_PATTERN = r'(\d+).*'


StagingModule = Any
StagingParams = dict
StagingInstruction = Tuple[StagingModule, StagingParams]


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
    for staging_key in ALL_STAGING_KEYS:
        staging_filenames = filter_filenames_by_pattern(
            filenames, FILE_PATTERN_BY_NAME[staging_key]
        )
        for filename in staging_filenames:
            yield (
                STAGING_MODULE_BY_NAME[staging_key],
                {
                    'file_path': filename
                }
            )


def sort_filenames(filenames):
    return natsorted(filenames)


def extract_timestamp_from_filename(filename):
    m = re.match(TIMESTAMP_PATTERN, os.path.basename(filename))
    if not m:
        raise ValueError(
            'unrecognised filename pattern, missing timestamp: %s' % filename
        )
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
        staging_module.applyChanges(connection, source=None)


def process_filenames1(connection, filenames: List[str], is_batch_mode: bool):
    LOGGER.debug('filenames: %s', filenames)
    grouped_staging_instructions = filenames_to_grouped_staging_instruction(filenames)
    LOGGER.info(list(grouped_staging_instructions))
    # group
    for group_key, staging_instructions in grouped_staging_instructions:
        LOGGER.info('processing staging group: %s', group_key)
        process_staging_instructions_group(
            connection, staging_instructions, is_batch_mode=is_batch_mode
        )


def process_filenames2(_, filenames: List[str], is_batch_mode: bool):
    grouped_staging_instructions = filenames_to_grouped_staging_instruction(filenames)

    state = {}
    for timestamp, module_kwargs_pair_list in grouped_staging_instructions:
        # loads the csv files into the 'state' dict above
        # each module has a chance to do anything special it needs
        for module, kwargs in module_kwargs_pair_list:
            module.stage_csv(state, **kwargs)

        # process the loaded csv
        # extrapolate it into rows to insert/update
        for module, _ in module_kwargs_pair_list:
            module.applyChanges(state, source=None)

    # upsert our processed data
    # order is important
    for name in INSERTION_ORDER:
        module = STAGING_MODULE_BY_NAME[name]
        default_upsert = utils.insert_or_update
        upsertfn = getattr(module, 'upsert', default_upsert)
        upsert_rows = state.get(name, [])
        utils.doseq(upsertfn, upsert_rows)

    

#
#
#

process_filenames = process_filenames2

def process_source_dir(connection, source_dir: str, is_batch_mode: bool):
    LOGGER.info('importing data from: %s', source_dir)
    filenames = find_filenames_to_process(source_dir)
    process_filenames(connection, filenames, is_batch_mode=is_batch_mode)
