import logging
from unittest.mock import Mock, MagicMock, patch, call

import pytest

from db_manager import (
    dimCountry,
    dimPerson,
    dimPersonRole,
    dimManuscript,
    dimManuscriptVersion,
    dimManuscriptVersionHistory
)

from db_manager import processing as processing_module
from db_manager.processing import (
    group_filenames_to_staging_instruction,
    filenames_to_grouped_staging_instruction,
    process_source_dir,
    process_filenames,
    process_staging_instructions_group
)


LOGGER = logging.getLogger(__name__)


COUNTRY_CSV_FILENAME_SUFFIX = '_country_relabel.csv'
COUNTRY_CSV_FILENAME = '123_%s' % COUNTRY_CSV_FILENAME_SUFFIX

MANUSCRIPT_CSV_FILENAME = '123_manuscripts.csv'
MANUSCRIPT_VERSION_CSV_FILENAME = '123_versions.csv'
MANUSCRIPT_STAGES_CSV_FILENAME = '123_stages.csv'

PERSON_CSV_FILENAME = '123_persons.csv'
PERSON_ROLES_CSV_FILENAME = '123_person_roles.csv'

SOURCE_DIR = '/csv-data'


@pytest.fixture(name='connection_mock')
def _connection():
    yield MagicMock()


@pytest.fixture(name='find_filenames_to_process_mock')
def _find_filenames_to_process():
    with patch.object(processing_module, 'find_filenames_to_process') as mock:
        yield mock


@pytest.fixture(name='process_filenames_mock')
def _process_filenames():
    with patch.object(processing_module, 'process_filenames') as mock:
        yield mock


@pytest.fixture(name='filenames_to_grouped_staging_instruction_mock')
def _filenames_to_grouped_staging_instruction():
    with patch.object(processing_module, 'filenames_to_grouped_staging_instruction') as mock:
        yield mock


@pytest.fixture(name='process_staging_instructions_group_mock')
def _process_staging_instructions_group():
    with patch.object(processing_module, 'process_staging_instructions_group') as mock:
        yield mock


class TestGroupedFilenamesToStagingInstructions:
    @pytest.mark.parametrize('csv_filename, expected_staging_module', [
        (COUNTRY_CSV_FILENAME, dimCountry),
        (PERSON_CSV_FILENAME, dimPerson),
        (PERSON_ROLES_CSV_FILENAME, dimPersonRole),
        (MANUSCRIPT_CSV_FILENAME, dimManuscript),
        (MANUSCRIPT_VERSION_CSV_FILENAME, dimManuscriptVersion),
        (MANUSCRIPT_STAGES_CSV_FILENAME, dimManuscriptVersionHistory)
    ])
    def test_should_match_csv_filename_to_module(self, csv_filename, expected_staging_module):
        assert list(group_filenames_to_staging_instruction([
            csv_filename
        ])) == [(
            expected_staging_module,
            {
                'file_path': csv_filename
            }
        )]


class TestFilenamesToGroupedStagingInstruction:
    def test_should_group_filenames_with_same_timestamp_in_same_group(self):
        filenames = [
            '11%s' % COUNTRY_CSV_FILENAME_SUFFIX.replace('.csv', '_a.csv'),
            '11%s' % COUNTRY_CSV_FILENAME_SUFFIX.replace('.csv', '_b.csv')
        ]
        assert [
            (g, [params.get('file_path') for _, params in staging_instructions])
            for g, staging_instructions in filenames_to_grouped_staging_instruction(
                filenames
            )
        ] == [('11', filenames)]


    def test_should_sort_and_group_filenames_with_different_timestamp_in_separate_groups(self):
        filenames = [
            '11%s' % COUNTRY_CSV_FILENAME_SUFFIX,
            '2%s' % COUNTRY_CSV_FILENAME_SUFFIX
        ]
        assert [
            (g, [params.get('file_path') for _, params in staging_instructions])
            for g, staging_instructions in filenames_to_grouped_staging_instruction(
                filenames
            )
        ] == [
            ('2', [filenames[1]]),
            ('11', [filenames[0]])
        ]


    def test_should_group_filenames_in_a_sub_directory(self):
        filenames = [
            '../path/11%s' % COUNTRY_CSV_FILENAME_SUFFIX.replace('.csv', '_a.csv'),
            '../path/11%s' % COUNTRY_CSV_FILENAME_SUFFIX.replace('.csv', '_b.csv')
        ]
        assert [
            (g, [params.get('file_path') for _, params in staging_instructions])
            for g, staging_instructions in filenames_to_grouped_staging_instruction(
                filenames
            )
        ] == [('11', filenames)]


class TestProcessSourceDir:
    def test_should_call_process_filenames(
        self, connection_mock, find_filenames_to_process_mock, process_filenames_mock):

        process_source_dir(connection_mock, SOURCE_DIR, is_batch_mode=False)
        find_filenames_to_process_mock.assert_called_with(SOURCE_DIR)
        process_filenames_mock.assert_called_with(
            connection_mock, find_filenames_to_process_mock.return_value, is_batch_mode=False
        )


class TestProcessSourceFilenames:
    def test_should_call_filenames_to_grouped_staging_instruction(
        self, connection_mock, filenames_to_grouped_staging_instruction_mock):

        process_filenames(connection_mock, [COUNTRY_CSV_FILENAME], is_batch_mode=False)
        filenames_to_grouped_staging_instruction_mock.assert_called_with([COUNTRY_CSV_FILENAME])


    def test_should_call_process_staging_instructions_group(
        self, connection_mock, filenames_to_grouped_staging_instruction_mock,
        process_staging_instructions_group_mock):

        staging_instructions = [MagicMock()]
        grouped_staging_instructions = [('t1', staging_instructions)]
        filenames_to_grouped_staging_instruction_mock.return_value = grouped_staging_instructions
        process_filenames(connection_mock, [COUNTRY_CSV_FILENAME], is_batch_mode=False)
        process_staging_instructions_group_mock.assert_called_with(
            connection_mock,
            staging_instructions,
            is_batch_mode=False
        )


class TestProcessStagingInstructionsGroup:
    def test_should_stage_and_apply_single_staging_instruction(self, connection_mock):
        staging_module = MagicMock()
        staging_module.__name__ = 'staging_module'
        staging_params = {'file_path': COUNTRY_CSV_FILENAME}
        staging_instructions = [(staging_module, staging_params)]
        process_staging_instructions_group(
            connection_mock, staging_instructions, is_batch_mode=False
        )
        staging_module.stage_csv.assert_called_with(connection_mock, **staging_params)
        staging_module.applyChanges.assert_called_with(connection_mock, source=None)


    def test_should_apply_changes_after_staging_all_files_in_group(self, connection_mock):
        m = MagicMock()
        m.staging_module_0.__name__ = 'staging_module_0'
        m.staging_module_1.__name__ = 'staging_module_1'
        staging_params_list = [{'file_path': 'file0.csv'}, {'file_path': 'file1.csv'}]
        staging_instructions = [
            (m.staging_module_0, staging_params_list[0]),
            (m.staging_module_1, staging_params_list[1])
        ]
        process_staging_instructions_group(
            connection_mock, staging_instructions, is_batch_mode=False
        )
        expected_calls = [
            call.staging_module_0.stage_csv(connection_mock, **staging_params_list[0]),
            call.staging_module_1.stage_csv(connection_mock, **staging_params_list[1]),
            call.staging_module_0.applyChanges(connection_mock, source=None),
            call.staging_module_1.applyChanges(connection_mock, source=None)
        ]
        LOGGER.debug('mock_calls: %s', m.mock_calls)
        LOGGER.debug('expected_calls: %s', expected_calls)
        assert m.mock_calls == expected_calls
