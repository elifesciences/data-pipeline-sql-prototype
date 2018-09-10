from csv_generator.consumers import (
  get_source_file_name
)


class TestGetSourceFileName:
    def test_should_return_combine_source_file_with_zip_file_if_present(self):
        assert get_source_file_name('foo.zip', 'foo.xml') == 'foo.zip/foo.xml'

    def test_should_return_source_file_only_if_zip_file_is_none(self):
        assert get_source_file_name(None, 'foo.xml') == 'foo.xml'
