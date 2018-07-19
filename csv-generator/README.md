## CSV Data Generator

Provides the ability to consume `xml` data files, extract required values and generate aggrogated `csv` output files. 

#### Prerequisites

- [Python](https://www.python.org/) >= 3.5
- [pipenv](https://github.com/pypa/pipenv)

#### Installation

Whilst in `csv_generator` directory:

`$ pipenv install --three`

or specify a version:

`$ pipenv install --python 3.6`

#### Input data

For `process_xml_zip.py` the following zip file layout is expected:

```
- some_file.zip
    - some_manuscript_data_file_1.xml
    - some_manuscript_data_file_2.xml
    - some_manuscript_data_file_3.xml
    - some_manuscript_data_file_4.xml
    ...
    - go.xml (should contain `file_list` and `create_date`)
```   

Example `go.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<file_list create_date="2018-05-21 02:02:46">
  <file_nm>some_manuscript_data_file_1.xml</file_nm>
  <file_nm>some_manuscript_data_file_2.xml</file_nm>
  <file_nm>some_manuscript_data_file_3.xml</file_nm>
  <file_nm>some_manuscript_data_file_4.xml</file_nm>
</file_list>
```

#### Usage

##### process_xml_zip

Whilst in `csv_generator` directory:

`$ pipenv run python -m csv_generator.process_xml_zip --source-zip '../some_file.zip' --output-dir '../some_dir'`

`--source-zip`: Your source `zip` file.

`--output-dir`: Where you want your output `csv` files. Defaults to current directory.


##### article_pub_dates

Whilst in `csv_generator` directory:

`$ pipenv run python -m csv_generator.article_pub_dates --output-dir '../some_dir'`

`--output-dir`: Where you want your output `csv` files. Defaults to current directory.

##### reviewer_sharing_name

Whilst in `csv_generator` directory:

`$ pipenv run python -m csv_generator.reviewer_sharing_name --source-file 'some_file.csv' --output-dir '../some_dir'`

`--source-zip`: Your source `csv` file.

`--output-dir`: Where you want your output `csv` files. Defaults to current directory.

#### Tests

Whilst in `csv_generator` directory:

`$ pipenv install --dev --three`

`$ pipenv run python -m pytest`