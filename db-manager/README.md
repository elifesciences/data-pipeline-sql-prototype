# DB Manager

Manages the import and update of data provided as CSV.

## Prerequisites

- [Python](https://www.python.org/) >= 3.5
- [pipenv](https://github.com/pypa/pipenv)

## Installation

Whilst in the `db-manager` directory:

```bash
pipenv install --three
```

or specify a version:

```bash
pipenv install --python 3.6
```

## Initialise Database

```bash
pipenv run python -m db_manager teardown &&
pipenv run python -m db_manager create
```

## Stage and Import Example

```bash
pipenv run python -m db_manager import-data --source-dir ./dummy_csv
```

## Tests

```bash
pipenv install --dev --three
```

```bash
pipenv run pytest
```

```bash
pipenv run pytest-watch
```

## Documentation

See [data import](doc/data-import.md) for a more detailed documentation of the steps involved when importing data.
