# Data Pipeline Airflow DAG

## Prerequisites

- [Python](https://www.python.org/) >= 3.5
- [pipenv](https://github.com/pypa/pipenv)

## Installation

Run:

```bash
./install-dev.sh
```

That will also install the dev dependencies as well as the [CSV Generator](../csv-generator) and [DB Manager](../db-manager) from source.

## Tests

```bash
pipenv run python -m pytest
```
