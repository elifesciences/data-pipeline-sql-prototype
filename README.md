# data-pipeline

A central place for resources relating to the eLife Data Pipeline tools

## Pre-requisites

* [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)

## CSV Generator

### Convert Example Data using Docker

```bash
docker-compose run --rm csv-generator --source-zip '/example-data/xml-set-1' --output-dir '/csv-data'
```

List converted files:

```bash
docker-compose run --rm --entrypoint 'sh -c' csv-generator 'ls -l /csv-data'
```

To view one of the generated CSV files:

```bash
docker-compose run --rm --entrypoint 'sh -c' csv-generator \
  'cat /csv-data/1514862245_manuscripts.csv'
```

To copy one of the generated CSV files:

```bash
mkdir -p ./output && docker-compose run --rm --entrypoint 'sh -c' csv-generator \
  'cat /csv-data/1514862245_manuscripts.csv | tr -d "\r"' > ./output/1514862245_manuscripts.csv
```

## DB Manager

### Staging Example

Convert dummy examples:

```bash
docker-compose run --rm db-manager python example_csv_import.py
```

Inspect results:

```bash
docker-compose exec db psql --user elife_ejp -c 'select * from dim.dimManuscriptVersion;'
```

## Airflow

Start up:

```bash
docker-compose up
```

Access the `airflow` admin console at http://localhost:8086/admin

Access the `celery-flower` console at http://localhost:55555
