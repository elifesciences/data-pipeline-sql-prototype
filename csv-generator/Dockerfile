FROM python:3.6-stretch

# pre-requisites
RUN apt-get update && \
    pip install -U pipenv

ARG PROJECT_HOME=/usr/local/csv-generator
ARG PROJECT_USER=csv-generator
WORKDIR ${PROJECT_HOME}

RUN useradd -s /bin/bash -d ${PROJECT_HOME} ${PROJECT_USER}

COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv install --system --deploy

COPY ./pytest.ini ./
COPY ./docker ./docker
COPY ./csv_generator ./csv_generator

# tests
COPY ./project_tests.sh ./
COPY ./tests ./tests

RUN chown -R ${PROJECT_USER}: .
RUN mkdir -p /xml-data && chown -R ${PROJECT_USER}: /xml-data
RUN mkdir -p /csv-data && chown -R ${PROJECT_USER}: /csv-data

USER ${PROJECT_USER}

VOLUME [ "/xml-data", "/csv-data" ]

ENTRYPOINT ["docker/entrypoint.sh"]
