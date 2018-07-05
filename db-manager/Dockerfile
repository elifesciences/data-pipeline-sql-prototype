FROM python:3.6-stretch

# pre-requisites
RUN apt-get update && \
    pip install -U pipenv

ARG PROJECT_HOME=/usr/local/db-manager
ARG PROJECT_USER=db-manager
WORKDIR ${PROJECT_HOME}

RUN useradd -s /bin/bash -d ${PROJECT_HOME} ${PROJECT_USER}

COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv install --system --deploy

COPY . ./

RUN chown -R ${PROJECT_USER}: .
RUN mkdir -p /csv-data && chown -R ${PROJECT_USER}: /csv-data

VOLUME [ "/csv-data" ]

USER ${PROJECT_USER}
