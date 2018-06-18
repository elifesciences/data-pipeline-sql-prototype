from contextlib import contextmanager
import os
import shutil

import pytest

from consumers.consumer import BaseXMLConsumer
from consumers.manuscript_consumer import ManuscriptXMLConsumer
from consumers.stage_consumer import StageXMLConsumer
from consumers.version_consumer import VersionXMLConsumer


CREATE_DATE = '1526868166'
OUTPUT_DIR = 'test_output'
ZIP_FILE_NAME = 'test_file.zip'


@contextmanager
def temp_dir():
    if not os.path.isdir(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
    yield
    shutil.rmtree(OUTPUT_DIR)


def _create_consumer(consumer_cls):
    with temp_dir():
        return consumer_cls(CREATE_DATE, ZIP_FILE_NAME, OUTPUT_DIR)


@pytest.fixture
def base_consumer():
    return _create_consumer(BaseXMLConsumer)


@pytest.fixture
def manuscript_consumer():
    return _create_consumer(ManuscriptXMLConsumer)


@pytest.fixture
def stage_consumer():
    return _create_consumer(StageXMLConsumer)


@pytest.fixture
def version_consumer():
    return _create_consumer(VersionXMLConsumer)


@pytest.fixture
def go_xml():
    return '''
    <?xml version="1.0" encoding="UTF-8"?>
    <file_list create_date="2018-05-21 02:02:46">
      <file_nm>19-09-2017-ISRA-eLife-32152.xml</file_nm>
      <file_nm>25-08-2017-RA-ELIFE-31557.xml</file_nm>
      <file_nm>24-10-2017-RA-eLife-33092.xml</file_nm>
      <file_nm>25-10-2017-RA-eLife-33123.xml</file_nm>
      <file_nm>25-10-2017-RA-eLife-33099.xml</file_nm>
      <file_nm>07-11-2017-RA-eLife-33423.xml</file_nm>
      <file_nm>06-12-2017-RA-eLife-34152.xml</file_nm>
      <file_nm>18-12-2017-RA-eLife-34465.xml</file_nm>
      <file_nm>21-12-2017-RA-eLife-34586.xml</file_nm>
    </file_list>
    '''


@pytest.fixture
def manuscript_xml():
    return '''
    <?xml version="1.0" encoding="UTF-8"?>
    <xml>
      <manuscript>
        <country>United States</country>
        <production-data>
          <production-data-doi>10.7554/eLife.33099</production-data-doi>
        </production-data>
        <version>
          <manuscript-number>25-10-2017-RA-eLife-33099</manuscript-number>
        </version>
      </manuscript>
    </xml>
    '''


@pytest.fixture
def stages_xml():
    return '''
    <?xml version="1.0" encoding="UTF-8"?>
    <xml>
      <manuscript>
        <country>United States</country>
        <production-data>
          <production-data-doi>10.7554/eLife.33099</production-data-doi>
        </production-data>
        <version>
          <decision>Revise Full Submission</decision>
          <manuscript-number>25-10-2017-RA-eLife-33099</manuscript-number>
          <manuscript-type>Research Article</manuscript-type>
          <history>
            <stage>
              <stage-affective-person-id>7733</stage-affective-person-id>
              <stage-name>Preliminary Manuscript Data Submitted</stage-name>
              <stage-triggered-by-person-id>7733</stage-triggered-by-person-id>
              <start-date>2nd May 18  23:32:47</start-date>
            </stage>
            <stage>
              <stage-affective-person-id>7733</stage-affective-person-id>
              <stage-name>Author Approved Converted Files</stage-name>
              <stage-triggered-by-person-id>7733</stage-triggered-by-person-id>
              <start-date>3rd May 18  00:17:06</start-date>
            </stage>
            <stage>
              <stage-affective-person-id>7733</stage-affective-person-id>
              <stage-name>Senior Editor Assigned</stage-name>
              <stage-triggered-by-person-id>7733</stage-triggered-by-person-id>
              <start-date>3rd May 18  00:17:06</start-date>
            </stage>
            <stage>
              <stage-affective-person-id>7733</stage-affective-person-id>
              <stage-name>Initial QC Started</stage-name>
              <stage-triggered-by-person-id>7733</stage-triggered-by-person-id>
              <start-date>3rd May 18  00:17:06</start-date>
            </stage>
          </history>
        </version>
        <version>
          <decision>Accept Full Submission</decision>
          <manuscript-number>25-10-2017-RA-eLife-33099</manuscript-number>
          <manuscript-type>Research Article</manuscript-type>
          <history>
            <stage>
              <stage-affective-person-id>7733</stage-affective-person-id>
              <stage-name>Preliminary Manuscript Data Submitted</stage-name>
              <stage-triggered-by-person-id>7733</stage-triggered-by-person-id>
              <start-date>2nd May 18  23:32:47</start-date>
            </stage>
            <stage>
              <stage-affective-person-id>7733</stage-affective-person-id>
              <stage-name>Author Approved Converted Files</stage-name>
              <stage-triggered-by-person-id>7733</stage-triggered-by-person-id>
              <start-date>3rd May 18  00:17:06</start-date>
            </stage>
          </history>
        </version>
      </manuscript>
    </xml>
    '''


@pytest.fixture
def versions_xml():
    return '''
    <?xml version="1.0" encoding="UTF-8"?>
    <xml>
      <manuscript>
        <country>United States</country>
        <production-data>
          <production-data-doi>10.7554/eLife.33099</production-data-doi>
        </production-data>
        <version>
          <decision>Revise Full Submission</decision>
          <manuscript-number>25-10-2017-RA-eLife-33099</manuscript-number>
          <manuscript-type>Research Article</manuscript-type>
        </version>
        <version>
          <decision>Accept Full Submission</decision>
          <manuscript-number>25-10-2017-RA-eLife-33099</manuscript-number>
          <manuscript-type>Research Article</manuscript-type>
        </version>
      </manuscript>
    </xml>
    '''
