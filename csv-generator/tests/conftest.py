from contextlib import contextmanager
import logging
import os
import shutil

import pytest

from csv_generator.consumers.consumer import BaseXMLConsumer
from csv_generator.consumers.manuscript_consumer import ManuscriptXMLConsumer
from csv_generator.consumers.person_consumer import PersonXMLConsumer
from csv_generator.consumers.person_address_consumer import PersonAddressXMLConsumer
from csv_generator.consumers.person_role_consumer import PersonRoleXMLConsumer
from csv_generator.consumers.stage_consumer import StageXMLConsumer
from csv_generator.consumers.version_consumer import VersionXMLConsumer


CREATE_DATE = '1526868166'
OUTPUT_DIR = 'test_output'
ZIP_FILE_NAME = 'test_file.zip'


@pytest.fixture(autouse=True)
def configure_logging():
    logging.root.handlers = []
    logging.basicConfig(level='DEBUG')


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
def person_consumer():
    return _create_consumer(PersonXMLConsumer)


@pytest.fixture
def person_address_consumer():
    return _create_consumer(PersonAddressXMLConsumer)


@pytest.fixture
def person_role_consumer():
    return _create_consumer(PersonRoleXMLConsumer)


@pytest.fixture
def stage_consumer():
    return _create_consumer(StageXMLConsumer)


@pytest.fixture
def version_consumer():
    return _create_consumer(VersionXMLConsumer)


@pytest.fixture
def go_xml():
    return '''
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
def manuscript_appeal_xml():
    return '''
    <xml>
      <manuscript>
        <country>United States</country>
        <production-data>
          <production-data-doi>10.7554/eLife.33099</production-data-doi>
        </production-data>
        <version>
          <manuscript-number>01-02-2017-RA-eLife-12345R1-Appeal</manuscript-number>
        </version>
      </manuscript>
    </xml>
    '''


@pytest.fixture
def persons_xml():
    return '''
    <xml>
      <people>
        <person>
          <addresses>
            <address>
              <address-city>Chicago</address-city>
              <address-country>United States</address-country>
              <address-department>Biological Sciences Division</address-department>
              <address-end-date></address-end-date>
              <address-start-date>2nd Mar 18  11:48:09</address-start-date>
              <address-state-province></address-state-province>
              <address-street-address-1></address-street-address-1>
              <address-street-address-2></address-street-address-2>
              <address-type>Primary Work</address-type>
              <address-zip-postal-code></address-zip-postal-code>
            </address>
          </addresses>
          <create-date></create-date>
          <email>user1@fake.com</email>
          <first-name>User</first-name>
          <institution>University of Fake</institution>
          <last-name>One</last-name>
          <middle-name>Test</middle-name>
          <person-id>1099</person-id>
          <profile-display-date>2nd Mar 18  11:54:01</profile-display-date>
          <profile-modify-date>2nd Mar 18  11:54:02</profile-modify-date>
          <roles>
            <role>
              <role-type>Reviewing Editor</role-type>
            </role>
            <role>
              <role-type>Board of Reviewing Editors</role-type>
            </role>
          </roles>
          <secondary_email></secondary_email>
          <status>Active</status>
          <telephone>000 000 0000</telephone>
          <title></title>
        </person>
        <person>
          <addresses>
            <address>
              <address-city>Lausanne</address-city>
              <address-country>Switzerland</address-country>
              <address-department>Department of Plant Molecular Biology</address-department>
              <address-end-date></address-end-date>
              <address-start-date>18th Jun 12  13:22:21</address-start-date>
              <address-state-province></address-state-province>
              <address-street-address-1></address-street-address-1>
              <address-street-address-2></address-street-address-2>
              <address-type>Primary Work</address-type>
              <address-zip-postal-code></address-zip-postal-code>
            </address>
          </addresses>
          <area-of-interest-keyword></area-of-interest-keyword>
          <create-date></create-date>
          <email>user2@fake.com</email>
          <fax></fax>
          <first-name>User</first-name>
          <institution>University of Fake2</institution>
          <last-name>Two</last-name>
          <middle-name>S.</middle-name>
          <person-id>1009</person-id>
          <profile-display-date>2nd Jun 15  15:53:15</profile-display-date>
          <profile-modify-date>17th May 18  09:30:51</profile-modify-date>
          <roles>
            <role>
              <role-type>Senior Editor</role-type>
            </role>
          </roles>
          <secondary_email></secondary_email>
          <status>Active</status>
          <telephone></telephone>
          <title>Prof.</title>
        </person>
      </people>
    </xml>
    '''


@pytest.fixture
def stages_xml():
    return '''
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
          <editors>
            <editor>
              <editor-assigned-date>25th May 18  18:15:30</editor-assigned-date>
              <editor-decision></editor-decision>
              <editor-decision-date></editor-decision-date>
              <editor-decision-due-date></editor-decision-due-date>
              <editor-person-id>1132</editor-person-id>
            </editor>
          </editors>
          <senior-editors>
            <senior-editor>
              <senior-editor-assigned-date>14th May 18  14:20:16</senior-editor-assigned-date>
              <senior-editor-person-id>1122</senior-editor-person-id>
            </senior-editor>
          </senior-editors>
        </version>
        <version>
          <decision>Accept Full Submission</decision>
          <manuscript-number>25-10-2017-RA-eLife-33099</manuscript-number>
          <manuscript-type>Research Article</manuscript-type>
          <editors>
            <editor>
              <editor-assigned-date>25th May 18  18:15:30</editor-assigned-date>
              <editor-decision></editor-decision>
              <editor-decision-date></editor-decision-date>
              <editor-decision-due-date></editor-decision-due-date>
              <editor-person-id>1132</editor-person-id>
            </editor>
          </editors>
          <senior-editors>
            <senior-editor>
              <senior-editor-assigned-date>17th May 17  14:22:16</senior-editor-assigned-date>
              <senior-editor-person-id>1109</senior-editor-person-id>
            </senior-editor>
          </senior-editors>
        </version>
      </manuscript>
    </xml>
    '''
