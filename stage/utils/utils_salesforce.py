# Copyright 2020 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A module providing utils for working with Salesforce

import logging
import string
from datetime import datetime, timedelta
from io import BytesIO
from json import JSONDecodeError
from time import sleep
from zipfile import ZipFile

from streamsets.testframework.utils import get_random_string

CONTACT = 'Contact'
CDC = 'CDC'
PUSH_TOPIC = 'PUSH_TOPIC'
API_VERSION = '47.0'
COLON = ':'
PERIOD = '.'

logger = logging.getLogger(__name__)

ADD_CUSTOM_FIELD_PACKAGE = f'''<?xml version="1.0" encoding="UTF-8"?>
<Package xmlns="http://soap.sforce.com/2006/04/metadata">
    <types>
        <members>Contact</members>
        <name>CustomObject</name>
    </types>
    <types>
        <members>Admin</members>
        <name>Profile</name>
    </types>
    <version>{API_VERSION}</version>
</Package>'''

ADD_CUSTOM_FIELD = '''<?xml version="1.0" encoding="UTF-8"?>
<CustomObject xmlns="http://soap.sforce.com/2006/04/metadata">
    <fields>
        <fullName>BoolCustField__c</fullName>
        <defaultValue>false</defaultValue>
        <description>ThisIsABoolCustField</description>
        <externalId>false</externalId>
        <inlineHelpText>ThisIsABoolCustField</inlineHelpText>
        <label>BoolCustField</label>
        <trackTrending>false</trackTrending>
        <type>Checkbox</type>
    </fields>
</CustomObject>'''

CUSTOM_FIELD_PERMISSION = '''<?xml version="1.0" encoding="UTF-8"?>
<Profile xmlns="http://soap.sforce.com/2006/04/metadata">
  <fieldPermissions>
        <editable>true</editable>
        <field>Contact.BoolCustField__c</field>
        <readable>true</readable>
    </fieldPermissions>
</Profile>'''

DELETE_CUSTOM_FIELD_PACKAGE = f'''<?xml version="1.0" encoding="UTF-8"?>
<Package xmlns="http://soap.sforce.com/2006/04/metadata">
    <version>{API_VERSION}</version>
</Package>'''

DELETE_CUSTOM_FIELD = '''<?xml version="1.0" encoding="UTF-8"?>
<Package xmlns="http://soap.sforce.com/2006/04/metadata">
    <types>
        <members>Contact.BoolCustField__c</members>
        <name>CustomField</name>
    </types>
</Package>'''

# Folder for Documents
FOLDER_NAME = 'TestFolder'

CASE_SUBJECT = 'Test Case'

ACCOUNTS_FOR_SUBQUERY = 5
CONTACTS_FOR_SUBQUERY = 5

CONTACTS_FOR_NO_MORE_DATA = 100

TIMEOUT = 300

TEST_DATA = {}


def set_up_random(salesforce):
    """" This function is used to generate unique set of values for each test.
    Every time this function is used, generates a unique RANDOM string to
    set up the values used in every test."""

    TEST_DATA['STR_15_RANDOM'] = get_random_string(string.ascii_letters, 15)
    logger.info(f"STR_15_RANDOM : '{TEST_DATA['STR_15_RANDOM']}'")

    TEST_DATA['DATA_TO_INSERT'] = [
        {'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest1@example.com',
         'LeadSource': 'Advertisement'},
        {'FirstName': 'Test2', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest2@example.com',
         'LeadSource': 'Partner'},
        {'FirstName': 'Test3', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest3@example.com',
         'LeadSource': 'Web'}]

    # For testing of SDC-7548
    # Since email is used in WHERE clause in lookup processory query,
    # create data containing 'from' word in emails to verify the bug is fixed.
    TEST_DATA['DATA_WITH_FROM_IN_EMAIL'] = [{'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'],
                                             'Email': 'FROMxtest1@example.com', 'LeadSource': 'Advertisement'},
                                            {'FirstName': 'Test2', 'LastName': TEST_DATA['STR_15_RANDOM'],
                                             'Email': 'xtefromst2@example.com', 'LeadSource': 'Partner'},
                                            {'FirstName': 'Test3', 'LastName': TEST_DATA['STR_15_RANDOM'],
                                             'Email': 'xtes3@example.comFROM', 'LeadSource': 'Web'}]
    TEST_DATA['CSV_DATA_TO_INSERT'] = [','.join(TEST_DATA['DATA_TO_INSERT'][0].keys())] + [','.join(item.values()) for
                                                                                           item in
                                                                                           TEST_DATA['DATA_TO_INSERT']]

    # When the server has too many queries it returns temporary unavailable
    # This is sleep is to avoid that error
    sleep(10)
    # Log to know if limits are reached
    client = salesforce.client
    limits = client.limits()
    log_limits = f'Limits: DailyApiRequests: {limits["DailyApiRequests"]["Remaining"]}, ' \
                 f'DailyBulkApiRequests: {limits["DailyBulkApiRequests"]["Remaining"]}, ' \
                 f'DailyDurableStreamingApiEvents: {limits["DailyDurableStreamingApiEvents"]["Remaining"]}, ' \
                 f'HourlyPublishedStandardVolumePlatformEvents: ' \
                 f'{limits["HourlyPublishedStandardVolumePlatformEvents"]["Remaining"]}, ' \
                 f'MonthlyPlatformEvents: {limits["MonthlyPlatformEvents"]["Remaining"]} '

    logger.info(log_limits)


def get_dev_raw_data_source(pipeline_builder, raw_data):
    """Utility method to create a Dev Raw Data Origin containing delimited data

    Args:
        pipeline_builder (:py:class:`streamsets.sdk.sdc_models.PipelineBuilder`): Pipeline builder instance
        raw_data (obj:`list`): Data as a list of comma-separated values, including a header row

    Returns:
        The Dev Raw Data origin as a :py:class:`streamsets.sdk.sdc_models.Stage`
    """
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)
    return dev_raw_data_source


def _verify_wiretap_data(wiretap, expected_data, sort=True):
    """Utility method to verify that a wiretap matches the expected data

    Args:
        wiretap: wiretap containing data to be verified
        expected_data (obj:`list`): Expected data as a list of dicts
        sort (Boolean): Whether to sort or not before comparing

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    # SDC-10773 - source IDs must be unique
    source_ids = {record.header['sourceId'] for record in wiretap.output_records}
    assert len(source_ids) == len(wiretap.output_records)

    rows_from_wiretap = [record.field for record in wiretap.output_records]

    data_from_wiretap = [{field: record[field] for field in record if field not in ['Id', 'SystemModstamp']}
                         for record in rows_from_wiretap]

    if data_from_wiretap and sort:
        data_from_wiretap = sorted(data_from_wiretap,
                                   key=lambda k: k[
                                       'FirstName' if 'FirstName' in data_from_wiretap[0] else 'surName'].value)

    if data_from_wiretap:
        assert data_from_wiretap == expected_data


def _insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, expected_data, salesforce, data_to_insert, sort=True):
    """Utility method to insert data into Salesforce, start a pipeline, record a wiretap, verify that the data
    in the wiretap matches the expected data, and clean up the inserted records.

    Args:
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        pipeline (:py:class:`streamsets.sdk.sdc_models.Pipeline`): Pipeline instance
        wiretap: Wiretap after which data is to be verified
        expected_data (obj:`list`): Expected data as a list of dicts
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        data_to_insert (obj:`list`): Data to be inserted, as a list of dicts
        sort (Boolean): Whether to sort or not before comparing

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = get_ids(client.bulk.Contact.insert(data_to_insert), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        _verify_wiretap_data(wiretap, expected_data, sort)

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids)


def get_ids(records, key):
    """Utility method to extract list of Ids from Bulk API insert/query result.

    Args:
        records (:obj:`list`): List of records from a Bulk API insert or SOQL query.
        key (:obj:`str`): Key to extract - 'Id' for queries or 'id' for inserted data.

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    return [{'Id': record[key]}
            for record in records]


def clean_up(sdc_executor, pipeline, client, contact_ids):
    """Utility method to delete inserted contacts and stop the pipeline

    Args:
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        pipeline (:py:class:`streamsets.sdk.sdc_models.Pipeline`): Pipeline instance to be stopped
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client
        contact_ids (:obj:`list`): List of contacts to be deleted in form [{'Id':'001000000000001'},...]
    """
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        logger.info('Stopping pipeline')
        try:
            #Wait false is because no sincronization needed with the stop
            sdc_executor.stop_pipeline(pipeline, wait=False)
        except Exception:
            logger.error('Unable to stop the pipeline...')
    try:
        if contact_ids:
            logger.info('Deleting records ...')
            client.bulk.Contact.delete(contact_ids)
    except Exception:
        logger.error('Unable to delete records...')


def find_dataset(client, name):
    """Utility method to find a dataset by name

    Args:
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client
        name (:obj:`str`): Dataset name

    Returns:
        (:obj:`str`) Record ID of dataset
        (:obj:`str`) Current Version ID of dataset
    """
    result = client.restful('wave/datasets')
    for dataset in result['datasets']:
        if dataset['name'] == name and 'currentVersionId' in dataset:
            return dataset['id'], dataset['currentVersionId']

    return None, None


def find_dataset_include_timestamp(client, name):
    """Utility method to find a dataset by name when including a timestamp in the name
    """
    result = client.restful('wave/datasets')
    for dataset in result['datasets']:
        if dataset['name'].startswith(name) and 'currentVersionId' in dataset:
            return dataset['id'], dataset['currentVersionId']

    return None, None


def verify_cdc_wiretap(wiretap, inserted_data):
    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].header.values['salesforce.cdc.recordIds']
    assert wiretap.output_records[0].field['Email'] == inserted_data['Email']
    # CDC returns nested compound fields
    assert wiretap.output_records[0].field['Name']['FirstName'] == inserted_data['FirstName']
    assert wiretap.output_records[0].field['Name']['LastName'] == inserted_data['LastName']


def add_custom_field_to_contact(metadata):
    deploy_metadata(metadata,
                    ADD_CUSTOM_FIELD_PACKAGE,
                    [{'name': 'objects/Contact.object', 'content': ADD_CUSTOM_FIELD},
                      {'name': 'profiles/Admin.profile', 'content': CUSTOM_FIELD_PERMISSION}])


def delete_custom_field_from_contact(metadata):
    deploy_metadata(metadata,
                    DELETE_CUSTOM_FIELD_PACKAGE,
                    [{'name': 'destructiveChanges.xml', 'content': DELETE_CUSTOM_FIELD}])


def deploy_metadata(metadata, package_content, files):
    file_bytes = BytesIO()

    with ZipFile(file_bytes, 'w') as zip_file:
        zip_file.writestr('package.xml', package_content)
        for file in files:
            zip_file.writestr(file['name'], file['content'])
        zip_file.close()

    deployment = metadata.deploy(file_bytes, {})

    result = None
    end_time = datetime.now() + timedelta(seconds=60)
    while result != 'Succeeded' and result != 'Failed' and datetime.now() < end_time:
        sleep(1)
        result = metadata.check_deploy_status(deployment[0])[0]

    logger.info(f'Deployment {result}')

    assert result == 'Succeeded'


def enable_cdc(client):
    """Utility method to enable Change Data Capture for Contact change events in an org.

    Args:
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client

    Returns:
        (:obj:`str`) Event channel Id
    """
    payload = {
        "FullName": "ChangeEvents_ContactChangeEvent",
        "Metadata": {
            "eventChannel": "ChangeEvents",
            "selectedEntity": "ContactChangeEvent"
        }
    }
    result = client.restful('tooling/sobjects/PlatformEventChannelMember', method='POST', json=payload)
    assert result['success']
    return result['id']


def disable_cdc(client, subscription_id):
    """Utility method to disable Change Data Capture for Contact change events in an org.

    Args:
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client
        subscription_id (:obj:`str`) Event channel Id
    """
    try:
        client.restful(f'tooling/sobjects/PlatformEventChannelMember/{subscription_id}', method='DELETE')
    except JSONDecodeError:
        # Simple Salesforce issue #327
        # https://github.com/simple-salesforce/simple-salesforce/issues/327
        pass


def create_push_topic(client):
    """Utility method to create a PushTopic to subscribe to Contact change events.

    Args:
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client

    Returns:
        (:obj:`str`) PushTopic record Id
        (:obj:`str`) PushTopic name
    """
    push_topic_name = TEST_DATA['STR_15_RANDOM']
    logger.info(f'Creating PushTopic {push_topic_name} in Salesforce')
    result = client.PushTopic.create({'Name': push_topic_name,
                                      'Query': 'SELECT Id, FirstName, LastName, Email, LeadSource FROM Contact',
                                      'ApiVersion': '47.0',
                                      'NotifyForOperationCreate': True,
                                      'NotifyForOperationUpdate': True,
                                      'NotifyForOperationUndelete': True,
                                      'NotifyForOperationDelete': True,
                                      'NotifyForFields': 'All'})
    return result['id'], push_topic_name