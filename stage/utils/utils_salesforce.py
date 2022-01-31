# Copyright 2021 StreamSets Inc.
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
import requests
import string
from datetime import datetime, timedelta
from io import BytesIO
from json import JSONDecodeError
from operator import itemgetter
from string import Template
from time import sleep
from zipfile import ZipFile

from streamsets.sdk.utils import (DEFAULT_TIME_BETWEEN_CHECKS as DEFAULT_WAIT_TIME_BETWEEN_CHECKS,
                                  DEFAULT_TIMEOUT as DEFAULT_WAIT_TIMEOUT, wait_for_condition)
from streamsets.testframework.utils import get_random_string
from streamsets.testframework.environments.salesforce import API_VERSION

CONTACT = 'Contact'
CDC = 'CDC'
PUSH_TOPIC = 'PUSH_TOPIC'
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
        <fullName>$CUSTOM_FIELD</fullName>
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
        <field>Contact.$CUSTOM_FIELD</field>
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
        <members>Contact.$CUSTOM_FIELD</members>
        <name>CustomField</name>
    </types>
</Package>'''

# Template objects
ADD_CUSTOM_FIELD_TEMPLATE = Template(ADD_CUSTOM_FIELD)
CUSTOM_FIELD_PERMISSION_TEMPLATE = Template(CUSTOM_FIELD_PERMISSION)
DELETE_CUSTOM_FIELD_TEMPLATE = Template(DELETE_CUSTOM_FIELD)

# Folder for Documents
FOLDER_NAME = 'TestFolder'

CASE_SUBJECT = 'Test Case'

ACCOUNTS_FOR_SUBQUERY = 5
CONTACTS_FOR_SUBQUERY = 5

CONTACTS_FOR_NO_MORE_DATA = 100

TIMEOUT = 300

TEST_DATA = {}

MULTIPLE_UPLOADS_BATCH_SIZE = 1000

MULTIPLE_UPLOADS_PER_BATCH_SCRIPT = """# Script to test a batch that spans multiple uploads in Salesforce Analytics destination
try:
    sdc.importLock()
    import math
    import sys
    import time
finally:
    sdc.importUnlock()

entityName = ''
offset = 0
prefix = ''

cur_batch = sdc.createBatch()

# We want a ~25 MB batch to generate three uploads, so do the math...
# Note:
#   Maximum number of fields in a dataset: 5,000
#   Maximum number of characters in a field without creating metadata: 255
# Limits are documented at https://help.salesforce.com/s/articleView?id=sf.bi_limits.htm&type=5

# With the default batch size of 1000 records, record_size is 26,214.4 bytes
record_size = float(25 * 1024 * 1024) / float(sdc.batchSize)
field_size  = 255
# With the default batch size, field_count is 103, giving an actual batch size of 103 * 255 * 1000 ~= 25MB
field_count = int(math.ceil(record_size / field_size))

hasNext = True
while hasNext:
    try:
        record = sdc.createRecord('record offset ' + str(offset))

        record.value = sdc.createMap(True)
        for f in range(field_count):
            # Need to subtract width of numeric prefix (3) from field_size
            record.value['field' + str(f).zfill(3)] = str(offset).zfill(3) + ("x" * (field_size - 3))

        cur_batch.add(record)

        # if the batch is full, process it and end the script
        if cur_batch.size() >= sdc.batchSize:
            # blocks until all records are written to all destinations
            # (or failure) and updates offset
            # in accordance with delivery guarantee
            cur_batch.process(entityName, str(offset))
            hasNext = False

        offset = offset + 1

    except Exception as e:
        cur_batch.addError(record, str(e))
        cur_batch.process(entityName, str(offset))
        hasNext = False
"""

FORCE_13 = 'FORCE_13'

# Bulk API pipelines can take a minute or two just to process the query
BULK_PIPELINE_TIMEOUT_SECONDS = 300
SOAP_PIPELINE_TIMEOUT_SECONDS = 60

USERINFO_URL_FORMAT = "https://{}/services/oauth2/userinfo"

STANDARD_FIELDS = {'label': 'Value', 'fullName': 'Value__c', 'required': False}

SAMPLE_EMAIL = 'user@example.com'
SAMPLE_TEXT  = 'The quick brown fox jumps over the lazy dog'
MASKED_TEXT  = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
SAMPLE_PHONE = '111-222-3333'
LONG_SAMPLE_TEXT = SAMPLE_TEXT * 50
SAMPLE_URL   = 'https://www.example.com/'
SAMPLE_HTML  = 'The <i>quick</i> brown fox <u>jumps</u> over the <b>lazy</b> dog'
SAMPLE_LOCATION = {'Value__Latitude__s': 12.345, 'Value__Longitude__s': 54.321}
# Decimals appear in field._data['value'] as strings
EXPECTED_LOCATION = {'Value__Latitude__s': '12.345', 'Value__Longitude__s': '54.321'}
NULL_LOCATION = {'Value__Latitude__s': None, 'Value__Longitude__s': None}

# https://developer.salesforce.com/docs/atlas.en-us.api_meta.meta/api_meta/meta_field_types.htm#meta_type_fieldtype
DATA_TYPES = [
    {
        'metadata': {'type': 'AutoNumber', 'startingNumber': 1, 'displayFormat': 'A-{0000}'},
        'expected_type': 'STRING',
        'expected_value': 'A-0001',
        'null_value': 'A-0002'
    },
    {
        'metadata': {'type': 'Checkbox', 'defaultValue': False},
        'data_to_insert': True,
        'expected_type': 'BOOLEAN',
        'expected_value': True,
        'null_value': False
    },
    {
        'metadata': {'type': 'Currency', 'precision': 5, 'scale': 2},
        'data_to_insert': 123.45,
        'expected_type': 'DECIMAL',
        'expected_value': '123.45',
    },
    {
        'metadata': {'type': 'Date'},
        'data_to_insert': '2022-02-14',
        'expected_type': 'DATETIME',
        'expected_value': 1644796800000,
    },
    {
        'metadata': {'type': 'DateTime'},
        'data_to_insert': '2022-02-14T12:34:56Z',
        'expected_type': 'DATETIME',
        'expected_value': 1644842096000,
    },
    {
        'metadata': {'type': 'Email'},
        'data_to_insert': SAMPLE_EMAIL,
        'expected_type': 'STRING',
        'expected_value': SAMPLE_EMAIL,
    },
    {
        'metadata': {'type': 'EncryptedText', 'length': 64, 'maskChar': 'X', 'maskType': 'all'},
        'data_to_insert': SAMPLE_TEXT,
        'expected_type': 'STRING',
        'expected_value': MASKED_TEXT,
    },
    {
        'metadata': {'type': 'Number', 'precision': 5, 'scale': 2},
        'data_to_insert': 123.45,
        'expected_type': 'DECIMAL',
        'expected_value': '123.45',
    },
    {
        'metadata': {'type': 'Percent', 'precision': 5, 'scale': 2},
        'data_to_insert': 123.45,
        'expected_type': 'DECIMAL',
        'expected_value': '123.45',
    },
    {
        'metadata': {'type': 'Phone'},
        'data_to_insert': SAMPLE_PHONE,
        'expected_type': 'STRING',
        'expected_value': SAMPLE_PHONE,
    },
    {
        'metadata': {
            'type': 'Picklist',
            'valueSet': {
                'valueSetDefinition' : {
                    'sorted': 'false',
                    'value': [
                        {
                            'fullName': 'red',
                            'default': 'true'
                        },
                        {
                            'fullName': 'green',
                            'default': 'false'
                        },
                        {
                            'fullName': 'blue',
                            'default': 'false'
                        }
                    ]
                }
            }
        },
        'data_to_insert': 'blue',
        'expected_type': 'STRING',
        'expected_value': 'blue',
        'null_value': 'red'
    },
    {
        'metadata': {
            'type': 'MultiselectPicklist',
            'valueSet': {
                'valueSetDefinition' : {
                    'sorted': 'false',
                    'value': [
                        {
                            'fullName': 'red',
                            'default': 'true'
                        },
                        {
                            'fullName': 'green',
                            'default': 'false'
                        },
                        {
                            'fullName': 'blue',
                            'default': 'false'
                        }
                    ]
                }
            },
            'visibleLines': 3
        },
        'data_to_insert': 'blue;green',
        'expected_type': 'STRING',
        'expected_value': 'blue;green',
        'null_value': 'red'
    },
    {
        'metadata': {'type': 'Text', 'length': 64},
        'data_to_insert': SAMPLE_TEXT,
        'expected_type': 'STRING',
        'expected_value': SAMPLE_TEXT,
    },
    {
        'metadata': {'type': 'TextArea'},
        'data_to_insert': SAMPLE_TEXT,
        'expected_type': 'STRING',
        'expected_value': SAMPLE_TEXT,
    },
    {
        'metadata': {'type': 'LongTextArea', 'length': 32000, 'visibleLines': 12},
        'data_to_insert': LONG_SAMPLE_TEXT,
        'expected_type': 'STRING',
        'expected_value': LONG_SAMPLE_TEXT,
    },
    {
        'metadata': {'type': 'Url'},
        'data_to_insert': SAMPLE_URL,
        'expected_type': 'STRING',
        'expected_value': SAMPLE_URL,
    },
    {
        'metadata': {'type': 'Html', 'length': 32768, 'visibleLines': 12},
        'data_to_insert': SAMPLE_HTML,
        'expected_type': 'STRING',
        'expected_value': SAMPLE_HTML,
    },
    {
        'metadata': {'type': 'Location', 'scale': 3},
        'data_to_insert': SAMPLE_LOCATION,
        'expected_type': 'DECIMAL',
        'expected_value': EXPECTED_LOCATION,
        'compound_field': True,
        'null_value': NULL_LOCATION
    },
    {
        'metadata': {'type': 'Time'},
        'data_to_insert': '12:34:56Z',
        'expected_type': 'DATETIME',
        'expected_value': 45296000,
    },
]


# Max name length 40: https://help.salesforce.com/s/articleView?id=000313503&type=1
# "This name can contain only underscores and alphanumeric characters, and must
# be unique in your org. It must begin with a letter, not include spaces, not
# end with an underscore, and not contain two consecutive underscores."
# https://help.salesforce.com/s/articleView?id=sf.adding_fields.htm&type=5
OBJECT_NAMES = [
    ('keywords', 'object', 'field'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_object_name', get_random_string(string.ascii_letters, 40), get_random_string(string.ascii_letters, 20)),
    ('max_field_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 40)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    (
        'special',
        get_random_string(string.ascii_letters, 5) + "_" + get_random_string(string.ascii_letters, 5),
        get_random_string(string.ascii_letters, 5) + "_" + get_random_string(string.ascii_letters, 5)
    ),
]


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

    TEST_DATA['QUOTED_DATA_TO_INSERT'] = [
        {'FirstName': 'Matthew', 'LastName': 'O\' Smith', 'Email': 'matthewsmith@example.com'}]

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
                 f'DailyBulkApiBatches: {limits["DailyBulkApiBatches"]["Remaining"]}, ' \
                 f'DailyDurableStreamingApiEvents: {limits["DailyDurableStreamingApiEvents"]["Remaining"]}, ' \
                 f'HourlyPublishedStandardVolumePlatformEvents: ' \
                 f'{limits["HourlyPublishedStandardVolumePlatformEvents"]["Remaining"]}, ' \
                 f'MonthlyPlatformEventsUsageEntitlement: {limits["MonthlyPlatformEventsUsageEntitlement"]["Remaining"]} '

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


def verify_wiretap_records_data(wiretap_records, expected_data, sort=True):
    # SDC-10773 - source IDs must be unique
    source_ids = {record.header['sourceId'] for record in wiretap_records}
    assert len(source_ids) == len(wiretap_records)

    rows_from_wiretap = [record.field for record in wiretap_records]

    data_from_wiretap = [{field: record[field] for field in record if field not in ['Id', 'SystemModstamp']}
                         for record in rows_from_wiretap]

    if data_from_wiretap and sort:
        data_from_wiretap = sorted(data_from_wiretap,
                                   key=lambda k: k[
                                       'FirstName' if 'FirstName' in data_from_wiretap[0] else 'surName'].value)

    if data_from_wiretap:
        assert data_from_wiretap == expected_data


def verify_wiretap_data(wiretap, expected_data, sort=True):
    """Utility method to verify that a wiretap matches the expected data

    Args:
        wiretap: wiretap containing data to be verified
        expected_data (obj:`list`): Expected data as a list of dicts
        sort (Boolean): Whether to sort or not before comparing

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    verify_wiretap_records_data(wiretap.output_records, expected_data, sort)


def insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, expected_data, salesforce, data_to_insert, sort=True):
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

        verify_wiretap_data(wiretap, expected_data, sort)
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
    return [{'Id': record[key]} for record in records]


def check_ids(records):
    """Utility method to verify that records were actually written to Salesforce.
    Creation can fail due to errors such as STORAGE_LIMIT_EXCEEDED, resulting in an
    Id of None. Bail early for clarity and speed!

    Args:
        records (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]

    Returns:
        The `records` argument, for easy chaining.
    """
    for record in records:
        assert record['Id'], "Error creating Salesforce data"
    return records

def clean_up(sdc_executor, pipeline, client, record_ids, object_name='Contact'):
    """Utility method to delete inserted records and stop the pipeline

    Args:
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        pipeline (:py:class:`streamsets.sdk.sdc_models.Pipeline`): Pipeline instance to be stopped
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client
        record_ids (:obj:`list`): List of records to be deleted in form [{'Id':'001000000000001'},...]
    """
    if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        logger.info('Stopping pipeline ...')
        try:
            # Wait false is because no synchronization needed with the stop
            sdc_executor.stop_pipeline(pipeline, wait=False)
        except Exception:
            logger.error('Unable to stop the pipeline ...')
    try:
        if record_ids:
            logger.info(f'Deleting {object_name}s with id(s) {record_ids} ...')
            getattr(client.bulk, object_name).delete(record_ids)
    except Exception:
        logger.error(f'Unable to delete {object_name}s ...')


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


def add_custom_field_to_contact(salesforce, custom_field_name):
    field_content = ADD_CUSTOM_FIELD_TEMPLATE.safe_substitute({'CUSTOM_FIELD': custom_field_name})
    permission_content = CUSTOM_FIELD_PERMISSION_TEMPLATE.safe_substitute({'CUSTOM_FIELD': custom_field_name})
    deploy_metadata(salesforce.metadata_client,
                    ADD_CUSTOM_FIELD_PACKAGE,
                    [{'name': 'objects/Contact.object', 'content': field_content},
                      {'name': 'profiles/Admin.profile', 'content': permission_content}])
    # Salesforce orgs can have a namespace associated with them. The namespace
    # is prepended to custom field names, breaking this test.
    namespace = get_org_namespace_prefix(salesforce.client)
    return namespace + "__" + custom_field_name if namespace else custom_field_name


def get_org_namespace_prefix(client):
    result = client.query("SELECT Id, NamespacePrefix FROM Organization")
    return result['records'][0]['NamespacePrefix']


def delete_custom_field_from_contact(metadata, custom_field_name):
    field_content = DELETE_CUSTOM_FIELD_TEMPLATE.safe_substitute({'CUSTOM_FIELD': custom_field_name})
    deploy_metadata(metadata,
                    DELETE_CUSTOM_FIELD_PACKAGE,
                    [{'name': 'destructiveChanges.xml', 'content': field_content}])


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


def get_cdc_wiretap_records(wiretap, record_ids, expected_count=1,
                            time_between_checks=DEFAULT_WAIT_TIME_BETWEEN_CHECKS, timeout=DEFAULT_WAIT_TIMEOUT):
    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting to get CDC record(s) for id(s) {}'.format(timeout,
                                                                                                      record_ids))

    recorded_ids = []
    records = []
    def wiretap_condition():
        for record in wiretap.output_records:
            wiretap_record_id = record.header.values.get('salesforce.cdc.recordIds')
            if wiretap_record_id and wiretap_record_id in record_ids and wiretap_record_id not in recorded_ids:
                records.append(record)
                recorded_ids.append(wiretap_record_id)

        return len(records) == expected_count

    wait_for_condition(condition=wiretap_condition, failure=failure,
                       time_between_checks=time_between_checks, timeout=timeout)
    return records


def create_push_topic(client, last_name):
    """Utility method to create a PushTopic to subscribe to an existing Contact change events.

    Args:
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client
        last_name (:obj:`str`): A random and unique last name of the Contact for the topic to listen by

    Returns:
        (:obj:`str`) PushTopic record Id
        (:obj:`str`) PushTopic name
    """
    push_topic_name = TEST_DATA['STR_15_RANDOM']
    logger.info(f'Creating PushTopic {push_topic_name} in Salesforce ...')
    result = client.PushTopic.create({'Name': push_topic_name,
                                      'Query': ('SELECT Id, FirstName, LastName, Email, LeadSource '
                                                f"FROM Contact WHERE LastName = '{last_name}'"),
                                      'ApiVersion': API_VERSION,
                                      'NotifyForOperationCreate': True,
                                      'NotifyForOperationUpdate': True,
                                      'NotifyForOperationUndelete': True,
                                      'NotifyForOperationDelete': True,
                                      'NotifyForFields': 'All'})
    return result['id'], push_topic_name

def verify_analytics_data(client, edgemart_alias, test_data, order_key, multiple_data = False):
    records_per_batch = len(test_data)
    minimum_total_records = 2 * records_per_batch
    identifier = None
    current_version_id = None
    try:
        # Einstein Analytics data load is asynchronous, so poll until it's done
        logger.info('Looking for dataset in Einstein Analytics')
        end_time = datetime.now() + timedelta(seconds=120)
        while identifier is None and datetime.now() < end_time:
            sleep(5)
            identifier, current_version_id = find_dataset(client, edgemart_alias)

        # Make sure we found a dataset and didn't time out!
        assert identifier is not None

        # Now query the data from Einstein Analytics using SAQL

        # Build the load statement
        load = f'q = load \"{identifier}/{current_version_id}\";'

        # Build the identity projection - e.g.
        # q = foreach q generate Email as Email, FirstName as FirstName, LastName as LastName, LeadSource as LeadSource;
        field_list = []
        for key in test_data[0]:
            field_list.append(f'{key} as {key}')
        projection = 'q = foreach q generate ' + ', '.join(field_list) + ';'

        # Ensure consistent ordering
        ordering = f'q = order q by {order_key};'

        query = load + projection + ordering

        logger.info('Querying Einstein Analytics: %s', query)
        response = client.restful('wave/query', method='POST', json={'query': query})

        sorted_input_data = sorted(test_data, key=itemgetter(order_key))
        returned_records_count = len(response['results']['records'])

        if multiple_data:
            # It's possible for the pipeline to process more than the minimum number of records
            assert minimum_total_records <= returned_records_count
            # The returned number of records must be a multiple of the batch size
            assert returned_records_count % records_per_batch == 0
            batch_count = returned_records_count // records_per_batch
            # The returned data comprises batch_count copies of each input record
            for i in range(records_per_batch):
                for j in range(batch_count):
                    assert sorted_input_data[i] == response['results']['records'][(i * batch_count) + j]
        else:
            assert records_per_batch == returned_records_count
            assert sorted_input_data == response['results']['records']

    finally:
        if identifier:
            # simple_salesforce assumes there will be a JSON response,
            # but DELETE returns 204 with no response
            # See https://github.com/simple-salesforce/simple-salesforce/issues/327
            try:
                logger.info('Deleting dataset in Einstein Analytics')
                client.restful(f'wave/datasets/{identifier}', method='DELETE')
            except JSONDecodeError:
                pass


def get_current_user_id(client):
    # Can't use simple_salesforce for userinfo service URL
    userinfo_url = USERINFO_URL_FORMAT.format(client.sf_instance)
    r = requests.get(userinfo_url, headers={'Authorization':  'Bearer ' + client.session_id})
    assert r.status_code == 200
    return r.json()['user_id']

def assign_hard_delete(client):
    # Get Id of current user
    user_id = get_current_user_id(client)

    # Create a permission set to allow hard delete
    result = client.PermissionSet.create({
        'Label': f'Hard Delete Allowed {TEST_DATA["STR_15_RANDOM"]}',
        'Name': f'Hard_Delete_Allowed_{TEST_DATA["STR_15_RANDOM"]}',
        'PermissionsBulkApiHardDelete': True
    })
    assert result['success']
    permission_set_id = result['id']

    # Assign permission set to current user
    result = client.PermissionSetAssignment.create({
        'AssigneeId': user_id,
        'PermissionSetId': permission_set_id
    })
    assert result['success']
    permission_set_assignment_id = result['id']


def revoke_hard_delete(client):
    # Get Id of current user
    user_id = get_current_user_id(client)

    # Find permission set
    result = client.query('SELECT Id FROM PermissionSet'
                          f' WHERE Name = \'Hard_Delete_Allowed_{TEST_DATA["STR_15_RANDOM"]}\'')
    if len(result['records']) == 1:
        permission_set_id = result['records'][0]['Id']

        # Find permission set assignment
        result = client.query('SELECT Id FROM PermissionSetAssignment'
                              f' WHERE AssigneeId = \'{user_id}\' AND PermissionSetId = \'{permission_set_id}\'')
        if len(result['records']) == 1:
            permission_set_assignment_id = result['records'][0]['Id']
            client.PermissionSetAssignment.delete(permission_set_assignment_id)

        client.PermissionSet.delete(permission_set_id)


def verify_result_ids(expected_ids, result, result_key='Id'):
    assert len(expected_ids) == len(result['records'])
    for i in range(len(expected_ids)):
        assert expected_ids[i]['Id'] == result['records'][i][result_key]


def set_field_permissions(mdapi, object_name, field_name):
    # By default, no one has permission to access a new custom field.
    # Add field permission for the System Administrator
    profile = mdapi.Profile()
    profile.fullName = 'Admin'
    profile.fieldPermissions = {
        'field': f'{object_name}__c.{field_name}__c',
        'readable': True,
        'editable': True
    }
    mdapi.Profile.update(profile)


def compare_values(expected, actual, object_type):
    if object_type == 'MultiselectPicklist':
        # Multiselect picklist values are returned in an arbitrary order
        return (not expected and not actual) or set(expected.split(';')) == set(actual.split(';'))
    return expected == actual
