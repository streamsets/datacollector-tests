# Copyright 2017 StreamSets Inc.
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

import base64
import copy
import json
import logging
import string
import time
from io import BytesIO
from uuid import uuid4
from json.decoder import JSONDecodeError
from datetime import datetime, timedelta
from itertools import zip_longest
from operator import itemgetter
from sfdclib import SfdcSession, SfdcMetadataApi
from time import sleep
from xml.sax.saxutils import escape
from zipfile import ZipFile

import pytest
import requests
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string

CONTACT = 'Contact'
CDC = 'CDC'
PUSH_TOPIC = 'PUSH_TOPIC'
API_VERSION = '47.0'

logger = logging.getLogger(__name__)

DATA_TO_INSERT = [{'FirstName': 'Test1', 'LastName': 'User1',
                   'Email': 'xtest1@example.com', 'LeadSource': 'Advertisement'},
                  {'FirstName': 'Test2', 'LastName': 'User2', 'Email': 'xtest2@example.com', 'LeadSource': 'Partner'},
                  {'FirstName': 'Test3', 'LastName': 'User3', 'Email': 'xtest3@example.com', 'LeadSource': 'Web'}]

# For testing of SDC-7548
# Since email is used in WHERE clause in lookup processory query,
# create data containing 'from' word in emails to verify the bug is fixed.
DATA_WITH_FROM_IN_EMAIL = [{'FirstName': 'Test1', 'LastName': 'User1',
                            'Email': 'FROMxtest1@example.com', 'LeadSource': 'Advertisement'},
                           {'FirstName': 'Test2', 'LastName': 'User2',
                            'Email': 'xtefromst2@example.com', 'LeadSource': 'Partner'},
                           {'FirstName': 'Test3', 'LastName': 'User3',
                            'Email': 'xtes3@example.comFROM', 'LeadSource': 'Web'}]
CSV_DATA_TO_INSERT = [','.join(DATA_TO_INSERT[0].keys())] + [','.join(item.values()) for item in DATA_TO_INSERT]

ADD_CUSTOM_FIELD_PACKAGE= f'''<?xml version="1.0" encoding="UTF-8"?>
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

ADD_CUSTOM_FIELD= '''<?xml version="1.0" encoding="UTF-8"?>
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

CUSTOM_FIELD_PERMISSION='''<?xml version="1.0" encoding="UTF-8"?>
<Profile xmlns="http://soap.sforce.com/2006/04/metadata">
  <fieldPermissions>
        <editable>true</editable>
        <field>Contact.BoolCustField__c</field>
        <readable>true</readable>
    </fieldPermissions>
</Profile>'''

DELETE_CUSTOM_FIELD_PACKAGE=f'''<?xml version="1.0" encoding="UTF-8"?>
<Package xmlns="http://soap.sforce.com/2006/04/metadata">
    <version>{API_VERSION}</version>
</Package>'''

DELETE_CUSTOM_FIELD='''<?xml version="1.0" encoding="UTF-8"?>
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


@pytest.fixture(autouse=True)
def check_salesforce_is_clean(salesforce):
    """pytest fixture to check that there are no stray records in Salesforce as a result of broken tests.

    Args:
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    for sobject in ['Contact', 'Account', 'Document', 'Event', 'PushTopic', 'Case', 'Contract']:
        logger.info(f'Checking that there are no {sobject} records')
        count = salesforce.client.query(f'SELECT COUNT(Id) FROM {sobject}')['records'][0]['expr0']
        assert count == 0, f'Found {count} {sobject} records'


def _get_ids(records, key):
    """Utility method to extract list of Ids from Bulk API insert/query result.

    Args:
        records (:obj:`list`): List of records from a Bulk API insert or SOQL query.
        key (:obj:`str`): Key to extract - 'Id' for queries or 'id' for inserted data.

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    return [{'Id': record[key]}
            for record in records]


def _clean_up(sdc_executor, pipeline, client, contact_ids):
    """Utility method to delete inserted contacts and stop the pipeline

    Args:
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        pipeline (:py:class:`streamsets.sdk.sdc_models.Pipeline`): Pipeline instance to be stopped
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client
        contact_ids (:obj:`list`): List of contacts to be deleted in form [{'Id':'001000000000001'},...]
    """
    try:
        if contact_ids:
            logger.info('Deleting records ...')
            client.bulk.Contact.delete(contact_ids)
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)


@salesforce
def test_salesforce_destination(sdc_builder, sdc_executor, salesforce):
    """Send text to Salesforce destination from Dev Raw Data Source and confirm
    that Salesforce destination successfully reads them using Salesforce client.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, CSV_DATA_TO_INSERT)

    salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
    field_mapping = [{'sdcField': '/FirstName', 'salesforceField': 'FirstName'},
                     {'sdcField': '/LastName', 'salesforceField': 'LastName'},
                     {'sdcField': '/Email', 'salesforceField': 'Email'},
                     {'sdcField': '/LeadSource', 'salesforceField': 'LeadSource'}]
    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          sobject_type=CONTACT)

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    read_ids = None

    client = salesforce.client
    try:
        # Produce Salesforce records using pipeline.
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        # Using Salesforce connection, read the contents in the Salesforce destination.
        # Changing " with ' and vice versa in following string makes the query execution fail.
        query_str = ("SELECT Id, FirstName, LastName, Email, LeadSource "
                     "FROM Contact WHERE Email LIKE 'xtest%' ORDER BY Id")
        result = client.query(query_str)

        read_data = [f'{item["FirstName"]},{item["LastName"]},{item["Email"]},{item["LeadSource"]}'
                     for item in result['records']]
        # Following is used later to delete these records.
        read_ids = _get_ids(result['records'], 'Id')

        assert CSV_DATA_TO_INSERT[1:] == read_data

    finally:
        _clean_up(sdc_executor, pipeline, client, read_ids)


@salesforce
@sdc_min_version('3.11.0')
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_destination_default_mapping(sdc_builder, sdc_executor, salesforce, api):
    """Send text to Salesforce destination from Dev Raw Data Source and confirm
    that Salesforce destination successfully reads them using Salesforce client.
    This test checks that field name mappings are correctly applied.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Replace field names in raw data, map them back in destination
    data_to_insert = [item.replace('Email', 'em').replace('LeadSource', 'ls') for item in CSV_DATA_TO_INSERT]
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, data_to_insert)

    salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
    # FirstName and LastName should be mapped by default
    field_mapping = [{'sdcField': '/em', 'salesforceField': 'Email'},
                     {'sdcField': '/ls', 'salesforceField': 'LeadSource'}]
    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          use_bulk_api=(api == 'bulk'),
                                          sobject_type=CONTACT)

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    read_ids = None

    client = salesforce.client
    try:
        # Produce Salesforce records using pipeline.
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        # Using Salesforce connection, read the contents in the Salesforce destination.
        # Changing " with ' and vice versa in following string makes the query execution fail.
        query_str = ("SELECT Id, FirstName, LastName, Email, LeadSource "
                     "FROM Contact WHERE Email LIKE 'xtest%' ORDER BY Id")
        result = client.query(query_str)

        read_data = [f'{item["FirstName"]},{item["LastName"]},{item["Email"]},{item["LeadSource"]}'
                     for item in result['records']]
        # Following is used later to delete these records.
        read_ids = _get_ids(result['records'], 'Id')

        # Read data should match the original data, before we changed the field names
        assert CSV_DATA_TO_INSERT[1:] == read_data

    finally:
        _clean_up(sdc_executor, pipeline, client, read_ids)


@salesforce
def test_salesforce_destination_commit_before_stopping(sdc_builder, sdc_executor, salesforce):
    """Send text to Salesforce destination from Dev Raw Data Source and confirm
    that Salesforce destination successfully reads them using Salesforce client.

    It verifies that SalesForce destination is able to write data without
    stopping the pipeline (SDC-10475).

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, CSV_DATA_TO_INSERT)

    salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
    field_mapping = [{'sdcField': '/FirstName', 'salesforceField': 'FirstName'},
                     {'sdcField': '/LastName', 'salesforceField': 'LastName'},
                     {'sdcField': '/Email', 'salesforceField': 'Email'},
                     {'sdcField': '/LeadSource', 'salesforceField': 'LeadSource'}]
    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          sobject_type=CONTACT)

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    read_ids = None

    client = salesforce.client
    try:
        # Produce Salesforce records using pipeline.
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)

        # Using Salesforce connection, read the contents in the Salesforce destination.
        # Changing " with ' and vice versa in following string makes the query execution fail.
        query_str = ("SELECT Id, FirstName, LastName, Email, LeadSource "
                     "FROM Contact WHERE Email LIKE 'xtest%' ORDER BY Id")
        result = client.query(query_str)

        read_data = [f'{item["FirstName"]},{item["LastName"]},{item["Email"]},{item["LeadSource"]}'
                     for item in result['records']]
        # Following is used later to delete these records.
        read_ids = _get_ids(result['records'], 'Id')

        assert CSV_DATA_TO_INSERT[1:] == read_data

        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count > 3

        # That will mean that duplicated are marked as error, but at the same time means that is able to commit
        # records to SalesForce
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count > 0

    finally:
        _clean_up(sdc_executor, pipeline, client, read_ids)


@salesforce
@pytest.mark.parametrize(('condition'), [
    'query_without_prefix',
    # Testing of SDC-9067
    'query_with_prefix'
])
def test_salesforce_origin(sdc_builder, sdc_executor, salesforce, condition):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        condition (:obj:`str`): Whether or not to include SObject prefix in query - 'query_without_prefix' or
            'query_with_prefix'
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    if condition == 'query_without_prefix':
        query = ("SELECT Id, FirstName, LastName, Email, LeadSource FROM Contact "
                 "WHERE Id > '000000000000000' AND "
                 "Email LIKE 'xtest%' "
                 "ORDER BY Id")
    else:
        # SDC-9067 - redundant object name prefix caused NPE
        query = ("SELECT Contact.Id, Contact.FirstName, Contact.LastName, Contact.Email, Contact.LeadSource "
                 "FROM Contact WHERE Id > '000000000000000' AND Email LIKE 'xtest%' ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    # Changing " with ' and vice versa in following string makes the query execution fail.
    salesforce_origin.set_attributes(soql_query=query,
                                     subscribe_for_notifications=False)

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    verify_by_snapshot(sdc_executor, pipeline, salesforce_origin, DATA_TO_INSERT, salesforce)


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_nulls(sdc_builder, sdc_executor, salesforce, api):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using snapshot. This test checks that nulls are correctly
    read for fields that are not set in Salesforce (SDC-11086).

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Specify some fields that we haven't set
    query = ("SELECT Id, FirstName, LastName, Description, HomePhone FROM Contact "
             "WHERE Id > '000000000000000' AND "
             "Email LIKE 'xtest%' "
             "ORDER BY Id")

    expected_data = [{'FirstName': 'Test1', 'LastName': 'User1', 'Description': None, 'HomePhone': None},
                     {'FirstName': 'Test2', 'LastName': 'User2', 'Description': None, 'HomePhone': None},
                     {'FirstName': 'Test3', 'LastName': 'User3', 'Description': None, 'HomePhone': None}]

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=(api == 'bulk'),
                                     subscribe_for_notifications=False)

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    verify_by_snapshot(sdc_executor, pipeline, salesforce_origin, expected_data, salesforce)


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
                                       raw_data='\n'.join(raw_data))
    return dev_raw_data_source


def verify_snapshot(snapshot, stage, expected_data):
    """Utility method to verify that a snapshot matches the expected data

    Args:
        snapshot (:py:class:`streamsets.sdk.sdc_models.Snapshot`): Snapshot containing data to be verified
        stage (:py:class:`streamsets.sdk.sdc_models.Stage`): Stage after which data is to be verified
        expected_data (obj:`list`): Expected data as a list of dicts

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    # SDC-10773 - source IDs must be unique
    source_ids = {record.header['sourceId'] for record in snapshot[stage].output}
    assert len(source_ids) == len(snapshot[stage].output)

    rows_from_snapshot = [record.field
                          for record in snapshot[stage].output]

    data_from_snapshot = [{field:record[field] for field in record if field not in ['Id', 'SystemModstamp']}
                          for record in rows_from_snapshot]

    data_from_snapshot = sorted(data_from_snapshot, key=lambda k: k['FirstName'].value)

    assert data_from_snapshot == expected_data


def verify_by_snapshot(sdc_executor, pipeline, stage, expected_data, salesforce, data_to_insert=DATA_TO_INSERT):
    """Utility method to insert data into Salesforce, start a pipeline, capture a snapshot, verify that the snapshot
    matches the expected data, and clean up the inserted records.

    Args:
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        pipeline (:py:class:`streamsets.sdk.sdc_models.Pipeline`): Pipeline instance
        stage (:py:class:`streamsets.sdk.sdc_models.Stage`): Stage after which data is to be verified
        expected_data (obj:`list`): Expected data as a list of dicts
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        data_to_insert (obj:`list`): Data to be inserted, as a list of dicts

    Returns:
        (:obj:`list`) of inserted record Ids in form [{'Id':'001000000000001'},...]
    """
    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = _get_ids(client.bulk.Contact.insert(data_to_insert), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        verify_snapshot(snapshot, stage, expected_data)

    finally:
        _clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_datetime(sdc_builder, sdc_executor, salesforce, api):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using snapshot. This test checks that datetime fields can
    be used as offsets (SDC-10352).

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = ("SELECT Id, FirstName, LastName, Email, LeadSource, SystemModstamp "
             "FROM Contact WHERE SystemModstamp > ${OFFSET} ORDER BY SystemModstamp")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    # Changing " with ' and vice versa in following string makes the query execution fail.
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=(api == 'bulk'),
                                     subscribe_for_notifications=False,
                                     repeat_query='INCREMENTAL',
                                     query_interval='${24 * HOURS}',
                                     initial_offset='2018-10-16T00:00:00.000Z',
                                     offset_field='SystemModstamp')

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, timeout_sec=60).snapshot

        verify_snapshot(snapshot, salesforce_origin, DATA_TO_INSERT)

        sdc_executor.stop_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, timeout_sec=60).snapshot
        rows_from_snapshot = [record.value['value']
                              for record in snapshot[salesforce_origin].output]

        assert len(rows_from_snapshot) == 0

    finally:
        _clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
@pytest.mark.parametrize(('data'), [
    DATA_TO_INSERT,
    # Testing of SDC-7548
    DATA_WITH_FROM_IN_EMAIL
])
@pytest.mark.parametrize(('query_with_time'), [
    # Testing of SDC-10207
    True,
    False
])
def test_salesforce_lookup_processor(sdc_builder, sdc_executor, salesforce, data, query_with_time):
    """Simple Salesforce Lookup processor test.
    Pipeline will enrich records with the 'LastName' of contacts by adding a field as 'surName'.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        data (:obj:`list`): Dataset to use in test
        query_with_time (:obj:`bool`): Whether or not to filter results by time
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    lookup_data = ['Email'] + [row['Email'] for row in data]
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

    salesforce_lookup = pipeline_builder.add_stage('Salesforce Lookup')
    # Changing " with ' and vice versa in following string makes the query execution fail.
    query_str = ("SELECT Id, FirstName, LastName, LeadSource FROM Contact "
                 "WHERE Email = '${record:value(\"/Email\")}'")

    if query_with_time:
        # Testing of SDC-10207 - select records that changed before an hour from now -
        # i.e. all of them! We're just checking here that the time functions are supported
        query_str += (" AND LastModifiedDate < ${time:extractStringFromDate("
                      "     time:millisecondsToDateTime(time:dateTimeToMilliseconds("
                      "         time:now()) + 3600000), \"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}")

    field_mappings = [dict(dataType='USE_SALESFORCE_TYPE',
                           salesforceField='LastName',
                           sdcField='/surName')]
    salesforce_lookup.set_attributes(soql_query=query_str,
                                     field_mappings=field_mappings)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> salesforce_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    LOOKUP_EXPECTED_DATA = copy.deepcopy(data)
    for record in LOOKUP_EXPECTED_DATA:
        record['surName'] = record.pop('LastName')
    verify_by_snapshot(sdc_executor, pipeline, salesforce_lookup, LOOKUP_EXPECTED_DATA,
                       salesforce, data_to_insert=data)


@salesforce
def test_salesforce_lookup_processor_retrieve(sdc_builder, sdc_executor, salesforce):
    """Simple Salesforce Lookup processor test - retrieve by record id rather than query
    Pipeline will enrich records with the 'LastName' of contacts by adding a field as 'surName'.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    client = salesforce.client
    contact_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        contact_ids = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT), 'id')

        pipeline_builder = sdc_builder.get_pipeline_builder()

        # Lookup data is record Id's
        lookup_data = ['Id'] + [row['Id'] for row in contact_ids]
        dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

        salesforce_lookup = pipeline_builder.add_stage('Salesforce Lookup')

        # Map LastName to surName
        field_mappings = [dict(dataType='USE_SALESFORCE_TYPE',
                               salesforceField='LastName',
                               sdcField='/surName')]
        # Ask for all of the fields we set, so that we can assert their values
        salesforce_lookup.set_attributes(lookup_mode='RETRIEVE',
                                         id_field='/Id',
                                         salesforce_fields=','.join(DATA_TO_INSERT[0].keys()),
                                         object_type=CONTACT,
                                         field_mappings=field_mappings)

        trash = pipeline_builder.add_stage('Trash')
        dev_raw_data_source >> salesforce_lookup >> trash
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # We need to look for surName field instead of LastName
        lookup_expected_data = copy.deepcopy(DATA_TO_INSERT)
        for record in lookup_expected_data:
            record['surName'] = record.pop('LastName')

        verify_snapshot(snapshot, salesforce_lookup, lookup_expected_data)

    finally:
        _clean_up(sdc_executor, pipeline, client, contact_ids)


@pytest.mark.parametrize(('missing_values_behavior'), [
    'SEND_TO_ERROR',
    'PASS_RECORD_ON'
])
@salesforce
def test_salesforce_retrieve_deleted_record(sdc_builder, sdc_executor, salesforce, missing_values_behavior):
    """Test SDC-13390 - attempt to retrieve a deleted record
    Pipeline will attempt to enrich records with the 'LastName' of contacts by
    adding a field as 'surName'. Depending on missing_values_behavior, the
    record with the deleted id should be passed along the pipeline or sent to
    error.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> trash
    """
    client = salesforce.client
    contact_ids = None
    pipeline = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        result = client.bulk.Contact.insert(DATA_TO_INSERT)
        contact_ids = [{'Id': item['id']}
                       for item in result]

        # Delete a record
        client.Contact.delete(contact_ids[1]['Id'])

        pipeline_builder = sdc_builder.get_pipeline_builder()

        # Lookup by record Id's
        lookup_data = ['Id'] + [row['Id'] for row in contact_ids]
        dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

        salesforce_lookup = pipeline_builder.add_stage('Salesforce Lookup')

        # Map LastName to surName
        field_mappings = [dict(dataType='USE_SALESFORCE_TYPE',
                               salesforceField='LastName',
                               sdcField='/surName')]

        # Ask for all of the fields we set, so that we can assert their values
        # If we don't find a record in Salesforce, we want to pass the record along the pipeline
        salesforce_lookup.set_attributes(lookup_mode='RETRIEVE',
                                         id_field='/Id',
                                         salesforce_fields=','.join(DATA_TO_INSERT[0].keys()),
                                         object_type=CONTACT,
                                         missing_values_behavior=missing_values_behavior,
                                         field_mappings=field_mappings)

        trash = pipeline_builder.add_stage('Trash')
        dev_raw_data_source >> salesforce_lookup >> trash
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # We need to look for surName field instead of LastName
        lookup_expected_data = copy.deepcopy(DATA_TO_INSERT)
        for record in lookup_expected_data:
            record['surName'] = record.pop('LastName')

        rows_from_snapshot = [record.field
                              for record in snapshot[salesforce_lookup].output]

        if (missing_values_behavior == 'PASS_RECORD_ON'):
            # Middle record should just have its Id field
            assert rows_from_snapshot[1]['Id'] == contact_ids[1]['Id']
        else:
            assert len(snapshot[salesforce_lookup].error_records) == 1
            assert snapshot[salesforce_lookup].error_records[0].field['Id'] == contact_ids[1]['Id']

        data_from_snapshot = [{field:record[field] for field in record if field not in ['Id', 'SystemModstamp']}
                              for record in rows_from_snapshot]

        # Remove the middle element(s) so we can do the next assert
        if (missing_values_behavior == 'PASS_RECORD_ON'):
            del(data_from_snapshot[1])

        del(lookup_expected_data[1])

        assert data_from_snapshot == lookup_expected_data

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        if contact_ids:
            logger.info('Deleting records ...')
            client.bulk.Contact.delete(contact_ids)


# Test SDC-9251, SDC-9493
@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_subquery(sdc_builder, sdc_executor, salesforce, api):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using snapshot. This test focuses on following relationships
    (SDC-9493) and ordering in subqueries (SDC-9251).

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Also tests SDC-9742 (ORDER BY field causes parse error), SDC-9762 (NPE in sub-query when following relationship)
    # We don't need to populate the ReportsTo field - the error was in retrieving its metadata
    query = ("SELECT Id, Name, (SELECT Id, LastName, ReportsTo.LastName FROM Contacts ORDER BY Id) FROM Account "
             "WHERE Id > '000000000000000' AND "
             "Name LIKE 'Account %' "
             "ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=(api == 'bulk'),
                                     subscribe_for_notifications=False)

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    account_ids = None
    contact_ids = None

    client = salesforce.client
    try:
        expected_accounts = []
        account_ids = []
        logger.info('Creating accounts using Salesforce client ...')
        for i in range(ACCOUNTS_FOR_SUBQUERY):
            expected_accounts.append({'Name': f'Account {i}'})

        account_ids = _get_ids(client.bulk.Account.insert(expected_accounts), 'id')

        expected_contacts = []
        contact_ids = []
        logger.info('Creating contacts using Salesforce client ...')
        for i in range(len(expected_accounts)):
            for j in range(CONTACTS_FOR_SUBQUERY):
                contact = {'AccountId': account_ids[i]['Id'], 'LastName': f'Contact {i} {j}'}
                expected_contacts.append(contact)

        contact_ids = _get_ids(client.bulk.Contact.insert(expected_contacts), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # Verify correct rows are received using snapshot.
        assert ACCOUNTS_FOR_SUBQUERY == len(snapshot[salesforce_origin].output)
        for i, account in enumerate(snapshot[salesforce_origin].output):
            assert expected_accounts[i]['Name'] == account.field['Name'].value
            assert CONTACTS_FOR_SUBQUERY == len(account.field['Contacts'])
            for j in range(CONTACTS_FOR_SUBQUERY):
                assert expected_contacts[(i * 5) + j]['LastName'] == account.field['Contacts'][j]['LastName']

    finally:
        logger.info('Deleting records ...')
        if account_ids:
            client.bulk.Account.delete(account_ids)
        _clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_origin_aggregate_count(sdc_builder, sdc_executor, salesforce):
    """Create data using Salesforce client and then check if Salesforce origin
    retrieves correct aggregate data using snapshot (SDC-10694).

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = "SELECT COUNT() FROM Contact"

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=False,
                                     subscribe_for_notifications=False,
                                     disable_query_validation=True)

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    contact_ids = None

    client = salesforce.client
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        contact_ids = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with a count field
        assert len(snapshot[salesforce_origin].output) == 1
        assert snapshot[salesforce_origin].output[0].field['count'] == 3

    finally:
        _clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_origin_aggregate(sdc_builder, sdc_executor, salesforce):
    """Create data using Salesforce client and then check if Salesforce origin
    retrieves correct aggregate data using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = ("SELECT COUNT(Id), "
             "MAX(NumberOfEmployees), "
             "MIN(Industry), "
             "SUM(AnnualRevenue) "
             "FROM Account")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=False,
                                     subscribe_for_notifications=False,
                                     disable_query_validation=True)

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    account_ids = None

    client = salesforce.client
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        data_to_insert = [{'Name': 'Test1',
                           'NumberOfEmployees': 1,
                           'Industry': 'Agriculture',
                           'AnnualRevenue': 123},
                          {'Name': 'Test2',
                           'NumberOfEmployees': 2,
                           'Industry': 'Finance',
                           'AnnualRevenue': 456},
                          {'Name': 'Test3',
                           'NumberOfEmployees': 3,
                           'Industry': 'Utilities',
                           'AnnualRevenue': 789}]
        account_ids = _get_ids(client.bulk.Account.insert(data_to_insert), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with a count field
        assert len(snapshot[salesforce_origin].output) == 1
        assert snapshot[salesforce_origin].output[0].field['expr0'].value == 3
        assert snapshot[salesforce_origin].output[0].field['expr1'].value == 3
        assert snapshot[salesforce_origin].output[0].field['expr2'].value == 'Agriculture'
        assert snapshot[salesforce_origin].output[0].field['expr3'].value == 1368

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        if account_ids:
            logger.info('Deleting records ...')
            client.bulk.Account.delete(account_ids)


@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_lookup_aggregate_count(sdc_builder, sdc_executor, salesforce):
    """Simple Salesforce Lookup processor test.
    Pipeline will enrich records with the number of contacts whose FirstName
    matches the /prefix field, adding the value as a string in the /count field

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    lookup_data = ['prefix', 'Test']
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

    salesforce_lookup = pipeline_builder.add_stage('Salesforce Lookup')
    # Changing " with ' and vice versa in following string makes the query execution fail.
    query_str = ("SELECT COUNT() FROM Contact "
                 "WHERE FirstName LIKE '${record:value(\"/prefix\")}%'")

    salesforce_lookup.set_attributes(soql_query=query_str)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> salesforce_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    contact_ids = None

    client = salesforce.client
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        contact_ids = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with a /count field containing an integer
        assert len(snapshot[salesforce_lookup].output) == 1
        assert snapshot[salesforce_lookup].output[0].field['count'].value == 3

    finally:
        _clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_lookup_aggregate(sdc_builder, sdc_executor, salesforce):
    """Simple Salesforce Lookup processor test.
    Pipeline will enrich records with the number of contacts whose FirstName
    matches the /prefix field, adding the value as a string in the /count field

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    lookup_data = ['prefix', 'Test']
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

    salesforce_lookup = pipeline_builder.add_stage('Salesforce Lookup')
    # Can sort reliably on FirstName and LastName
    # LeadSource ordering varies between org types!
    query_str = ("SELECT COUNT(Id), MIN(FirstName), MAX(LastName) FROM Contact "
                 "WHERE FirstName LIKE '${record:value(\"/prefix\")}%'")

    field_mappings = [dict(dataType='STRING',
                           salesforceField='expr0',
                           sdcField='/count')]
    salesforce_lookup.set_attributes(soql_query=query_str,
                                     field_mappings=field_mappings)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> salesforce_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    contact_ids = None

    client = salesforce.client
    try:
        contact_ids = []
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        contact_ids = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with a /count field containing three strings
        assert len(snapshot[salesforce_lookup].output) == 1
        assert snapshot[salesforce_lookup].output[0].field['count'].value == str(len(DATA_TO_INSERT))
        assert snapshot[salesforce_lookup].output[0].field['expr1'].value == DATA_TO_INSERT[0]['FirstName']
        assert snapshot[salesforce_lookup].output[0].field['expr2'].value == DATA_TO_INSERT[-1]['LastName']

    finally:
        _clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_session_timeout(sdc_builder, sdc_executor, salesforce, api):
    """Test that Salesforce origin correctly handles a session timing out while the pipeline
    is running

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = ("SELECT Id, FirstName, LastName, Email, LeadSource FROM Contact "
             "WHERE Id > '000000000000000' AND "
             "Email LIKE 'xtest%' "
             "ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     subscribe_for_notifications=False,
                                     use_bulk_api=(api == 'bulk'),
                                     repeat_query='FULL',
                                     query_interval=60)

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        verify_snapshot(snapshot, salesforce_origin, DATA_TO_INSERT)

        logger.info('Revoking Salesforce session')
        r = requests.get(f'https://{client.sf_instance}/services/oauth2/revoke',
                         params={'token': client.session_id})
        assert r.status_code == 200

        # We killed the client's session, so we need to make a new one to be
        # able to delete the data in the finally block
        client = salesforce.client

        logger.info('Capturing another snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=False, timeout_sec=120).snapshot
        verify_snapshot(snapshot, salesforce_origin, DATA_TO_INSERT)

    finally:
        _clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
def test_salesforce_origin_stop_resume(sdc_builder, sdc_executor, salesforce):
    """Create data using Salesforce client, stop the pipeline
    and then check if Salesforce origin receives them using snapshot.
    Insert more data and check again.

    The pipeline looks like:
        salesforce_origin >> trash
        salesforce_origin >= trash_2

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """

    aux_email= get_random_string(string.ascii_letters, 10).lower()

    DATA_TO_INSERT = [{'FirstName': 'Test1', 'LastName': 'User1',
                       'Email': f'{aux_email}1@example.com', 'LeadSource': 'Advertisement'},
                      {'FirstName': 'Test2', 'LastName': 'User2', 'Email': f'{aux_email}2@example.com',
                       'LeadSource': 'Partner'},
                      {'FirstName': 'Test3', 'LastName': 'User3', 'Email': f'{aux_email}3@example.com', 'LeadSource': 'Web'}]

    DATA_TO_INSERT_2 = [{'FirstName': 'XTest1', 'LastName': 'XUser2',
                         'Email': f'{aux_email}4@example.com', 'LeadSource': 'Advertisement'},
                        {'FirstName': 'XTest2', 'LastName': 'XUser2', 'Email': f'{aux_email}5@example.com',
                         'LeadSource': 'Partner'},
                        {'FirstName': 'XTest3', 'LastName': 'XUser3', 'Email': f'{aux_email}6@example.com',
                         'LeadSource': 'Web'}]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = ("SELECT Contact.Id, Contact.FirstName, Contact.LastName, Contact.Email, Contact.LeadSource "
             "FROM Contact WHERE Id > '000000000000000' AND Email LIKE '"+ aux_email + "%' ORDER BY Id")


    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     subscribe_for_notifications=False)

    trash = pipeline_builder.add_stage('Trash')
    trash_2 = pipeline_builder.add_stage('Trash')

    salesforce_origin >> trash
    salesforce_origin >= trash_2

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids_1 = None
    inserted_ids_2 = None

    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids_1 = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        verify_snapshot(snapshot, salesforce_origin, DATA_TO_INSERT)

        # Stage should produce events, and it does, since the fix for SDC-12418
        assert len(snapshot[salesforce_origin].event_records) == 1
        assert snapshot[salesforce_origin].event_records[0].header.values['sdc.event.type'] == 'no-more-data'

        # Pipeline stops, but if it changes in a future version
        sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)

        # Using Salesforce client, create new rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids_2 = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT_2), 'id')

        logger.info('Starting pipeline and snapshot')
        snapshot_2 = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        verify_snapshot(snapshot_2, salesforce_origin, DATA_TO_INSERT + DATA_TO_INSERT_2)

        # stage should produce events...
        assert len(snapshot[salesforce_origin].event_records) == 1
        assert snapshot[salesforce_origin].event_records[0].header.values['sdc.event.type'] == 'no-more-data'

    finally:
        inserted_ids = []
        if inserted_ids_1:
            inserted_ids += inserted_ids_1
        if inserted_ids_2:
            inserted_ids += inserted_ids_2
        if len(inserted_ids) == 0:
            inserted_ids = None
        _clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
def test_salesforce_origin_document(sdc_builder, sdc_executor, salesforce):
    """Test that base64-encoded Document data can be correctly read (SDC-12041).

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    doc_name = get_random_string(string.ascii_letters, 10).lower()

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=f'SELECT Id, Body FROM Document WHERE Name = \'{doc_name}\'',
                                     disable_query_validation=True,
                                     subscribe_for_notifications=False)

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_id = None
    folder_id = None
    try:
        # We need to put our document somewhere - create a folder if necessary
        # NOTE - it is not possible to delete a Folder when an associated Document is in the Recycle Bin, we don't have
        # the 'hard delete' permission by default and assigning that permission or emptying the recycle bin is not
        # possible via simple_salesforce. So - create a folder with a well-known name if necessary
        logger.info(f'Checking for Folder: {FOLDER_NAME}')
        result = client.query(f'SELECT Id FROM Folder WHERE Name = \'{FOLDER_NAME}\'')
        if (len(result['records']) > 0):
            logger.info(f'Found Folder: {FOLDER_NAME}')
            folder_id = result['records'][0]['Id']
        else:
            logger.info(f'Creating Folder: {FOLDER_NAME}')
            result = client.Folder.create({
                "Name": FOLDER_NAME,
                "DeveloperName": FOLDER_NAME,
                "Type": "Document",
                "AccessType": "Public"
            })
            folder_id = result['id']

        # Some content for our document
        body = 'The quick brown fox jumps over the lazy dog'.encode("utf-8")

        # Salesforce API wants base64-encoded Body
        data_to_insert = {'Name': doc_name,
                          'FolderId': folder_id,
                          'Body': base64.b64encode(body).decode("utf-8")}

        logger.info('Creating Document record using Salesforce client ...')
        ret = client.Document.create(data_to_insert)
        inserted_id = ret['id']

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with Id and Body fields
        assert len(snapshot[salesforce_origin].output) == 1
        assert snapshot[salesforce_origin].output[0].field['Id'].value == inserted_id
        assert snapshot[salesforce_origin].output[0].field['Body'].value == body

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (inserted_id):
            client.Document.delete(inserted_id)


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_destination_datetime(sdc_builder, sdc_executor, salesforce, api):
    """Test that datetimes are correctly written to Salesforce (SDC-12193).
    Create an Event record as this is one of the few standard objects with a
    settable Datetime

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    event_data = {
        "IsAllDayEvent": False,
        "DurationInMinutes": 10,
        "Location": "Casa Pat",
        "Description": "This is a test event"
    }

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json.dumps(event_data))

    # Use an Expression Evaluator to create a datetime value that we can write to Salesforce
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.field_expressions = [
        dict(fieldToSet='/ActivityDateTime',
             expression="${time:now()}")]

    salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
    field_mapping = [{'sdcField': '/ActivityDateTime', 'salesforceField': 'ActivityDateTime'},
                     {'sdcField': '/IsAllDayEvent', 'salesforceField': 'IsAllDayEvent'},
                     {'sdcField': '/DurationInMinutes', 'salesforceField': 'DurationInMinutes'},
                     {'sdcField': '/Description', 'salesforceField': 'Description'},
                     {'sdcField': '/Location', 'salesforceField': 'Location'}]
    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          sobject_type='Event',
                                          use_bulk_api=(api == 'bulk'))

    dev_raw_data_source >> expression_evaluator >> salesforce_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    read_ids = None

    client = salesforce.client
    try:
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        query_str = ('SELECT Id, ActivityDateTime, IsAllDayEvent, DurationInMinutes, Description, Location '
                     f'FROM Event WHERE Location = \'{event_data["Location"]}\'')
        result = client.query(query_str)

        read_ids = _get_ids(result['records'], 'Id')

        # Raw data source typically produces multiple records event if we only want one
        assert len(result['records']) > 0
        assert event_data['IsAllDayEvent'] == result['records'][0]['IsAllDayEvent']
        assert event_data['DurationInMinutes'] == result['records'][0]['DurationInMinutes']
        assert event_data['Location'] == result['records'][0]['Location']
        assert event_data['Description'] == result['records'][0]['Description']

    finally:
        logger.info('Deleting records ...')
        if read_ids:
            client.bulk.Event.delete(read_ids)


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_destination_relationship(sdc_builder, sdc_executor, salesforce, api):
    """Test that we can write to related external ID fields (SDC-12636).

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT), 'id')

        # Relate the created contacts to each other
        # first contact reports to second contact; second contact reports to third
        csv_data_to_insert = ['Id,ReportsTo.Email']
        csv_data_to_insert.append(f'{inserted_ids[0]["Id"]},{DATA_TO_INSERT[1]["Email"]}')
        csv_data_to_insert.append(f'{inserted_ids[1]["Id"]},{DATA_TO_INSERT[2]["Email"]}')

        pipeline_builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, csv_data_to_insert)

        salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
        field_mapping = [{'sdcField': '/Id', 'salesforceField': 'Id'},
                         {'sdcField': '/ReportsTo.Email', 'salesforceField': 'ReportsTo.Email'}]
        salesforce_destination.set_attributes(default_operation='UPDATE',
                                              field_mapping=field_mapping,
                                              sobject_type=CONTACT,
                                              use_bulk_api=(api == 'bulk'))

        dev_raw_data_source >> salesforce_destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        # Now the pipeline will make the contacts report to each other
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        # Using Salesforce connection, read the contents in the Salesforce destination.
        query_str = ("SELECT Id, Email, ReportsToId FROM Contact WHERE Email LIKE 'xtest%' ORDER BY Id")
        result = client.query(query_str)

        # The magic of external id fields - we knitted the contacts together via their emails
        # Salesforce set the related record ids accordingly
        assert result['records'][0]['ReportsToId'] == result['records'][1]['Id']
        assert result['records'][1]['ReportsToId'] == result['records'][2]['Id']

    finally:
        _clean_up(sdc_executor, pipeline, client, inserted_ids)


def create_push_topic(client):
    """Utility method to create a PushTopic to subscribe to Contact change events.

    Args:
        client (:py:class:`simple_salesforce.Salesforce`): Salesforce client

    Returns:
        (:obj:`str`) PushTopic record Id
        (:obj:`str`) PushTopic name
    """
    push_topic_name = get_random_string(string.ascii_letters, 10).lower()
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
    assert True == result['success']
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


@salesforce
@pytest.mark.parametrize(('subscription_type'), [
    PUSH_TOPIC,
    CDC
])
def test_salesforce_subscription(sdc_builder, sdc_executor, salesforce, subscription_type):
    """Start pipeline, create data using Salesforce client
    and then check if Salesforce origin receives notifications using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        subscription_type (:obj:`str`): Type of subscription: 'PUSH_TOPIC' or 'CDC'
    """
    client = salesforce.client

    pipeline = None
    subscription_id = None
    contact = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        if subscription_type == PUSH_TOPIC:
            subscription_id, push_topic_name = create_push_topic(client)
        else:
            subscription_id = enable_cdc(client)

        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(query_existing_data=False,
                                         subscribe_for_notifications=True,
                                         subscription_type=subscription_type)
        if subscription_type == PUSH_TOPIC:
            salesforce_origin.set_attributes(push_topic=push_topic_name)
        else:
            salesforce_origin.set_attributes(change_data_capture_object=CONTACT)

        trash = pipeline_builder.add_stage('Trash')
        salesforce_origin >> trash
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline')
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just create one contact
        logger.info('Creating record using Salesforce client...')
        contact = client.Contact.create(DATA_TO_INSERT[0])

        logger.info('Taking snapshot')
        snapshot = snapshot_command.wait_for_finished().snapshot

        if subscription_type == PUSH_TOPIC:
            verify_snapshot(snapshot, salesforce_origin, [DATA_TO_INSERT[0]])
        else:
            # CDC returns more than just the record fields, so verify_snapshot isn't so useful
            assert len(snapshot[salesforce_origin].output) == 1
            assert snapshot[salesforce_origin].output[0].header.values['salesforce.cdc.recordIds']
            assert snapshot[salesforce_origin].output[0].field['Email'] == DATA_TO_INSERT[0]['Email']
            # CDC returns nested compound fields
            assert snapshot[salesforce_origin].output[0].field['Name']['FirstName'] == DATA_TO_INSERT[0]['FirstName']
            assert snapshot[salesforce_origin].output[0].field['Name']['LastName'] == DATA_TO_INSERT[0]['LastName']

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        if subscription_id:
            if subscription_type == PUSH_TOPIC:
                logger.info('Deleting PushTopic...')
                client.PushTopic.delete(subscription_id)
            else:
                logger.info('Disabling CDC for Contact...')
                disable_cdc(client, subscription_id)
        if contact:
            logger.info('Deleting contact...')
            client.contact.delete(contact['id'])


def deploy_metadata(metadata, package_content, files):
    b = BytesIO()

    with ZipFile(b,'w') as zip:
        zip.writestr('package.xml', package_content)
        for file in files:
            zip.writestr(file['name'], file['content'])
        zip.close()

    deployment = metadata.deploy(b, {})

    result = None
    end_time = datetime.now() + timedelta(seconds=60)
    while result != 'Succeeded' and result != 'Failed' and datetime.now() < end_time:
        sleep(1)
        result = metadata.check_deploy_status(deployment[0])[0]

    logger.info(f'Deployment {result}')

    assert result == 'Succeeded'


def add_custom_field_to_contact(metadata):
    deploy_metadata(metadata,
                    ADD_CUSTOM_FIELD_PACKAGE,
                    [{'name':'objects/Contact.object', 'content':ADD_CUSTOM_FIELD},
                     {'name':'profiles/Admin.profile', 'content':CUSTOM_FIELD_PERMISSION}])


def delete_custom_field_from_contact(metadata):
    deploy_metadata(metadata,
                    DELETE_CUSTOM_FIELD_PACKAGE,
                    [{'name':'destructiveChanges.xml', 'content':DELETE_CUSTOM_FIELD}])


@salesforce
def test_salesforce_cdc_delete_field(sdc_builder, sdc_executor, salesforce):
    """Start pipeline, create data using Salesforce client
    and then check if Salesforce origin receives notifications using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        subscription_type (:obj:`str`): Type of subscription: 'PUSH_TOPIC' or 'CDC'
    """
    client = salesforce.client

    pipeline = None
    subscription_id = None
    contact = None
    contact2 = None
    try:
        session = SfdcSession(username=escape(salesforce.username),
                              password=escape(salesforce.password),
                              is_sandbox=True,
                              api_version=API_VERSION)
        session.login()

        metadata = SfdcMetadataApi(session)

        logger.info('Adding custom field to Contact object...')
        add_custom_field_to_contact(metadata)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        subscription_id = enable_cdc(client)

        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(query_existing_data=False,
                                         subscribe_for_notifications=True,
                                         subscription_type=CDC,
                                         change_data_capture_object=CONTACT)

        trash = pipeline_builder.add_stage('Trash')
        salesforce_origin >> trash
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline')
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just create one contact
        logger.info('Creating first record using Salesforce client...')
        contact = client.Contact.create(DATA_TO_INSERT[0])

        logger.info('Taking snapshot')
        snapshot = snapshot_command.wait_for_finished().snapshot

        # CDC returns more than just the record fields, so verify_snapshot isn't so useful
        assert len(snapshot[salesforce_origin].output) == 1
        assert snapshot[salesforce_origin].output[0].header.values['salesforce.cdc.recordIds']
        assert snapshot[salesforce_origin].output[0].field['Email'] == DATA_TO_INSERT[0]['Email']
        # CDC returns nested compound fields
        assert snapshot[salesforce_origin].output[0].field['Name']['FirstName'] == DATA_TO_INSERT[0]['FirstName']
        assert snapshot[salesforce_origin].output[0].field['Name']['LastName'] == DATA_TO_INSERT[0]['LastName']

        logger.info('Stopping pipeline')
        sdc_executor.stop_pipeline(pipeline)

        # Create another record, setting the custom field, then delete the
        # field, so that the current schema doesn't match the CDC notification
        logger.info('Creating second record using Salesforce client...')
        data_to_insert = copy.deepcopy(DATA_TO_INSERT[1])
        data_to_insert['BoolCustField__c'] = True
        contact2 = client.Contact.create(data_to_insert)

        logger.info('Deleting custom field from Contact object...')
        delete_custom_field_from_contact(metadata)

        logger.info('Restarting pipeline')
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        logger.info('Taking another snapshot')
        snapshot = snapshot_command.wait_for_finished().snapshot

        assert len(snapshot[salesforce_origin].output) == 1
        assert snapshot[salesforce_origin].output[0].header.values['salesforce.cdc.recordIds']
        assert snapshot[salesforce_origin].output[0].field['Email'] == data_to_insert['Email']
        assert snapshot[salesforce_origin].output[0].field['BoolCustField__c'] == data_to_insert['BoolCustField__c']
        # CDC returns nested compound fields
        assert snapshot[salesforce_origin].output[0].field['Name']['FirstName'] == data_to_insert['FirstName']
        assert snapshot[salesforce_origin].output[0].field['Name']['LastName'] == data_to_insert['LastName']

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        if subscription_id:
            logger.info('Disabling CDC for Contact...')
            disable_cdc(client, subscription_id)
        if contact:
            logger.info('Deleting contact...')
            client.contact.delete(contact['id'])
        if contact2:
            logger.info('Deleting contact...')
            client.contact.delete(contact2['id'])


@salesforce
@sdc_min_version('3.12.0')
@pytest.mark.parametrize(('good_or_bad'), [
    'good',
    'bad'
])
def test_salesforce_streaming_api_buffer(sdc_builder, sdc_executor, salesforce, good_or_bad):
    """Testing that pipeline will fail if Streaming Buffer Size is too small,
    succeed if it is ample (SDC-12771).

    Start pipeline with given buffer size, create data using Salesforce client,
    check for error or correct snapshot as appropriate

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        good_or_bad (:obj:`str`): Whether to test the happy path, or a too-small buffer size: 'good' or 'bad'
    """
    client = salesforce.client

    push_topic = None
    contact = None
    pipeline = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        push_topic, push_topic_name = create_push_topic(client)

        # 1048576 is default buffer size
        # 256 is big enough to connect, but not to receive data
        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(query_existing_data=False,
                                         subscribe_for_notifications=True,
                                         subscription_type=PUSH_TOPIC,
                                         push_topic=push_topic_name,
                                         streaming_buffer_size=(1048576 if good_or_bad == 'good' else 256))

        trash = pipeline_builder.add_stage('Trash')
        salesforce_origin >> trash
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline')

        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just create one contact
        logger.info('Creating record using Salesforce client...')
        contact = client.Contact.create(DATA_TO_INSERT[0])

        logger.info('Taking snapshot')
        if good_or_bad == 'good':
            snapshot = snapshot_command.wait_for_finished().snapshot

            verify_snapshot(snapshot, salesforce_origin, [DATA_TO_INSERT[0]])
        else:
            # Pipeline should stop with StageException
            with pytest.raises(Exception):
                snapshot_command.wait_for_finished().snapshot

            status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
            assert 'RUN_ERROR' == status
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if push_topic:
            client.PushTopic.delete(push_topic)
        if contact:
            client.contact.delete(contact['id'])


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_no_more_data(sdc_builder, sdc_executor, salesforce, api):
    """Test for SDC-12418 - Salesforce origin should only generate no-more-data if query returns zero rows. Queries with
    a LIMIT clause were generating a no-more-data event after a single query ran, instead of repeating the query
    until no rows are returned.

    The pipeline looks like:
        salesforce_origin >> trash
        salesforce_origin >= finisher

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Constrain the size of the result set with LIMIT clause; pipeline should still retrieve all the data before
    # triggering no-more-data
    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query="SELECT Id, Name FROM Contact WHERE Id > '${OFFSET}' ORDER BY Id LIMIT 10",
                                     repeat_query='INCREMENTAL',
                                     query_interval='${1 * SECONDS}',
                                     subscribe_for_notifications=False,
                                     use_bulk_api=(api == 'bulk'))

    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    salesforce_origin >= finisher
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    contact_ids = None
    try:
        # Make a load of Contacts
        data_to_insert = []
        for _ in range(CONTACTS_FOR_NO_MORE_DATA):
            data_to_insert.append({'FirstName': get_random_string(string.ascii_letters, 10).lower(),
                                   'LastName': get_random_string(string.ascii_letters, 10).lower(),
                                   'Email': f'{get_random_string(string.ascii_letters, 10).lower()}@example.com'})

        logger.info('Creating Contact records using Salesforce client ...')
        contact_ids = _get_ids(client.bulk.Contact.insert(data_to_insert), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)

        logger.info('Waiting for pipeline to finish')
        sdc_executor.get_pipeline_status(pipeline).wait_for_status(status = 'FINISHED', timeout_sec = 120)

        logger.info('Getting pipeline history')
        history = sdc_executor.get_pipeline_history(pipeline)

        # We should see all the records we created as input, and all of them plus the event record as output
        input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        error_records = history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count
        assert input_records == CONTACTS_FOR_NO_MORE_DATA, f'Observed {input_records} input records (expected {CONTACTS_FOR_NO_MORE_DATA})'
        assert output_records == CONTACTS_FOR_NO_MORE_DATA + 1, f'Observed {output_records} output records (expected {CONTACTS_FOR_NO_MORE_DATA + 1})'
        assert error_records == 0, f'Observed {error_records} error records (expected 0)'

    finally:
        _clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
def test_salesforce_destination_null_relationship(sdc_builder, sdc_executor, salesforce):
    """Test that we can clear related external ID fields (SDC-12704).
    Only applicable to SOAP API as Bulk API does not allow this.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = _get_ids(client.bulk.Contact.insert(DATA_TO_INSERT), 'id')

        # Link the records via ReportsToId
        logger.info('Updating rows using Salesforce client ...')
        data_for_update = [{'Id': inserted_ids[1]["Id"], 'ReportsToId': inserted_ids[0]["Id"]},
                           {'Id': inserted_ids[2]["Id"], 'ReportsToId': inserted_ids[1]["Id"]}]
        client.bulk.Contact.update(data_for_update)

        # Now disconnect the created contacts from each other
        csv_data_to_insert = ['Id,ReportsTo.Email']
        csv_data_to_insert.append(f'{inserted_ids[1]["Id"]},')
        csv_data_to_insert.append(f'{inserted_ids[2]["Id"]},')

        pipeline_builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, csv_data_to_insert)

        salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
        field_mapping = [{'sdcField': '/Id', 'salesforceField': 'Id'},
                         {'sdcField': '/ReportsTo.Email', 'salesforceField': 'ReportsTo.Email'}]
        salesforce_destination.set_attributes(default_operation='UPDATE',
                                              field_mapping=field_mapping,
                                              sobject_type=CONTACT,
                                              use_bulk_api=False)

        dev_raw_data_source >> salesforce_destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        # Now the pipeline will make the contacts report to each other
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        # Using Salesforce connection, read the contents in the Salesforce destination.
        query_str = ("SELECT Id, Email, ReportsToId FROM Contact WHERE Email LIKE 'xtest%'")
        result = client.query(query_str)

        # Nobody should report to anybody any more
        assert None == result['records'][0]['ReportsToId']
        assert None == result['records'][1]['ReportsToId']
        assert None == result['records'][2]['ReportsToId']

    finally:
        _clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_destination_polymorphic(sdc_builder, sdc_executor, salesforce, api):
    """Test that we can write to polymorphic external ID fields (SDC-13117).
    Create a case, since its owner can be a user or a group.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    client = salesforce.client
    case_id = None
    try:
        # Using Salesforce client, create a Case
        logger.info('Creating rows using Salesforce client ...')
        result = client.Case.create({'Subject': CASE_SUBJECT})
        case_id = result['id']

        # Set the case owner. Even though we're not changing the owner, SDC-13117 would cause an error to
        # be thrown due to the bad syntax for the field name
        csv_data_to_insert = ['Id,Owner']
        csv_data_to_insert.append(f'{case_id},{salesforce.username}')

        pipeline_builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, csv_data_to_insert)

        salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
        field_mapping = [{'sdcField': '/Id', 'salesforceField': 'Id'},
                         {'sdcField': '/Owner', 'salesforceField': 'User:Owner.Username'}]
        salesforce_destination.set_attributes(default_operation='UPDATE',
                                              field_mapping=field_mapping,
                                              sobject_type='Case',
                                              use_bulk_api=(api == 'bulk'))

        dev_raw_data_source >> salesforce_destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        # Now the pipeline will update the Case
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        # Using Salesforce connection, read the Case, just to check
        query_str = (f"SELECT Id, Subject, Owner.Username FROM Case WHERE Id = '{case_id}'")
        result = client.query(query_str)

        assert 1 == len(result['records'])
        assert case_id == result['records'][0]['Id']
        assert CASE_SUBJECT == result['records'][0]['Subject']
        assert salesforce.username == result['records'][0]['Owner']['Username']

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (case_id):
            client.Case.delete(case_id)


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_datetime_in_history(sdc_builder, sdc_executor, salesforce, api):
    """Test SDC-12334 - field history data is untyped in the Salesforce schema, since OldValue and NewValue depend on
    the field that changed. For some datatypes, the XML holds type information in an xmltype attribute. We were using
    this to create the correct SDC field type, but not handling datetimes, throwing a FORCE_04 error.

    ActivatedDate on Contract is one of the few datetime fields that will show up in a standard object's field history.

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    client = salesforce.client

    try:
        # Create an account
        acc = client.Account.create({'Name': str(uuid4())})

        # Create a contract for that account
        con = client.Contract.create({'AccountId': acc['id']})

        # Update the contract status - this will have the side effect of updating ActivatedDate
        client.Contract.update(con['id'], {'Status': 'Activated'})

        query = f"SELECT Id, NewValue FROM ContractHistory WHERE Field = 'ActivatedDate' AND ContractId = '{con['id']}'"

        pipeline_builder = sdc_builder.get_pipeline_builder()

        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(soql_query=query,
                                         disable_query_validation=True,
                                         use_bulk_api=(api == 'bulk'),
                                         subscribe_for_notifications=False)
        trash = pipeline_builder.add_stage('Trash')
        salesforce_origin >> trash
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with Id and NewValue fields. For SOAP API, NewValue should be a DATETIME, for
        # Bulk API it's a STRING
        assert len(snapshot[salesforce_origin].output) == 1
        assert snapshot[salesforce_origin].output[0].field['Id']
        if api == 'soap':
            assert snapshot[salesforce_origin].output[0].field['NewValue'].type == 'DATETIME'
        else:
            assert snapshot[salesforce_origin].output[0].field['NewValue'].type == 'STRING'

    finally:
        if con and con['id']:
            client.Contract.delete(con['id'])
        if acc and acc['id']:
            client.Account.delete(acc['id'])


@salesforce
def test_salesforce_origin_query_cdc_no_object(sdc_builder, sdc_executor, salesforce):
    """Test SDC-12378 - enabling CDC with blank object name ('get notifications for all objects') was causing
    initial query to fail.

    Create data using Salesforce client and then check if Salesforce origin receives them using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = ("SELECT Id, FirstName, LastName, Email, LeadSource FROM Contact "
             "WHERE Id > '000000000000000' AND "
             "Email LIKE 'xtest%' "
             "ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     subscribe_for_notifications=True,
                                     subscription_type=CDC,
                                     change_data_capture_object='')

    trash = pipeline_builder.add_stage('Trash')
    salesforce_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    verify_by_snapshot(sdc_executor, pipeline, salesforce_origin, DATA_TO_INSERT, salesforce)


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


@salesforce
def test_einstein_analytics_destination(sdc_builder, sdc_executor, salesforce):
    """Basic test for Einstein Analytics destination. Write some data and check that it's there

    The pipeline looks like:
        dev_raw_data_source >> delay >> einstein_analytics_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    client = salesforce.client

    id = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, CSV_DATA_TO_INSERT)

        # Delay so that we can stop the pipeline after a single batch is processed
        delay = pipeline_builder.add_stage('Delay')
        delay.delay_between_batches = 5*1000

        analytics_destination = pipeline_builder.add_stage('Einstein Analytics', type='destination')
        edgemart_alias = get_random_string(string.ascii_letters, 10).lower()
        # Explicitly set auth credentials since Salesforce environment doesn't know about Einstein Analytics destination
        analytics_destination.set_attributes(edgemart_alias=edgemart_alias,
                                             username=salesforce.username,
                                             password=salesforce.password,
                                             auth_endpoint='test.salesforce.com')

        dev_raw_data_source >> delay >> analytics_destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        # Now the pipeline will write data to Einstein Analytics
        logger.info('Starting Einstein Analytics destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        # Einstein Analytics data load is asynchronous, so poll until it's done
        logger.info('Looking for dataset in Einstein Analytics')
        end_time = datetime.now() + timedelta(seconds=60)
        while not id and datetime.now() < end_time:
            sleep(1)
            id, currentVersionId = find_dataset(client, edgemart_alias)

        # Make sure we found a dataset and didn't time out!
        assert id != None

        # Now query the data from Einstein Analytics using SAQL

        # Build the load statement
        load = f'q = load \"{id}/{currentVersionId}\";'

        # Build the identity projection - e.g.
        # q = foreach q generate Email as Email, FirstName as FirstName, LastName as LastName, LeadSource as LeadSource;
        field_list = []
        for key in DATA_TO_INSERT[0]:
            field_list.append(f'{key} as {key}')
        projection = 'q = foreach q generate ' + ', '.join(field_list) + ';'

        # Ensure consistent ordering
        order_key = 'Email'
        ordering = f'q = order q by {order_key};'

        logger.info('Querying Einstein Analytics')
        response = client.restful('wave/query', method='POST', json={'query': load + projection + ordering})

        assert sorted(DATA_TO_INSERT, key=itemgetter(order_key)) == response['results']['records']

    finally:
        if id:
            # simple_salesforce assumes there will be a JSON response,
            # but DELETE returns 204 with no response
            # See https://github.com/simple-salesforce/simple-salesforce/issues/327
            try:
                logger.info('Deleting dataset in Einstein Analytics')
                client.restful(f'wave/datasets/{id}', method='DELETE')
            except JSONDecodeError:
                pass
