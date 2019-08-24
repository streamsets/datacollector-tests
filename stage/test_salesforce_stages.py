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
from uuid import uuid4

import pytest
import requests
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string

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

# Folder for Documents
FOLDER_NAME = 'TestFolder'

CASE_SUBJECT = 'Test Case'

ACCOUNTS_FOR_SUBQUERY = 5
CONTACTS_FOR_SUBQUERY = 5

CONTACTS_FOR_NO_MORE_DATA = 100

@salesforce
def test_salesforce_destination(sdc_builder, sdc_executor, salesforce):
    """
    Send text to Salesforce destination from Dev Raw Data Source and
    confirm that Salesforce destination successfully reads them using Salesforce client.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination
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
                                          sobject_type='Contact')

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build(title='Salesforce destination').configure_for_environment(salesforce)
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
        read_ids = [{'Id': item['Id']} for item in result['records']]

        assert CSV_DATA_TO_INSERT[1:] == read_data

    finally:
        logger.info('Deleting records ...')
        client.bulk.Contact.delete(read_ids)


@salesforce
@sdc_min_version('3.11.0')
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_destination_default_mapping(sdc_builder, sdc_executor, salesforce, api):
    """
    Send text to Salesforce destination from Dev Raw Data Source and
    confirm that Salesforce destination successfully reads them using Salesforce client.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination
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
                                          sobject_type='Contact')

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
        read_ids = [{'Id': item['Id']} for item in result['records']]

        # Read data should match the original data, before we changed the field names
        assert CSV_DATA_TO_INSERT[1:] == read_data

    finally:
        logger.info('Deleting records ...')
        client.bulk.Contact.delete(read_ids)


# Testing of SDC-10475
@salesforce
def test_salesforce_destination_commit_before_stopping(sdc_builder, sdc_executor, salesforce):
    """
    Send text to Salesforce destination from Dev Raw Data Source and
    confirm that Salesforce destination successfully reads them using Salesforce client.

    It verifies that SalesForce destination is able to write data without stopping the pipeline.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination
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
                                          sobject_type='Contact')

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build(title='Salesforce destination').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

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
        read_ids = [{'Id': item['Id']} for item in result['records']]

        assert CSV_DATA_TO_INSERT[1:] == read_data

        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count > 3

        # That will mean that duplicated are marked as error, but at the same time means that is able to commit
        # records to SalesForce
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count > 0

    finally:
        logger.info('Deleting records ...')
        client.bulk.Contact.delete(read_ids)


@salesforce
@pytest.mark.parametrize(('condition'), [
    'query_without_prefix',
    # Testing of SDC-9067
    'query_with_prefix'
])
def test_salesforce_origin(sdc_builder, sdc_executor, salesforce, condition):
    """
    Create data using Salesforce client
    and then check if Salesforce origin receives them using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash
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
    pipeline = pipeline_builder.build(title='Salesforce Origin').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    verify_by_snapshot(sdc_executor, pipeline, salesforce_origin, DATA_TO_INSERT, salesforce)


# Test of SDC-11086
@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_nulls(sdc_builder, sdc_executor, salesforce, api):
    """
    Create data using Salesforce client
    and then check if Salesforce origin receives them using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash
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
    pipeline = pipeline_builder.build(title='Salesforce Origin').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    verify_by_snapshot(sdc_executor, pipeline, salesforce_origin, expected_data, salesforce)


def get_dev_raw_data_source(pipeline_builder, raw_data):
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data))
    return dev_raw_data_source


def verify_snapshot(snapshot, stage_name, expected_data):
    rows_from_snapshot = [record.value['value']
                          for record in snapshot[stage_name].output]

    inserted_ids = [dict([(rec['sqpath'].strip('/'), rec['value'])
                          for rec in item if rec['sqpath'] == '/Id'])
                    for item in rows_from_snapshot]

    # Verify correct rows are received using snaphot.
    data_from_snapshot = [dict([(rec['sqpath'].strip('/'), rec['value'])
                                for rec in item if rec['sqpath'] != '/Id' and rec['sqpath'] != '/SystemModstamp'])
                          for item in rows_from_snapshot]

    data_from_snapshot = sorted(data_from_snapshot, key=lambda k: k['FirstName'])

    assert data_from_snapshot == expected_data

    return inserted_ids


def verify_by_snapshot(sdc_executor, pipeline, stage_name, expected_data, salesforce, data_to_insert=DATA_TO_INSERT):
    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        client.bulk.Contact.insert(data_to_insert)

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        inserted_ids = verify_snapshot(snapshot, stage_name, expected_data)

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (inserted_ids):
            client.bulk.Contact.delete(inserted_ids)


# Test of SDC-10352
@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_datetime(sdc_builder, sdc_executor, salesforce, api):
    """
    Create data using Salesforce client
    and then check if Salesforce origin receives them using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash
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
    pipeline = pipeline_builder.build(title='Salesforce Origin').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        client.bulk.Contact.insert(DATA_TO_INSERT)

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, timeout_sec=60).snapshot

        rows_from_snapshot = [record.value['value']
                              for record in snapshot[salesforce_origin].output]

        inserted_ids = [dict([(rec['sqpath'].strip('/'), rec['value'])
                              for rec in item if rec['sqpath'] == '/Id'])
                        for item in rows_from_snapshot]

        # Verify correct rows are received using snapshot.
        data_from_snapshot = [dict([(rec['sqpath'].strip('/'), rec['value'])
                                    for rec in item if rec['sqpath'] != '/Id' and rec['sqpath'] != '/SystemModstamp'])
                              for item in rows_from_snapshot]

        data_from_snapshot = sorted(data_from_snapshot, key=lambda k: k['FirstName'])

        assert data_from_snapshot == DATA_TO_INSERT

        # SDC-10773 - source IDs must be unique
        source_ids = {contact.header['sourceId'] for contact in snapshot[salesforce_origin].output}
        assert len(source_ids) == len(snapshot[salesforce_origin].output)

        sdc_executor.stop_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, timeout_sec=60).snapshot
        rows_from_snapshot = [record.value['value']
                              for record in snapshot[salesforce_origin].output]

        assert len(rows_from_snapshot) == 0

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (inserted_ids):
            client.bulk.Contact.delete(inserted_ids)


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
    pipeline = pipeline_builder.build(title='Salesforce Lookup').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    LOOKUP_EXPECTED_DATA = copy.deepcopy(data)
    for record in LOOKUP_EXPECTED_DATA:
        record['surName'] = record.pop('LastName')
    verify_by_snapshot(sdc_executor, pipeline, salesforce_lookup, LOOKUP_EXPECTED_DATA,
                       salesforce, data_to_insert=data)


# Test SDC-9251, SDC-9493
@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_subquery(sdc_builder, sdc_executor, salesforce, api):
    """
    Create data using Salesforce client
    and then check if Salesforce origin receives them using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash
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
    pipeline = pipeline_builder.build(title=f'Salesforce Origin Subquery {api}').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    try:
        expected_accounts = []
        account_ids = []
        logger.info('Creating accounts using Salesforce client ...')
        for i in range(ACCOUNTS_FOR_SUBQUERY):
            expected_accounts.append({'Name': f'Account {i}'})

        result = client.bulk.Account.insert(expected_accounts)
        account_ids = [{'Id': item['id']}
                       for item in result]

        expected_contacts = []
        contact_ids = []
        logger.info('Creating contacts using Salesforce client ...')
        for i in range(len(expected_accounts)):
            for j in range(CONTACTS_FOR_SUBQUERY):
                contact = {'AccountId': account_ids[i]['Id'], 'LastName': f'Contact {i} {j}'}
                expected_contacts.append(contact)

        result = client.bulk.Contact.insert(expected_contacts)
        contact_ids = [{'Id': item['id']}
                       for item in result]

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # Verify correct rows are received using snapshot.
        assert ACCOUNTS_FOR_SUBQUERY == len(snapshot[salesforce_origin].output)
        for i, account in enumerate(snapshot[salesforce_origin].output):
            assert expected_accounts[i]['Name'] == account.get_field_data('/Name')
            assert CONTACTS_FOR_SUBQUERY == len(account.get_field_data('/Contacts'))
            for j in range(CONTACTS_FOR_SUBQUERY):
                assert expected_contacts[(i * 5) + j]['LastName'] == account.get_field_data(f'/Contacts/{j}/LastName')

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if account_ids:
            client.bulk.Account.delete(account_ids)
        if contact_ids:
            client.bulk.Contact.delete(contact_ids)


# Test SDC-10694
@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_origin_aggregate_count(sdc_builder, sdc_executor, salesforce):
    """
    Create data using Salesforce client
    and then check if Salesforce origin retrieves correct aggregate data using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash
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
    pipeline = pipeline_builder.build(title='Salesforce Origin Aggregate Count').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        result = client.bulk.Contact.insert(DATA_TO_INSERT)
        contact_ids = [{'Id': item['id']}
                       for item in result]

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with a count field
        assert len(snapshot[salesforce_origin].output) == 1
        assert snapshot[salesforce_origin].output[0].get_field_data('/count') == 3

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        if contact_ids:
            logger.info('Deleting records ...')
            client.bulk.Contact.delete(contact_ids)


@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_origin_aggregate(sdc_builder, sdc_executor, salesforce):
    """
    Create data using Salesforce client
    and then check if Salesforce origin retrieves correct aggregate data using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash
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
    pipeline = pipeline_builder.build(title='Salesforce Origin Aggregate').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

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
        result = client.bulk.Account.insert(data_to_insert)
        account_ids = [{'Id': item['id']}
                       for item in result]

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with a count field
        assert len(snapshot[salesforce_origin].output) == 1
        assert snapshot[salesforce_origin].output[0].get_field_data('/expr0') == 3
        assert snapshot[salesforce_origin].output[0].get_field_data('/expr1') == 3
        assert snapshot[salesforce_origin].output[0].get_field_data('/expr2') == 'Agriculture'
        assert snapshot[salesforce_origin].output[0].get_field_data('/expr3') == 1368

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
    pipeline = pipeline_builder.build(title='Salesforce Lookup Aggregate').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        result = client.bulk.Contact.insert(DATA_TO_INSERT)
        contact_ids = [{'Id': item['id']}
                       for item in result]

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with a /count field containing an integer
        assert len(snapshot[salesforce_lookup].output) == 1
        assert snapshot[salesforce_lookup].output[0].get_field_data('/count') == 3

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        if contact_ids:
            logger.info('Deleting records ...')
            client.bulk.Contact.delete(contact_ids)


@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_lookup_aggregate(sdc_builder, sdc_executor, salesforce):
    """Simple Salesforce Lookup processor test.
    Pipeline will enrich records with the number of contacts whose FirstName
    matches the /prefix field, adding the value as a string in the /count field

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> trash
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
    pipeline = pipeline_builder.build(title='Salesforce Lookup Aggregate').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    try:
        contact_ids = []
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        result = client.bulk.Contact.insert(DATA_TO_INSERT)
        contact_ids = [{'Id': item['id']}
                       for item in result]

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # There should be a single row with a /count field containing three strings
        assert len(snapshot[salesforce_lookup].output) == 1
        assert snapshot[salesforce_lookup].output[0].get_field_data('/count') == str(len(DATA_TO_INSERT))
        assert snapshot[salesforce_lookup].output[0].get_field_data('/expr1') == DATA_TO_INSERT[0]['FirstName']
        assert snapshot[salesforce_lookup].output[0].get_field_data('/expr2') == DATA_TO_INSERT[-1]['LastName']

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        if contact_ids:
            logger.info('Deleting records ...')
            client.bulk.Contact.delete(contact_ids)


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_session_timeout(sdc_builder, sdc_executor, salesforce, api):
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
    pipeline = pipeline_builder.build(title='Salesforce Origin').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        logger.info('Creating rows using Salesforce client ...')
        client.bulk.Contact.insert(DATA_TO_INSERT)

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
        inserted_ids = verify_snapshot(snapshot, salesforce_origin, DATA_TO_INSERT)

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (inserted_ids):
            client.bulk.Contact.delete(inserted_ids)

@salesforce
def test_salesforce_origin_stop_resume(sdc_builder, sdc_executor, salesforce):
    """
    Create data using Salesforce client, stop the pipeline
    and then check if Salesforce origin receives them using snapshot.
    Insert more data and check again.

    The pipeline looks like:
        salesforce_origin >> trash
        salesforce_origin >= trash_2
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

    pipeline = pipeline_builder.build(title='Salesforce Origin').configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)


    client = salesforce.client
    inserted_ids_1 = None
    inserted_ids_2 = None

    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        client.bulk.Contact.insert(DATA_TO_INSERT)

        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        rows_from_snapshot = [record.value['value']
                              for record in snapshot[salesforce_origin].output]

        inserted_ids_1 = [dict([(rec['sqpath'].strip('/'), rec['value'])
                              for rec in item if rec['sqpath'] == '/Id'])
                        for item in rows_from_snapshot]

        # Verify correct rows are received using snaphot.
        data_from_snapshot = [dict([(rec['sqpath'].strip('/'), rec['value'])
                                    for rec in item if rec['sqpath'] != '/Id' and rec['sqpath'] != '/SystemModstamp'])
                              for item in rows_from_snapshot]

        data_from_snapshot = sorted(data_from_snapshot, key=lambda k: k['FirstName'])

        assert data_from_snapshot == DATA_TO_INSERT

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
        client.bulk.Contact.insert(DATA_TO_INSERT_2)

        logger.info('Starting pipeline and snapshot')
        snapshot_2 = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        rows_from_snapshot_2 = [record.value['value']
                              for record in snapshot_2[salesforce_origin].output]

        inserted_ids_2 = [dict([(rec['sqpath'].strip('/'), rec['value'])
                              for rec in item if rec['sqpath'] == '/Id'])
                        for item in rows_from_snapshot_2]

        # Verify correct rows are received using snaphot.
        data_from_snapshot_2 = [dict([(rec['sqpath'].strip('/'), rec['value'])
                                    for rec in item if rec['sqpath'] != '/Id' and rec['sqpath'] != '/SystemModstamp'])
                              for item in rows_from_snapshot_2]

        data_from_snapshot_2 = sorted(data_from_snapshot_2, key=lambda k: k['FirstName'])

        assert data_from_snapshot_2 == DATA_TO_INSERT + DATA_TO_INSERT_2

        # stage should produce events...
        assert len(snapshot[salesforce_origin].event_records) == 1
        assert snapshot[salesforce_origin].event_records[0].header.values['sdc.event.type'] == 'no-more-data'

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (inserted_ids_1):
            client.bulk.Contact.delete(inserted_ids_1)
        if (inserted_ids_2):
            client.bulk.Contact.delete(inserted_ids_2)


# Test SDC-12041
@salesforce
def test_salesforce_origin_document(sdc_builder, sdc_executor, salesforce):
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
        assert snapshot[salesforce_origin].output[0].get_field_data('/Id') == inserted_id
        assert snapshot[salesforce_origin].output[0].get_field_data('/Body') == body

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (inserted_id):
            client.Document.delete(inserted_id)


# Test SDC-12193
@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_destination_datetime(sdc_builder, sdc_executor, salesforce, api):
    # Create an Event record as this is one of the few standard objects with a settable Datetime
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

        read_ids = [{'Id': item['Id']} for item in result['records']]

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


# Test SDC-12636
@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_destination_relationship(sdc_builder, sdc_executor, salesforce, api):
    """
    Test that we can write to related external ID fields

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination
    """
    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        result = client.bulk.Contact.insert(DATA_TO_INSERT)
        inserted_ids = [{'Id': item['id']}
                        for item in result]

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
                                              sobject_type='Contact',
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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (inserted_ids):
            client.bulk.Contact.delete(inserted_ids)


def create_push_topic(client):
    push_topic_name = get_random_string(string.ascii_letters, 10).lower()
    logger.info(f'Creating PushTopic {push_topic_name} in Salesforce')
    push_topic = client.PushTopic.create({'Name': push_topic_name,
                                          'Query': 'SELECT Id, FirstName, LastName, Email, LeadSource FROM Contact',
                                          'ApiVersion': '47.0',
                                          'NotifyForOperationCreate': True,
                                          'NotifyForOperationUpdate': True,
                                          'NotifyForOperationUndelete': True,
                                          'NotifyForOperationDelete': True,
                                          'NotifyForFields': 'All'})
    return push_topic, push_topic_name


@salesforce
def test_salesforce_streaming_api(sdc_builder, sdc_executor, salesforce):
    """
    Start pipeline, create data using Salesforce client
    and then check if Salesforce origin receives data using snapshot.

    The pipeline looks like:
        salesforce_origin >> trash
    """
    client = salesforce.client

    push_topic = None
    contact = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        push_topic, push_topic_name = create_push_topic(client)

        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(query_existing_data=False,
                                         subscribe_for_notifications=True,
                                         subscription_type='PUSH_TOPIC',
                                         push_topic=push_topic_name)

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

        verify_snapshot(snapshot, salesforce_origin, [DATA_TO_INSERT[0]])

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if push_topic:
            client.PushTopic.delete(push_topic['id'])
        if contact:
            client.contact.delete(contact['id'])


# Test SDC-12771
@salesforce
@sdc_min_version('3.12.0')
@pytest.mark.parametrize(('good_or_bad'), [
    'good',
    'bad'
])
def test_salesforce_streaming_api_buffer(sdc_builder, sdc_executor, salesforce, good_or_bad):
    """
    Testing that pipeline will fail if Streaming Buffer Size is too small, succeed if it is ample

    Start pipeline with given buffer size, create data using Salesforce client,
    check for error or correct snapshot as appropriate

    The pipeline looks like:
        salesforce_origin >> trash
    """
    client = salesforce.client

    push_topic = None
    contact = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        push_topic, push_topic_name = create_push_topic(client)

        # 1048576 is default buffer size
        # 256 is big enough to connect, but not to receive data
        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(query_existing_data=False,
                                         subscribe_for_notifications=True,
                                         subscription_type='PUSH_TOPIC',
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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if push_topic:
            client.PushTopic.delete(push_topic['id'])
        if contact:
            client.contact.delete(contact['id'])


@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_origin_no_more_data(sdc_builder, sdc_executor, salesforce, api):
    """
    Test for SDC-12418 - Salesforce origin should only generate no-more-data if query returns zero rows. Queries with
    a LIMIT clause were generating a no-more-data event after a single query ran, instead of repeating the query
    until no rows are returned
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
        result = client.bulk.Contact.insert(data_to_insert)
        contact_ids = [{'Id': item['id']}
                       for item in result]

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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (contact_ids):
            client.bulk.Contact.delete(contact_ids)


# Test SDC-12704
@salesforce
def test_salesforce_destination_null_relationship(sdc_builder, sdc_executor, salesforce):
    """
    Test that we can clear related external ID fields. Only applicable to SOAP API as Bulk API does not allow this

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination
    """
    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        result = client.bulk.Contact.insert(DATA_TO_INSERT)
        inserted_ids = [{'Id': item['id']}
                        for item in result]

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
                                              sobject_type='Contact',
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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if (inserted_ids):
            client.bulk.Contact.delete(inserted_ids)


# Test SDC-13117
@salesforce
@pytest.mark.parametrize(('api'), [
    'soap',
    'bulk'
])
def test_salesforce_destination_polymorphic(sdc_builder, sdc_executor, salesforce, api):
    """
    Test that we can write to polymorphic external ID fields

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination
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
    """
    Test SDC-12334 - field history data is untyped in the Salesforce schema, since OldValue and NewValue depend on the
    field that changed. For some datatypes, the XML holds type information in an xmltype attribute. We were using this
    to create the correct SDC field type, but not handling datetimes, throwing a FORCE_04 error.

    ActivatedDate on Contract is one of the few datetime fields that will show up in a standard object's field history.
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

        print(snapshot[salesforce_origin].output[0].field['NewValue'])

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
