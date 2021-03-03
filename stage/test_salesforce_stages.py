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
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
from operator import itemgetter
from time import sleep
from uuid import uuid4
from xml.sax.saxutils import escape

import pytest
import requests
from sfdclib import SfdcSession, SfdcMetadataApi
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

from .utils.utils_salesforce import set_up_random, TEST_DATA, get_dev_raw_data_source, _insert_data_and_verify_using_wiretap, \
    _verify_wiretap_data, get_ids, clean_up, TIMEOUT, create_push_topic, enable_cdc, verify_cdc_wiretap, disable_cdc, \
    add_custom_field_to_contact, delete_custom_field_from_contact, FOLDER_NAME, CASE_SUBJECT, \
    find_dataset_include_timestamp, find_dataset, CONTACTS_FOR_NO_MORE_DATA, ACCOUNTS_FOR_SUBQUERY, \
    CONTACTS_FOR_SUBQUERY

CONTACT = 'Contact'
CDC = 'CDC'
ALL_EVENTS = 'ALL_EVENTS'
PUSH_TOPIC = 'PUSH_TOPIC'
API_VERSION = '47.0'
COLON = ':'
PERIOD = '.'

logger = logging.getLogger(__name__)


@salesforce
@pytest.fixture(autouse=True)
def _set_up_random(salesforce):
    set_up_random(salesforce)


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
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['CSV_DATA_TO_INSERT'])

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
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Using Salesforce connection, read the contents in the Salesforce destination.
        # Changing " with ' and vice versa in following string makes the query execution fail.
        query_str = ("SELECT Id, FirstName, LastName, Email, LeadSource "
                     f"FROM Contact WHERE Email LIKE \'xtest%\' and Lastname = '{TEST_DATA['STR_15_RANDOM']}'"
                     " ORDER BY Id")
        result = client.query(query_str)

        read_data = [f'{item["FirstName"]},{item["LastName"]},{item["Email"]},{item["LeadSource"]}'
                     for item in result['records']]
        # Following is used later to delete these records.
        read_ids = get_ids(result['records'], 'Id')

        assert TEST_DATA['CSV_DATA_TO_INSERT'][1:] == read_data

    finally:
        clean_up(sdc_executor, pipeline, client, read_ids)


@salesforce
@sdc_min_version('3.11.0')
@pytest.mark.parametrize('api', ['soap', 'bulk'])
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
    TEST_DATA['DATA_TO_INSERT'] = [item.replace('Email', 'em').replace('LeadSource', 'ls') for item in
                                   TEST_DATA['CSV_DATA_TO_INSERT']]
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['DATA_TO_INSERT'])

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
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Using Salesforce connection, read the contents in the Salesforce destination.
        # Changing " with ' and vice versa in following string makes the query execution fail.
        query_str = ("SELECT Id, FirstName, LastName, Email, LeadSource "
                     f"FROM Contact WHERE Email LIKE \'xtest%\' and Lastname = '{TEST_DATA['STR_15_RANDOM']}'"
                     " ORDER BY Id")
        result = client.query(query_str)

        read_data = [f'{item["FirstName"]},{item["LastName"]},{item["Email"]},{item["LeadSource"]}'
                     for item in result['records']]
        # Following is used later to delete these records.
        read_ids = get_ids(result['records'], 'Id')

        # Read data should match the original data, before we changed the field names
        assert TEST_DATA['CSV_DATA_TO_INSERT'][1:] == read_data

    finally:
        clean_up(sdc_executor, pipeline, client, read_ids)


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
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['CSV_DATA_TO_INSERT'])
    dev_raw_data_source.stop_after_first_batch = False

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
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(2)
        sdc_executor.stop_pipeline(pipeline)

        # Using Salesforce connection, read the contents in the Salesforce destination.
        # ORDER BY LastModifiedDate, Email to ensure consistency
        query_str = ("SELECT Id, FirstName, LastName, Email, LeadSource "
                     f"FROM Contact WHERE Email LIKE \'xtest%\' and Lastname = '{TEST_DATA['STR_15_RANDOM']}'"
                     " ORDER BY LastModifiedDate, Email")
        result = client.query(query_str)

        read_data = [f'{item["FirstName"]},{item["LastName"]},{item["Email"]},{item["LeadSource"]}'
                     for item in result['records']]
        # Following is used later to delete these records.
        read_ids = get_ids(result['records'], 'Id')

        # Compare only the first batch of data
        assert TEST_DATA['CSV_DATA_TO_INSERT'][1:] == read_data[:len(TEST_DATA['CSV_DATA_TO_INSERT']) - 1]

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count > 3

    finally:
        clean_up(sdc_executor, pipeline, client, read_ids)


@salesforce
@pytest.mark.parametrize('api', ['soap', 'bulk'])
@pytest.mark.parametrize('prefixed_query', [True, False])  # Testing of SDC-9067
def test_salesforce_origin(sdc_builder, sdc_executor, salesforce, api, prefixed_query):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using wiretap.

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        prefixed_query (:obj:`str`): Whether or not to include SObject prefix in query - True or False
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    if prefixed_query:
        # SDC-9067 - redundant object name prefix caused NPE
        query = ("SELECT Contact.Id, Contact.FirstName, Contact.LastName, Contact.Email, Contact.LeadSource "
                 "FROM Contact WHERE Id > '000000000000000' "
                 f"AND Email LIKE \'xtest%\' and LastName = '{TEST_DATA['STR_15_RANDOM']}'"
                 " ORDER BY Id")
    else:
        query = ("SELECT Id, FirstName, LastName, Email, LeadSource FROM Contact "
                 "WHERE Id > '000000000000000' AND "
                 f"Email LIKE \'xtest%\' and LastName = '{TEST_DATA['STR_15_RANDOM']}'"
                 " ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    # Changing " with ' and vice versa in following string makes the query execution fail.
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=(api == 'bulk'),
                                     subscribe_for_notifications=False)

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    _insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, TEST_DATA['DATA_TO_INSERT'], salesforce,
                                          TEST_DATA['DATA_TO_INSERT'])


@salesforce
@pytest.mark.parametrize('api', ['soap', 'bulk'])
def test_salesforce_origin_nulls(sdc_builder, sdc_executor, salesforce, api):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using wiretap. This test checks that nulls are correctly
    read for fields that are not set in Salesforce (SDC-11086).

    The pipeline looks like:
        salesforce_origin >> wiretap

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
             f"LastName = '{TEST_DATA['STR_15_RANDOM']}'"
             "ORDER BY Id")

    expected_data = [
        {'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Description': None, 'HomePhone': None},
        {'FirstName': 'Test2', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Description': None, 'HomePhone': None},
        {'FirstName': 'Test3', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Description': None, 'HomePhone': None}]

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=(api == 'bulk'),
                                     subscribe_for_notifications=False)

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    _insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, expected_data, salesforce, expected_data)


@salesforce
@pytest.mark.parametrize('api', ['soap', 'bulk'])
def test_salesforce_origin_datetime(sdc_builder, sdc_executor, salesforce, api):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using wiretap. This test checks that datetime fields can
    be used as offsets (SDC-10352).

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = ("SELECT Id, FirstName, LastName, Email, LeadSource, SystemModstamp "
             "FROM Contact WHERE SystemModstamp > ${OFFSET} and "
             f"Lastname = '{TEST_DATA['STR_15_RANDOM']}'"
             " ORDER BY SystemModstamp")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    # Changing " with ' and vice versa in following string makes the query execution fail.
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=(api == 'bulk'),
                                     subscribe_for_notifications=False,
                                     repeat_query='INCREMENTAL',
                                     query_interval='${24 * HOURS}',
                                     initial_offset='2018-10-16T00:00:00.000Z',
                                     offset_field='SystemModstamp')

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
        sdc_executor.stop_pipeline(pipeline)

        _verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

        wiretap.reset()
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 0)
        sdc_executor.stop_pipeline(pipeline)


        assert len([record.value['value'] for record in wiretap.output_records]) == 0
    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
@pytest.mark.parametrize('api', ['soap', 'bulk'])
@pytest.mark.parametrize('data_with_from_email', [False, True])  # Testing of SDC-7548
@pytest.mark.parametrize('query_with_time', [True, False])  # Testing of SDC-10207
def test_salesforce_lookup_processor(sdc_builder, sdc_executor, salesforce, api, data_with_from_email, query_with_time):
    """Simple Salesforce Lookup processor test.
    Pipeline will enrich records with the 'FirstName' of contacts by adding a field as 'surName'.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        data_with_from_email (:obj:`list`): Dataset to use in test
        query_with_time (:obj:`bool`): Whether or not to filter results by time
    """
    if api == 'bulk' and Version(sdc_builder.version) < Version('3.16.0'):
        pytest.skip('Skipping... Bulk API is not supported in Salesforce Lookup Processor until 3.16.0')

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Parametrizing directly the global variables does not work
    if data_with_from_email:
        TEST_DATA['DATA_TO_INSERT'] = TEST_DATA['DATA_WITH_FROM_IN_EMAIL']
    else:
        TEST_DATA['DATA_TO_INSERT'] = TEST_DATA['DATA_TO_INSERT']

    lookup_data = ['Email'] + [row['Email'] for row in TEST_DATA['DATA_TO_INSERT']]
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

    salesforce_lookup = pipeline_builder.add_stage('Salesforce Lookup')
    # Changing " with ' and vice versa in following string makes the query execution fail.
    query_str = ("SELECT Id, FirstName, LastName, LeadSource FROM Contact "
                 "WHERE Email = '${record:value(\"/Email\")}' AND "
                 f"Lastname = '{TEST_DATA['STR_15_RANDOM']}'")

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
    if Version(sdc_builder.version) >= Version('3.16.0'):
        salesforce_lookup.set_attributes(use_bulk_api=(api == 'bulk'))

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> salesforce_lookup >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    LOOKUP_EXPECTED_DATA = copy.deepcopy(TEST_DATA['DATA_TO_INSERT'])
    for record in LOOKUP_EXPECTED_DATA:
        record['surName'] = record.pop('LastName')
    _insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, LOOKUP_EXPECTED_DATA,
                                          salesforce, TEST_DATA['DATA_TO_INSERT'])


@salesforce
def test_salesforce_lookup_processor_retrieve(sdc_builder, sdc_executor, salesforce):
    """Simple Salesforce Lookup processor test - retrieve by record id rather than query
    Pipeline will enrich records with the 'FirstName' of contacts by adding a field as 'surName'.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    client = salesforce.client

    # Using Salesforce client, create rows in Contact.
    logger.info('Creating rows using Salesforce client ...')
    contact_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Lookup data is record Id's
    lookup_data = ['Id'] + [row['Id'] for row in contact_ids]
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

    salesforce_lookup = pipeline_builder.add_stage('Salesforce Lookup')

    # Map FirstName to surName
    field_mappings = [dict(dataType='USE_SALESFORCE_TYPE',
                           salesforceField='FirstName',
                           sdcField='/surName')]
    # Ask for all of the fields we set, so that we can assert their values
    salesforce_lookup.set_attributes(lookup_mode='RETRIEVE',
                                     id_field='/Id',
                                     salesforce_fields=','.join(TEST_DATA['DATA_TO_INSERT'][0].keys()),
                                     object_type=CONTACT,
                                     field_mappings=field_mappings)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> salesforce_lookup >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We need to look for surName field instead of LastName
        lookup_expected_data = copy.deepcopy(TEST_DATA['DATA_TO_INSERT'])
        for record in lookup_expected_data:
            record['surName'] = record.pop('FirstName')

        _verify_wiretap_data(wiretap, lookup_expected_data)

    finally:
        clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
@sdc_min_version('3.14.0')
@pytest.mark.parametrize('missing_values_behavior', ['SEND_TO_ERROR', 'PASS_RECORD_ON'])
def test_salesforce_retrieve_deleted_record(sdc_builder, sdc_executor, salesforce, missing_values_behavior):
    """Test SDC-13390 - attempt to retrieve a deleted record
    Pipeline will attempt to enrich records with the 'FirstName' of contacts by
    adding a field as 'surName'. Depending on missing_values_behavior, the
    record with the deleted id should be passed along the pipeline or sent to
    error.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> wiretap
    """
    client = salesforce.client
    contact_ids = None
    pipeline = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        result = client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT'])
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
                               salesforceField='FirstName',
                               sdcField='/surName')]

        # Ask for all of the fields we set, so that we can assert their values
        # If we don't find a record in Salesforce, we want to pass the record along the pipeline
        salesforce_lookup.set_attributes(lookup_mode='RETRIEVE',
                                         id_field='/Id',
                                         salesforce_fields=','.join(TEST_DATA['DATA_TO_INSERT'][0].keys()),
                                         object_type=CONTACT,
                                         missing_values_behavior=missing_values_behavior,
                                         field_mappings=field_mappings)

        wiretap = pipeline_builder.add_wiretap()
        dev_raw_data_source >> salesforce_lookup >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We need to look for surName field instead of LastName
        lookup_expected_data = copy.deepcopy(TEST_DATA['DATA_TO_INSERT'])
        for record in lookup_expected_data:
            record['surName'] = record.pop('FirstName')

        rows_from_wiretap = [record.field
                              for record in wiretap.output_records]

        if missing_values_behavior == 'PASS_RECORD_ON':
            # Middle record should just have its Id field
            assert rows_from_wiretap[1]['Id'] == contact_ids[1]['Id']
        else:
            assert len(wiretap.error_records) == 1
            assert wiretap.error_records[0].field['Id'] == contact_ids[1]['Id']

        data_from_wiretap = [{field: record[field] for field in record if field not in ['Id', 'SystemModstamp']}
                              for record in rows_from_wiretap]

        # Remove the middle element(s) so we can do the next assert
        if missing_values_behavior == 'PASS_RECORD_ON':
            del (data_from_wiretap[1])

        del (lookup_expected_data[1])

        assert data_from_wiretap == lookup_expected_data

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        if contact_ids:
            logger.info('Deleting records ...')
            client.bulk.Contact.delete(contact_ids)


# Test SDC-9251, SDC-9493
@salesforce
@pytest.mark.parametrize('api', ['soap', 'bulk'])
def test_salesforce_origin_subquery(sdc_builder, sdc_executor, salesforce, api):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using wiretap. This test focuses on following relationships
    (SDC-9493) and ordering in subqueries (SDC-9251).

    The pipeline looks like:
        salesforce_origin >> wiretap

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
             f"Name LIKE '{TEST_DATA['STR_15_RANDOM']} %' ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=(api == 'bulk'),
                                     subscribe_for_notifications=False)

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
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
            expected_accounts.append({'Name': f"{TEST_DATA['STR_15_RANDOM']} {i}"})

        account_ids = get_ids(client.bulk.Account.insert(expected_accounts), 'id')

        expected_contacts = []
        contact_ids = []
        logger.info('Creating contacts using Salesforce client ...')
        for i in range(len(expected_accounts)):
            for j in range(CONTACTS_FOR_SUBQUERY):
                contact = {'AccountId': account_ids[i]['Id'], 'LastName': f"{TEST_DATA['STR_15_RANDOM']} {i} {j}"}
                expected_contacts.append(contact)

        contact_ids = get_ids(client.bulk.Contact.insert(expected_contacts), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()


        # Verify correct rows are received using snapshot.
        assert ACCOUNTS_FOR_SUBQUERY == len(wiretap.output_records)
        for i, account in enumerate(wiretap.output_records):
            assert expected_accounts[i]['Name'] == account.field['Name'].value
            assert CONTACTS_FOR_SUBQUERY == len(account.field['Contacts'])
            for j in range(CONTACTS_FOR_SUBQUERY):
                assert expected_contacts[(i * 5) + j]['LastName'] == account.field['Contacts'][j]['LastName']

    finally:
        logger.info('Deleting records ...')
        if account_ids:
            client.bulk.Account.delete(account_ids)
        clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_origin_aggregate_count(sdc_builder, sdc_executor, salesforce):
    """Create data using Salesforce client and then check if Salesforce origin
    retrieves correct aggregate data using wiretap (SDC-10694).

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = f"SELECT COUNT() FROM Contact Where LastName = '{TEST_DATA['STR_15_RANDOM']}'"

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=False,
                                     subscribe_for_notifications=False,
                                     disable_query_validation=True)

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    contact_ids = None

    client = salesforce.client
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        contact_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # There should be a single row with a count field
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['count'] == 3

    finally:
        clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_origin_aggregate(sdc_builder, sdc_executor, salesforce):
    """Create data using Salesforce client and then check if Salesforce origin
    retrieves correct aggregate data using wiretap.

    The pipeline looks like:
        salesforce_origin >> wiretap

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
             f"FROM Account where Name like '{TEST_DATA['STR_15_RANDOM']} %'")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=False,
                                     subscribe_for_notifications=False,
                                     disable_query_validation=True)

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    account_ids = None

    client = salesforce.client
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        TEST_DATA['DATA_TO_INSERT'] = [{'Name': f"{TEST_DATA['STR_15_RANDOM']} 1",
                                        'NumberOfEmployees': 1,
                                        'Industry': 'Agriculture',
                                        'AnnualRevenue': 123},
                                       {'Name': f"{TEST_DATA['STR_15_RANDOM']} 2",
                                        'NumberOfEmployees': 2,
                                        'Industry': 'Finance',
                                        'AnnualRevenue': 456},
                                       {'Name': f"{TEST_DATA['STR_15_RANDOM']} 3",
                                        'NumberOfEmployees': 3,
                                        'Industry': 'Utilities',
                                        'AnnualRevenue': 789}]

        account_ids = get_ids(client.bulk.Account.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # There should be a single row with a count field
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['expr0'].value == 3
        assert wiretap.output_records[0].field['expr1'].value == 3
        assert wiretap.output_records[0].field['expr2'].value == 'Agriculture'
        assert wiretap.output_records[0].field['expr3'].value == 1368

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
        dev_raw_data_source >> salesforce_lookup >> wiretap

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
                 "WHERE FirstName LIKE '${record:value(\"/prefix\")}%' "
                 f"AND LastName= '{TEST_DATA['STR_15_RANDOM']}'")

    salesforce_lookup.set_attributes(soql_query=query_str)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> salesforce_lookup >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    contact_ids = None

    client = salesforce.client
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        contact_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # There should be a single row with a /count field containing an integer
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['count'].value == 3

    finally:
        clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
@sdc_min_version('3.8.0')
def test_salesforce_lookup_aggregate(sdc_builder, sdc_executor, salesforce):
    """Simple Salesforce Lookup processor test.
    Pipeline will enrich records with the number of contacts whose FirstName
    matches the /prefix field, adding the value as a string in the /count field

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> wiretap

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
                 "WHERE FirstName LIKE '${record:value(\"/prefix\")}%' "
                 f"AND LastName='{TEST_DATA['STR_15_RANDOM']}'")

    field_mappings = [dict(dataType='STRING',
                           salesforceField='expr0',
                           sdcField='/count')]
    salesforce_lookup.set_attributes(soql_query=query_str,
                                     field_mappings=field_mappings)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> salesforce_lookup >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    contact_ids = None

    client = salesforce.client
    try:
        contact_ids = []
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        contact_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # There should be a single row with a /count field containing three strings
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['count'].value == str(len(TEST_DATA['DATA_TO_INSERT']))
        assert wiretap.output_records[0].field['expr1'].value == TEST_DATA['DATA_TO_INSERT'][0]['FirstName']
        assert wiretap.output_records[0].field['expr2'].value == TEST_DATA['DATA_TO_INSERT'][-1]['LastName']

    finally:
        clean_up(sdc_executor, pipeline, client, contact_ids)


@salesforce
@pytest.mark.parametrize('api', ['soap', 'bulk'])
def test_salesforce_origin_session_timeout(sdc_builder, sdc_executor, salesforce, api):
    """Test that Salesforce origin correctly handles a session timing out while the pipeline
    is running

    The pipeline looks like:
        salesforce_origin >> wiretap

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
             f"AND LastName = '{TEST_DATA['STR_15_RANDOM']}'"
             "ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     subscribe_for_notifications=False,
                                     use_bulk_api=(api == 'bulk'),
                                     repeat_query='FULL',
                                     query_interval=60)

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
        _verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

        logger.info('Revoking Salesforce session')
        r = requests.get(f'https://{client.sf_instance}/services/oauth2/revoke',
                         params={'token': client.session_id})
        assert r.status_code == 200

        # We killed the client's session, so we need to make a new one to be
        # able to delete the data in the finally block
        client = salesforce.client

        logger.info('Capturing another wiretap')
        wiretap.reset()
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
        _verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
def test_salesforce_origin_stop_resume(sdc_builder, sdc_executor, salesforce):
    """Create data using Salesforce client, stop the pipeline
    and then check if Salesforce origin receives them using wiretap.
    Insert more data and check again.

    The pipeline looks like:
        salesforce_origin >> wiretap
        salesforce_origin >= wiretap_events

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """

    aux_email = get_random_string(string.ascii_letters, 10).lower()

    data_to_insert = [{'FirstName': 'Test1', 'LastName': f"{TEST_DATA['STR_15_RANDOM']} 1",
                       'Email': f"{aux_email}1@example.com", 'LeadSource': 'Advertisement'},
                      {'FirstName': 'Test2', 'LastName': f"{TEST_DATA['STR_15_RANDOM']} 2",
                       'Email': f"{aux_email}2@example.com", 'LeadSource': 'Partner'},
                      {'FirstName': 'Test3', 'LastName': f"{TEST_DATA['STR_15_RANDOM']} 3",
                       'Email': f"{aux_email}3@example.com", 'LeadSource': 'Web'}]

    data_to_insert_2 = [{'FirstName': 'XTest1', 'LastName': f"{TEST_DATA['STR_15_RANDOM']} 4",
                         'Email': f"{aux_email}4@example.com", 'LeadSource': 'Advertisement'},
                        {'FirstName': 'XTest2', 'LastName': f"{TEST_DATA['STR_15_RANDOM']} 5",
                         'Email': f"{aux_email}5@example.com", 'LeadSource': 'Partner'},
                        {'FirstName': 'XTest3', 'LastName': f"{TEST_DATA['STR_15_RANDOM']} 6",
                         'Email': f"{aux_email}6@example.com", 'LeadSource': 'Web'}]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = (f"SELECT Id, FirstName, LastName, Email, LeadSource "
             "FROM Contact WHERE Id > '000000000000000' AND Email LIKE '" + aux_email + "%' and "
             f"LastName like \'{TEST_DATA['STR_15_RANDOM']} %\' ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     subscribe_for_notifications=False)

    wiretap = pipeline_builder.add_wiretap()
    wiretap_events = pipeline_builder.add_wiretap()

    salesforce_origin >> wiretap.destination
    salesforce_origin >= wiretap_events.destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids_1 = None
    inserted_ids_2 = None

    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids_1 = get_ids(client.bulk.Contact.insert(data_to_insert), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        _verify_wiretap_data(wiretap, data_to_insert)

        # Stage should produce events, and it does, since the fix for SDC-12418
        assert len(wiretap_events.output_records) == 1
        assert wiretap_events.output_records[0].header.values['sdc.event.type'] == 'no-more-data'
        assert wiretap_events.output_records[0].field['record-count'] == len(data_to_insert)

        # Pipeline stops, but if it changes in a future version
        sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED', timeout_sec=TIMEOUT)
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)

        # Using Salesforce client, create new rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids_2 = get_ids(client.bulk.Contact.insert(data_to_insert_2), 'id')

        logger.info('Starting pipeline')
        wiretap.reset()
        wiretap_events.reset()
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        _verify_wiretap_data(wiretap, data_to_insert + data_to_insert_2)

        # stage should produce events...
        assert len(wiretap_events.output_records) == 1
        assert wiretap_events.output_records[0].header.values['sdc.event.type'] == 'no-more-data'
        assert wiretap_events.output_records[0].field['record-count'] == len(data_to_insert_2) + len(data_to_insert)

    finally:
        inserted_ids = []
        if inserted_ids_1:
            inserted_ids += inserted_ids_1
        if inserted_ids_2:
            inserted_ids += inserted_ids_2
        if len(inserted_ids) == 0:
            inserted_ids = None
        clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
def test_salesforce_origin_document(sdc_builder, sdc_executor, salesforce):
    """Test that base64-encoded Document data can be correctly read (SDC-12041).

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    doc_name = TEST_DATA['STR_15_RANDOM']

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=f'SELECT Id, Body FROM Document WHERE '
                                                f'Name = \'{doc_name}\'',
                                     disable_query_validation=True,
                                     subscribe_for_notifications=False)

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_id = None
    try:
        # We need to put our document somewhere - create a folder if necessary
        # NOTE - it is not possible to delete a Folder when an associated Document is in the Recycle Bin, we don't have
        # the 'hard delete' permission by default and assigning that permission or emptying the recycle bin is not
        # possible via simple_salesforce. So - create a folder with a well-known name if necessary
        logger.info(f'Checking for Folder: {FOLDER_NAME}')
        result = client.query(f'SELECT Id FROM Folder WHERE Name = \'{FOLDER_NAME}\'')
        if len(result['records']) > 0:
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
        TEST_DATA['DATA_TO_INSERT'] = {'Name': doc_name,
                                       'FolderId': folder_id,
                                       'Body': base64.b64encode(body).decode("utf-8")}

        logger.info('Creating Document record using Salesforce client ...')
        ret = client.Document.create(TEST_DATA['DATA_TO_INSERT'])
        inserted_id = ret['id']

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # There should be a single row with Id and Body fields
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['Id'].value == inserted_id
        assert wiretap.output_records[0].field['Body'].value == body

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting records ...')
        if inserted_id:
            client.Document.delete(inserted_id)


@salesforce
@pytest.mark.parametrize('api', ['soap', 'bulk'])
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
        "Location": TEST_DATA['STR_15_RANDOM'],
        "Description": "This is a test event"
    }

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json.dumps(event_data),
                                       stop_after_first_batch=True)

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
                     {'sdcField': '/Location', 'salesforceField': 'Location'},
                     {'sdcField': '/Description', 'salesforceField': 'Description'}]

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
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        query_str = ('SELECT Id, ActivityDateTime, IsAllDayEvent, DurationInMinutes, Description, Location '
                     f"FROM Event WHERE Location = '{TEST_DATA['STR_15_RANDOM']}'")
        result = client.query(query_str)

        read_ids = get_ids(result['records'], 'Id')

        # Raw data source typically produces multiple records event if we only want one
        assert len(result['records']) == 1
        assert event_data['IsAllDayEvent'] == result['records'][0]['IsAllDayEvent']
        assert event_data['DurationInMinutes'] == result['records'][0]['DurationInMinutes']
        assert event_data['Location'] == result['records'][0]['Location']
        assert event_data['Description'] == result['records'][0]['Description']

    finally:
        logger.info('Deleting records ...')
        if read_ids:
            client.bulk.Event.delete(read_ids)


@salesforce
@pytest.mark.parametrize('api', ['soap', 'bulk'])
@pytest.mark.parametrize('separator', [COLON, PERIOD])
def test_salesforce_destination_relationship(sdc_builder, sdc_executor, salesforce, api, separator):
    """Test that we can write to related external ID fields (SDC-12636).

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    if api == 'soap' and separator == COLON:
        pytest.skip('Skipping... Colon separator is only allowed with Bulk API')

    client = salesforce.client
    # Using Salesforce client, create rows in Contact.
    logger.info('Creating rows using Salesforce client ...')

    # Email value should not exist in the database
    TEST_DATA['DATA_TO_INSERT'][1]["Email"] = f"{TEST_DATA['STR_15_RANDOM']}_1@example.com"
    TEST_DATA['DATA_TO_INSERT'][2]["Email"] = f"{TEST_DATA['STR_15_RANDOM']}_2@example.com"
    inserted_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

    # Relate the created contacts to each other
    # first contact reports to second contact; second contact reports to third
    TEST_DATA['CSV_DATA_TO_INSERT'] = ['Id,ReportsTo.Email']
    TEST_DATA['CSV_DATA_TO_INSERT'].append(f"{inserted_ids[0]['Id']},{TEST_DATA['DATA_TO_INSERT'][1]['Email']}")
    TEST_DATA['CSV_DATA_TO_INSERT'].append(f"{inserted_ids[1]['Id']},{TEST_DATA['DATA_TO_INSERT'][2]['Email']}")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['CSV_DATA_TO_INSERT'])

    salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
    field_mapping = [{'sdcField': '/Id', 'salesforceField': 'Id'},
                     {'sdcField': '/ReportsTo.Email', 'salesforceField': f'ReportsTo{separator}Email'}]
    salesforce_destination.set_attributes(default_operation='UPDATE',
                                          field_mapping=field_mapping,
                                          sobject_type=CONTACT,
                                          use_bulk_api=(api == 'bulk'))

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Now the pipeline will make the contacts report to each other
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Using Salesforce connection, read the contents in the Salesforce destination.
        query_str = (f'SELECT Id, Email, ReportsToId FROM Contact WHERE '
                     f"Lastname = \'{TEST_DATA['STR_15_RANDOM']}\' ORDER BY Id")
        result = client.query(query_str)

        # The magic of external id fields - we knitted the contacts together via their emails
        # Salesforce set the related record ids accordingly
        assert result['records'][0]['ReportsToId'] == result['records'][1]['Id']
        assert result['records'][1]['ReportsToId'] == result['records'][2]['Id']

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
@pytest.mark.parametrize('subscription_type', [PUSH_TOPIC, CDC])
def test_salesforce_subscription(sdc_builder, sdc_executor, salesforce, subscription_type):
    """Start pipeline, create data using Salesforce client
    and then check if Salesforce origin receives notifications using wiretap.

    The pipeline looks like:
        salesforce_origin >> wiretap

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
    push_topic_name = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        if subscription_type == PUSH_TOPIC:
            subscription_id, push_topic_name = create_push_topic(client)
        else:
            if Version(sdc_builder.version) < Version('3.7.0'):
                pytest.skip('CDC Feature requires minimum SDC version 3.7.0')
            subscription_id = enable_cdc(client)

        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(query_existing_data=False,
                                         subscribe_for_notifications=True,
                                         subscription_type=subscription_type)
        if subscription_type == PUSH_TOPIC:
            salesforce_origin.set_attributes(push_topic=push_topic_name)
        else:
            salesforce_origin.set_attributes(change_data_capture_object=CONTACT)

        wiretap = pipeline_builder.add_wiretap()
        salesforce_origin >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just create one contact
        logger.info('Creating record using Salesforce client...')
        contact = client.Contact.create(TEST_DATA['DATA_TO_INSERT'][0])

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        if subscription_type == PUSH_TOPIC:
            _verify_wiretap_data(wiretap, [TEST_DATA['DATA_TO_INSERT'][0]])
        else:
            verify_cdc_wiretap(wiretap, TEST_DATA['DATA_TO_INSERT'][0])

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


@salesforce
@sdc_min_version('3.7.0')
def test_salesforce_cdc_delete_field(sdc_builder, sdc_executor, salesforce):
    """Start pipeline, create data using Salesforce client
    and then check if Salesforce origin receives notifications using wiretap.

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
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

        wiretap = pipeline_builder.add_wiretap()
        salesforce_origin >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just create one contact
        logger.info('Creating first record using Salesforce client...')
        contact = client.Contact.create(TEST_DATA['DATA_TO_INSERT'][0])

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        # CDC returns more than just the record fields, so verify_snapshot isn't so useful
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values['salesforce.cdc.recordIds']
        assert wiretap.output_records[0].field['Email'] == TEST_DATA['DATA_TO_INSERT'][0]['Email']
        # CDC returns nested compound fields
        assert wiretap.output_records[0].field['Name']['FirstName'] == TEST_DATA['DATA_TO_INSERT'][0][
            'FirstName']
        assert wiretap.output_records[0].field['Name']['LastName'] == TEST_DATA['DATA_TO_INSERT'][0][
            'LastName']

        logger.info('Stopping pipeline')
        sdc_executor.stop_pipeline(pipeline)

        # Create another record, setting the custom field, then delete the
        # field, so that the current schema doesn't match the CDC notification
        logger.info('Creating second record using Salesforce client...')
        TEST_DATA['DATA_TO_INSERT'] = copy.deepcopy(TEST_DATA['DATA_TO_INSERT'][1])
        TEST_DATA['DATA_TO_INSERT']['BoolCustField__c'] = True
        contact2 = client.Contact.create(TEST_DATA['DATA_TO_INSERT'])

        logger.info('Deleting custom field from Contact object...')
        delete_custom_field_from_contact(metadata)

        logger.info('Restarting pipeline')
        wiretap.reset()
        sdc_executor.start_pipeline(pipeline)

        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values['salesforce.cdc.recordIds']
        assert wiretap.output_records[0].field['Email'] == TEST_DATA['DATA_TO_INSERT']['Email']
        assert wiretap.output_records[0].field['BoolCustField__c'] == TEST_DATA['DATA_TO_INSERT'][
            'BoolCustField__c']
        # CDC returns nested compound fields
        assert wiretap.output_records[0].field['Name']['FirstName'] == TEST_DATA['DATA_TO_INSERT'][
            'FirstName']
        assert wiretap.output_records[0].field['Name']['LastName'] == TEST_DATA['DATA_TO_INSERT'][
            'LastName']

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
@pytest.mark.parametrize('enough_buffer_size', [True, False])
def test_salesforce_streaming_api_buffer(sdc_builder, sdc_executor, salesforce, enough_buffer_size):
    """Testing that pipeline will fail if Streaming Buffer Size is too small,
    succeed if it is ample (SDC-12771).

    Start pipeline with given buffer size, create data using Salesforce client,
    check for error or correct wiretap as appropriate

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        enough_buffer_size (:obj:`str`): Whether to test the happy path, or a too-small buffer size: True or False
    """
    client = salesforce.client
    contact = None

    pipeline_builder = sdc_builder.get_pipeline_builder()
    push_topic, push_topic_name = create_push_topic(client)

    # 1048576 is default buffer size
    # 256 is big enough to connect, but not to receive data
    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(query_existing_data=False,
                                     subscribe_for_notifications=True,
                                     subscription_type=PUSH_TOPIC,
                                     push_topic=push_topic_name,
                                     streaming_buffer_size=(1048576 if enough_buffer_size else 256))

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just create one contact
        logger.info('Creating record using Salesforce client...')
        contact = client.Contact.create(TEST_DATA['DATA_TO_INSERT'][0])

        logger.info('Waiting for one record')

        if enough_buffer_size:
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
            _verify_wiretap_data(wiretap, [TEST_DATA['DATA_TO_INSERT'][0]])
        else:
            # Pipeline should stop with StageException
            assert len(wiretap.output_records) == 0
            assert len(wiretap.output_records) == 0
            sdc_executor.get_pipeline_status(pipeline).wait_for_status('RUN_ERROR')
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
@pytest.mark.parametrize('api', ['soap', 'bulk'])
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
    salesforce_origin.set_attributes(soql_query="SELECT Id, Name FROM Contact WHERE Id > '${OFFSET}' "
                                                f"AND LastName = \'{TEST_DATA['STR_15_RANDOM']}\' ORDER BY Id LIMIT 10",
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
        for aux in range(CONTACTS_FOR_NO_MORE_DATA):
            data_to_insert.append({'FirstName': get_random_string(string.ascii_letters, 10).lower(),
                                   'LastName': TEST_DATA['STR_15_RANDOM'],
                                   'Email': f"{get_random_string(string.ascii_letters, 10).lower()}@example.com"})

        logger.info('Creating Contact records using Salesforce client ...')
        contact_ids = get_ids(client.bulk.Contact.insert(data_to_insert), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)

        logger.info('Waiting for pipeline to finish')
        sdc_executor.get_pipeline_status(pipeline).wait_for_status(status='FINISHED', timeout_sec=TIMEOUT)

        logger.info('Getting pipeline history')
        history = sdc_executor.get_pipeline_history(pipeline)

        # We should see all the records we created as input, and all of them plus the event record as output
        input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        error_records = history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count
        assert input_records == CONTACTS_FOR_NO_MORE_DATA, \
            f'Observed {input_records} input records (expected {CONTACTS_FOR_NO_MORE_DATA})'
        assert output_records == CONTACTS_FOR_NO_MORE_DATA + 1, \
            f'Observed {output_records} output records (expected {CONTACTS_FOR_NO_MORE_DATA + 1})'
        assert error_records == 0, f'Observed {error_records} error records (expected 0)'

    finally:
        clean_up(sdc_executor, pipeline, client, contact_ids)


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

    # Using Salesforce client, create rows in Contact.
    logger.info('Creating rows using Salesforce client ...')
    TEST_DATA['DATA_TO_INSERT'][0]["Email"] = f"{TEST_DATA['STR_15_RANDOM']}_0@example.com"
    TEST_DATA['DATA_TO_INSERT'][1]["Email"] = f"{TEST_DATA['STR_15_RANDOM']}_1@example.com"
    TEST_DATA['DATA_TO_INSERT'][2]["Email"] = f"{TEST_DATA['STR_15_RANDOM']}_2@example.com"
    inserted_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

    # Link the records via ReportsToId
    logger.info('Updating rows using Salesforce client ...')
    data_for_update = [{'Id': inserted_ids[1]["Id"], 'ReportsToId': inserted_ids[0]["Id"]},
                       {'Id': inserted_ids[2]["Id"], 'ReportsToId': inserted_ids[1]["Id"]}]
    client.bulk.Contact.update(data_for_update)

    # Now disconnect the created contacts from each other
    TEST_DATA['CSV_DATA_TO_INSERT'] = ['Id,ReportsTo.Email']
    TEST_DATA['CSV_DATA_TO_INSERT'].append(f'{inserted_ids[1]["Id"]},')
    TEST_DATA['CSV_DATA_TO_INSERT'].append(f'{inserted_ids[2]["Id"]},')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['CSV_DATA_TO_INSERT'])

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

    try:
        # Now the pipeline will make the contacts report to each other
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Using Salesforce connection, read the contents in the Salesforce destination.
        query_str = ("SELECT Id, Email, ReportsToId FROM Contact "
                     f"WHERE Email LIKE '{TEST_DATA['STR_15_RANDOM']}%'")
        result = client.query(query_str)

        # Nobody should report to anybody any more
        assert result['records'][0]['ReportsToId'] is None
        assert result['records'][1]['ReportsToId'] is None
        assert result['records'][2]['ReportsToId'] is None

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
@pytest.mark.parametrize('api', ['soap', 'bulk'])
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

    # Using Salesforce client, create a Case
    logger.info('Creating rows using Salesforce client ...')
    result = client.Case.create({'Subject': CASE_SUBJECT})
    case_id = result['id']

    # Set the case owner. Even though we're not changing the owner, SDC-13117 would cause an error to
    # be thrown due to the bad syntax for the field name
    TEST_DATA['CSV_DATA_TO_INSERT'] = ['Id,Owner']
    TEST_DATA['CSV_DATA_TO_INSERT'].append(f'{case_id},{salesforce.username}')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['CSV_DATA_TO_INSERT'])

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

    try:
        # Now the pipeline will update the Case
        logger.info('Starting Salesforce destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Using Salesforce connection, read the Case, just to check
        query_str = f"SELECT Id, Subject, Owner.Username FROM Case WHERE Id = '{case_id}'"
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
@pytest.mark.parametrize('api', ['soap', 'bulk'])
def test_salesforce_datetime_in_history(sdc_builder, sdc_executor, salesforce, api):
    """Test SDC-12334 - field history data is untyped in the Salesforce schema, since OldValue and NewValue depend on
    the field that changed. For some datatypes, the XML holds type information in an xmltype attribute. We were using
    this to create the correct SDC field type, but not handling datetimes, throwing a FORCE_04 error.

    ActivatedDate on Contract is one of the few datetime fields that will show up in a standard object's field history.

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    client = salesforce.client

    acc, con = None, None

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
        wiretap = pipeline_builder.add_wiretap()
        salesforce_origin >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # There should be a single row with Id and NewValue fields. For SOAP API, NewValue should be a DATETIME, for
        # Bulk API it's a STRING
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['Id']
        if api == 'soap':
            assert wiretap.output_records[0].field['NewValue'].type == 'DATETIME'
        else:
            assert wiretap.output_records[0].field['NewValue'].type == 'STRING'

    finally:
        if con and con['id']:
            client.Contract.delete(con['id'])
        if acc and acc['id']:
            client.Account.delete(acc['id'])


@salesforce
@sdc_min_version('3.7.0')
def test_salesforce_origin_query_cdc_no_object(sdc_builder, sdc_executor, salesforce):
    """Test SDC-12378 - enabling CDC with blank object name ('get notifications for all objects') was causing
    initial query to fail.

    Create data using Salesforce client and then check if Salesforce origin receives them using snapshot.

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = ("SELECT Id, FirstName, LastName, Email, LeadSource FROM Contact "
             "WHERE Id > '000000000000000' AND "
             f"Lastname = \'{TEST_DATA['STR_15_RANDOM']}\' "
             "ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     subscribe_for_notifications=True,
                                     subscription_type=CDC,
                                     change_data_capture_object='')

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)

        _verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
@pytest.mark.parametrize('multiple_data', [False, True])
def test_einstein_analytics_destination(sdc_builder, sdc_executor, salesforce, multiple_data):
    """Basic test for Einstein Analytics destination. Write some data and check that it's there

    The pipeline looks like:
        dev_raw_data_source >> delay >> einstein_analytics_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    client = salesforce.client

    identifier = None
    current_version_id = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['CSV_DATA_TO_INSERT'])
        if multiple_data:
            dev_raw_data_source.set_attributes(stop_after_first_batch=False)

        # Delay so that we can stop the pipeline after a single batch is processed
        delay = pipeline_builder.add_stage('Delay')
        delay.delay_between_batches = 5 * 1000

        analytics_destination = pipeline_builder.add_stage('Einstein Analytics', type='destination')
        edgemart_alias = get_random_string(string.ascii_letters, 10).lower()
        # Explicitly set auth credentials since Salesforce environment doesn't know about Einstein Analytics destination
        analytics_destination.set_attributes(edgemart_alias=edgemart_alias,
                                             username=salesforce.username,
                                             password=salesforce.password,
                                             auth_endpoint='test.salesforce.com')
        if multiple_data:
            analytics_destination.set_attributes(append_timestamp_to_alias=True)

        dev_raw_data_source >> delay >> analytics_destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        # Now the pipeline will write data to Einstein Analytics
        logger.info('Starting Einstein Analytics destination pipeline and waiting for it to produce records ...')
        if multiple_data:
            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(6)
            sdc_executor.stop_pipeline(pipeline)
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Einstein Analytics data load is asynchronous, so poll until it's done
        logger.info('Looking for dataset in Einstein Analytics')
        end_time = datetime.now() + timedelta(seconds=120)
        while identifier is None and datetime.now() < end_time:
            sleep(5)
            if multiple_data:
                identifier, current_version_id = find_dataset_include_timestamp(client, edgemart_alias)
            else:
                identifier, current_version_id = find_dataset(client, edgemart_alias)

        # Make sure we found a dataset and didn't time out!
        assert identifier is not None

        # Now query the data from Einstein Analytics using SAQL

        # Build the load statement
        load = f'q = load \"{identifier}/{current_version_id}\";'

        # Build the identity projection - e.g.
        # q = foreach q generate Email as Email, FirstName as FirstName, LastName as LastName, LeadSource as LeadSource;
        field_list = []
        for key in TEST_DATA['DATA_TO_INSERT'][0]:
            field_list.append(f'{key} as {key}')
        projection = 'q = foreach q generate ' + ', '.join(field_list) + ';'

        # Ensure consistent ordering
        order_key = 'Email'
        ordering = f'q = order q by {order_key};'

        logger.info('Querying Einstein Analytics')
        response = client.restful('wave/query', method='POST', json={'query': load + projection + ordering})

        assert sorted(TEST_DATA['DATA_TO_INSERT'], key=itemgetter(order_key)) == response['results']['records']

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


@salesforce
@pytest.mark.parametrize('subscription_type', [PUSH_TOPIC, CDC])
@pytest.mark.parametrize('api', ['soap', 'bulk'])
def test_salesforce_switch_from_query_to_subscription(sdc_builder, sdc_executor, salesforce, subscription_type, api):
    """Start pipeline, write data using Salesforce client, read existing data via query,
    check if Salesforce origin reads data via wiretap, write more data, check that Salesforce
    origin reads it via Push Topic/CDC.

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        subscription_type (:obj:`str`): Type of subscription: 'PUSH_TOPIC' or 'CDC'
        api (:obj:`str`): API to test: 'soap' or 'bulk'
    """
    client = salesforce.client

    pipeline = None
    subscription_id = None
    contact = None
    inserted_ids = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        if subscription_type == PUSH_TOPIC:
            subscription_id, push_topic_name = create_push_topic(client)
        else:
            if Version(sdc_builder.version) < Version('3.7.0'):
                pytest.skip('CDC Feature requires minimum SDC version 3.7.0')
            subscription_id = enable_cdc(client)

        query = ("SELECT Id, FirstName, LastName, Email, LeadSource FROM Contact "
                 "WHERE Id > '000000000000000' AND "
                 f"Email LIKE \'xtest%\' and LastName = '{TEST_DATA['STR_15_RANDOM']}'"
                 " ORDER BY Id")

        first_data_to_insert = TEST_DATA['DATA_TO_INSERT'][:-1]
        second_data_to_insert = TEST_DATA['DATA_TO_INSERT'][-1]

        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(query_existing_data=True,
                                         subscribe_for_notifications=True,
                                         use_bulk_api=(api == 'bulk'),
                                         soql_query=query,
                                         subscription_type=subscription_type)
        if subscription_type == PUSH_TOPIC:
            salesforce_origin.set_attributes(push_topic=push_topic_name)
        else:
            salesforce_origin.set_attributes(change_data_capture_object=CONTACT)

        wiretap = pipeline_builder.add_wiretap()
        salesforce_origin >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        client = salesforce.client
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = get_ids(client.bulk.Contact.insert(first_data_to_insert), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(first_data_to_insert))

        _verify_wiretap_data(wiretap, first_data_to_insert)

        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just create one contact
        logger.info('Creating record using Salesforce client...')
        wiretap.reset()
        contact = client.Contact.create(second_data_to_insert)

        logger.info('Capturing second batch data')
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(first_data_to_insert) + 1)

        if subscription_type == PUSH_TOPIC:
            _verify_wiretap_data(wiretap, [second_data_to_insert])
        else:
            verify_cdc_wiretap(wiretap, second_data_to_insert)

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids)
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


@salesforce
@sdc_min_version('3.7.0')
def test_salesforce_cdc_replay_all(sdc_builder, sdc_executor, salesforce):
    """Start pipeline with replay option "All events", read all Salesforce data (if any exists) and stop the pipeline.
    Then, create a new record in Salesforce, start pipeline again
    and check if Salesforce origin receives the new changes done while it was offline as well as the previous data.

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    client = salesforce.client

    pipeline = None
    subscription_id = None
    contact = None
    try:
        session = SfdcSession(username=escape(salesforce.username),
                              password=escape(salesforce.password),
                              is_sandbox=True,
                              api_version=API_VERSION)
        session.login()

        pipeline_builder = sdc_builder.get_pipeline_builder()

        subscription_id = enable_cdc(client)

        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(query_existing_data=False,
                                         subscribe_for_notifications=True,
                                         subscription_type=CDC,
                                         change_data_capture_object=CONTACT,
                                         replay_option=ALL_EVENTS)

        wiretap = pipeline_builder.add_wiretap()
        salesforce_origin >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        original_length = len(wiretap.output_records)
        sdc_executor.stop_pipeline(pipeline)

        logger.info('Creating a new record using Salesforce client...')
        contact = client.Contact.create(TEST_DATA['DATA_TO_INSERT'][0])

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        # Verify that we read one more entry than the first time, and check its values are correct
        assert len(wiretap.output_records) == (original_length + 1)
        assert wiretap.output_records[original_length].header.values['salesforce.cdc.recordIds']
        assert wiretap.output_records[original_length].field['Email'] == TEST_DATA['DATA_TO_INSERT'][0]['Email']
        # CDC returns nested compound fields
        assert wiretap.output_records[original_length].field['Name']['FirstName'] == TEST_DATA['DATA_TO_INSERT'][0][
            'FirstName']
        assert wiretap.output_records[original_length].field['Name']['LastName'] == TEST_DATA['DATA_TO_INSERT'][0][
            'LastName']

        logger.info('Stopping pipeline')
        sdc_executor.stop_pipeline(pipeline)

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
