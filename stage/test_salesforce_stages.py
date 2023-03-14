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

import base64
import copy
import json
import logging
import math
import string
import time
import json
from urllib.parse import urljoin
from collections import OrderedDict
from contextlib import ExitStack
from time import sleep
from uuid import uuid4

import pytest
import requests
from streamsets.sdk.sdc_api import StatusError
from streamsets.sdk.utils import wait_for_condition
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

from .utils.utils_salesforce import (insert_data_and_verify_using_wiretap, verify_wiretap_data, ACCOUNTS_FOR_SUBQUERY,
                                     add_custom_field_to_contact, CASE_SUBJECT, clean_up, CONTACTS_FOR_NO_MORE_DATA,
                                     CONTACTS_FOR_SUBQUERY, create_push_topic, delete_custom_field_from_contact,
                                     get_cdc_wiretap_records, get_dev_raw_data_source, get_ids, set_up_random,
                                     FOLDER_NAME, TEST_DATA, TIMEOUT, verify_analytics_data,
                                     MULTIPLE_UPLOADS_PER_BATCH_SCRIPT, MULTIPLE_UPLOADS_BATCH_SIZE,
                                     assign_hard_delete, revoke_hard_delete, verify_result_ids, FORCE_60,
                                     BULK_PIPELINE_TIMEOUT_SECONDS, SOAP_PIPELINE_TIMEOUT_SECONDS)

CONTACT = 'Contact'
ACCOUNT = 'Account'
CDC = 'CDC'
ALL_EVENTS = 'ALL_EVENTS'
PUSH_TOPIC = 'PUSH_TOPIC'
COLON = ':'
PERIOD = '.'

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def _set_up_random(salesforce):
    set_up_random(salesforce)


@pytest.fixture(scope="module", autouse=True)
def _set_up_hard_delete_permission(salesforce):
    # Having each test create and delete their own permissions file has various concurrency problems which make several
    # tests fail each execution. Additionally, if the cleanup is not properly done the account fills up with leftovers
    # and tests also start failing. Creating a single permissions file for the whole test file should alleviate both
    # problems.
    client = salesforce.client
    permission_set_id = assign_hard_delete(client, 'test_salesforce_stages')

    yield

    # Delete the hard delete permission file to keep the test account clean
    revoke_hard_delete(client, permission_set_id)

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
        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True)


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
        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True)


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
        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True)


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
    salesforce_origin.set_attributes(soql_query=query,
                                     use_bulk_api=(api == 'bulk'),
                                     subscribe_for_notifications=False)

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, TEST_DATA['DATA_TO_INSERT'], salesforce,
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

    insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, expected_data, salesforce, expected_data)


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
        timeout_sec = BULK_PIPELINE_TIMEOUT_SECONDS if api == 'bulk' else SOAP_PIPELINE_TIMEOUT_SECONDS
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              3,
                                              timeout_sec=timeout_sec)
        sdc_executor.stop_pipeline(pipeline)

        verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

        wiretap.reset()
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              0,
                                              timeout_sec=timeout_sec)
        sdc_executor.stop_pipeline(pipeline)

        assert len([record.value['value'] for record in wiretap.output_records]) == 0
    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)


@salesforce
def test_salesforce_origin_platform_events(sdc_builder, sdc_executor, salesforce):
    pipeline = None
    # We define a platform event in the same way we define a custom object
    # https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_define.htm
    salesforce_platform_event_name = "Test_event__e"
    salesforce_push_event_url = urljoin(salesforce.client.base_url, f'sobjects/{salesforce_platform_event_name}')
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(api_version="43.0",
                                         query_existing_data=False,
                                         max_batch_size_in_records=1,
                                         subscribe_for_notifications=True,
                                         subscription_type="PLATFORM_EVENT",
                                         replay_option="NEW_EVENTS",
                                         platform_event_api_name=salesforce_platform_event_name)

        wiretap = pipeline_builder.add_wiretap()

        salesforce_origin >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)
        pipeline_cmd = sdc_executor.start_pipeline(pipeline)
        pipeline_cmd.wait_for_status('RUNNING')
        sleep(10)

        # When there are simultaneous executions of this test, the records from all the executions are mixed together,
        # and they may be recovered by the salesforce origin stage because they all use the same platform event.
        # To make sure we have read the records from this execution, we give unique values to the test_text field and
        # check we have received as many records with these values as expected.

        # Publish platform event
        # https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_publish_api.htm
        number_of_events = 4
        field_name = f"test_text__c"
        field_content_base = get_random_string(string.ascii_letters, 10)
        for i in range(number_of_events):
            salesforce.client._call_salesforce(
                'POST',
                salesforce_push_event_url,
                data=json.dumps({field_name: f"{field_content_base} {i}"})
            )

        sleep(10)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', number_of_events, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        output_records = wiretap.output_records
        assert len(output_records) >= number_of_events, \
            f"At least {number_of_events} records should have been found, but only {len(output_records)} were found"

        records_found = 0
        for output_record in output_records:
            if field_content_base in output_record.field[field_name].value:
                assert output_record.field[field_name] == f"{field_content_base} {records_found}"
                records_found += 1

        assert records_found == number_of_events, \
            f"Only {records_found} of the expected {number_of_events} records generated by this instance of the test " \
            f"have been recovered"
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


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
    insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, LOOKUP_EXPECTED_DATA,
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

        verify_wiretap_data(wiretap, lookup_expected_data)

    finally:
        clean_up(sdc_executor, pipeline, client, contact_ids, hard_delete=True)


@salesforce
def test_salesforce_lookup_processor_single_quote_escaping(sdc_builder, sdc_executor, salesforce):
    """Simple Salesforce Lookup processor test - retrieve by record id rather than query
    Pipeline will enrich records with the 'FirstName' of contacts by adding a field as 'surName'.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    lookup_data = ['surName'] + [row['LastName'] for row in TEST_DATA['QUOTED_DATA_TO_INSERT']]
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

    salesforce_lookup = pipeline_builder.add_stage('Salesforce Lookup')
    query_str = \
        """SELECT FirstName, Email FROM Contact WHERE LastName = 
        '${str:replaceAll(record:value('/surName'),"'","\\\\\\\\'")}'"""

    salesforce_lookup.set_attributes(soql_query=query_str)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> salesforce_lookup >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = get_ids(client.bulk.Contact.insert(TEST_DATA['QUOTED_DATA_TO_INSERT']), 'id')

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We need to look for surName field instead of LastName
        lookup_expected_data = copy.deepcopy(TEST_DATA['QUOTED_DATA_TO_INSERT'])
        for record in lookup_expected_data:
            record['surName'] = record.pop('LastName')
        verify_wiretap_data(wiretap, lookup_expected_data)
    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)


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
        clean_up(sdc_executor, pipeline, client, contact_ids, hard_delete=True)


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
        clean_up(sdc_executor, pipeline, client, contact_ids, hard_delete=True)


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
            try:
                client.bulk.Account.delete(account_ids)
            except Exception as e:
                logger.warning(f'Unable to delete records: {e}')


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
        clean_up(sdc_executor, pipeline, client, contact_ids, hard_delete=True)


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
        clean_up(sdc_executor, pipeline, client, contact_ids, hard_delete=True)


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
        timeout_sec = BULK_PIPELINE_TIMEOUT_SECONDS if api == 'bulk' else SOAP_PIPELINE_TIMEOUT_SECONDS
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              3,
                                              timeout_sec=timeout_sec)
        verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

        logger.info('Revoking Salesforce session')
        r = requests.get(f'https://{client.sf_instance}/services/oauth2/revoke',
                         params={'token': client.session_id})
        assert r.status_code == 200

        # We killed the client's session, so we need to make a new one to be
        # able to delete the data in the finally block
        client = salesforce.client

        logger.info('Capturing another wiretap')
        wiretap.reset()
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              3,
                                              timeout_sec=timeout_sec)
        verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)


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

        verify_wiretap_data(wiretap, data_to_insert)

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

        verify_wiretap_data(wiretap, data_to_insert + data_to_insert_2)

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
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)


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
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)


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
    push_topic_name = None
    test_data = TEST_DATA['DATA_TO_INSERT'][0]

    contact = client.Contact.create(test_data)
    contact_id = contact['id']
    logger.info('Created a Contact using Salesforce client with id as %s', contact_id)
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        if subscription_type == PUSH_TOPIC:
            # note test_data['LastName'] is random unique data
            subscription_id, push_topic_name = create_push_topic(client, test_data['LastName'])
        else:
            if Version(sdc_builder.version) < Version('3.7.0'):
                pytest.skip('CDC Feature requires minimum SDC version 3.7.0')

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

        logger.info('Starting pipeline ...')
        pipeline_cmd = sdc_executor.start_pipeline(pipeline)
        pipeline_cmd.wait_for_status('RUNNING')
        # Give the pipeline time to connect to the Streaming API
        time.sleep(10)

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just create one contact
        test_data['FirstName'] = 'Updated FirstName' # a good case for CDC to detect this change
        client.Contact.update(contact_id, test_data)
        logger.info('Updated a Contact using Salesforce client with id as %s', contact_id)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=300)
        if subscription_type == PUSH_TOPIC:
            change_records = [record for record in wiretap.output_records if record.field['Id'] == contact_id]
            assert change_records
            change_record = change_records[0]
            assert change_record.field['Email'] == test_data['Email']
            assert change_record.field['FirstName'] == test_data['FirstName']
            assert change_record.field['LastName'] == test_data['LastName']
        else:
            change_records = get_cdc_wiretap_records(wiretap, [contact_id])
            assert change_records
            change_record = change_records[0]
            assert change_record.header.values['salesforce.cdc.changeType'] == 'UPDATE'
            assert change_record.field['Name']['FirstName'] == test_data['FirstName']

        logger.info('Stopping pipeline after success ...')
        sdc_executor.stop_pipeline(pipeline)
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline after possible failure ...')
            sdc_executor.stop_pipeline(pipeline)
        if subscription_id and subscription_type == PUSH_TOPIC:
            logger.info('Deleting PushTopic with id %s ...', subscription_id)
            client.PushTopic.delete(subscription_id)
        if contact_id:
            logger.info('Deleting Contact with id %s ...', contact_id)
            client.contact.delete(contact_id)


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
    test_data = TEST_DATA['DATA_TO_INSERT'][0]
    custom_field_name = '{}__c'.format(get_random_string(string.ascii_letters, 10))
    custom_field_label = 'BoolCustField'
    custom_field_type = 'Checkbox'
    parameters = '<defaultValue>false</defaultValue>'

    logger.info('Adding custom field %s to Contact object ...', custom_field_name)
    # If the Salesforce org has a namespace, it is prepended to the custom field name
    custom_field_name = add_custom_field_to_contact(salesforce, custom_field_name, custom_field_label, custom_field_type,
                                                    parameters)
    custom_field_deleted = False

    pipeline = None
    contact_id = None
    contact_id_2 = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        salesforce_origin.set_attributes(query_existing_data=False,
                                         subscribe_for_notifications=True,
                                         subscription_type=CDC,
                                         change_data_capture_object=CONTACT)

        wiretap = pipeline_builder.add_wiretap()
        salesforce_origin >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline ...')
        sdc_executor.start_pipeline(pipeline)

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just create one contact
        # create change data
        contact = client.Contact.create(test_data)
        contact_id = contact['id']
        logger.info('Created first record of Contact using Salesforce client with id as %s', contact_id)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=300)
        change_records = get_cdc_wiretap_records(wiretap, [contact_id])
        assert change_records
        change_record = change_records[0]
        assert change_record.field['Email'] == test_data['Email']
        assert change_record.field['Name']['FirstName'] == test_data['FirstName']
        assert change_record.field['Name']['LastName'] == test_data['LastName']

        logger.info('Stopping pipeline after success ...')
        sdc_executor.stop_pipeline(pipeline)

        # Create another record, setting the custom field, then delete the
        # field, so that the current schema doesn't match the CDC notification
        test_data_2 = copy.deepcopy(TEST_DATA['DATA_TO_INSERT'][1])
        test_data_2[custom_field_name] = True
        contact_2 = client.Contact.create(test_data_2)
        contact_id_2 = contact_2['id']
        logger.info('Created second record of Contact using Salesforce client with id as %s', contact_id_2)

        logger.info('Deleting custom field %s from Contact object ...', custom_field_name)
        delete_custom_field_from_contact(client, custom_field_name)
        custom_field_deleted = True

        logger.info('Restarting pipeline ...')
        wiretap.reset()
        sdc_executor.start_pipeline(pipeline)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=300)
        change_records_2 = get_cdc_wiretap_records(wiretap, [contact_id_2])
        assert change_records_2
        change_record_2 = change_records_2[0]
        assert change_record_2.field['Email'] == test_data_2['Email']
        assert change_record_2.field[custom_field_name] == test_data_2[custom_field_name]
        assert change_record_2.field['Name']['FirstName'] == test_data_2['FirstName']
        assert change_record_2.field['Name']['LastName'] == test_data_2['LastName']

        logger.info('Stopping pipeline after success ...')
        sdc_executor.stop_pipeline(pipeline)
    finally:
        if contact_id:
            logger.info('Deleting Contact with id %s ...', contact_id)
            client.contact.delete(contact_id)
        if contact_id_2:
            logger.info('Deleting Contact with id %s ...', contact_id_2)
            client.contact.delete(contact_id_2)
        if not custom_field_deleted:
            logger.info('Deleting custom field %s from Contact object ...', custom_field_name)
            delete_custom_field_from_contact(client, custom_field_name)
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline after possible failure ...')
            sdc_executor.stop_pipeline(pipeline)


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

    pipeline = None
    push_topic = None
    test_data = TEST_DATA['DATA_TO_INSERT'][0]

    contact = client.Contact.create(test_data)
    contact_id = contact['id']
    logger.info('Created a Contact using Salesforce client with id as %s', contact_id)
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        # note test_data['LastName'] is random unique data
        push_topic, push_topic_name = create_push_topic(client, test_data['LastName'])

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

        pipeline_cmd = sdc_executor.start_pipeline(pipeline).wait_for_status('RUNNING')
        time.sleep(10) # Give the pipeline time to connect to the Streaming API

        # We update the Contact (with same data) to trigger a push event
        client.Contact.update(contact_id, test_data)

        if enough_buffer_size:
            sdc_executor.wait_for_pipeline_metric(pipeline,
                                                  'input_record_count',
                                                  1,
                                                  timeout_sec=SOAP_PIPELINE_TIMEOUT_SECONDS)
            verify_wiretap_data(wiretap, [test_data])
            logger.info('Stopping pipeline after success ...')
            sdc_executor.stop_pipeline(pipeline)
        else:
            # Pipeline should stop with StageException
            assert len(wiretap.output_records) == 0
            sdc_executor.get_pipeline_status(pipeline).wait_for_status('RUN_ERROR')
            status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
            assert 'RUN_ERROR' == status
    finally:
        if contact_id:
            logger.info('Deleting Contact with id %s ...', contact_id)
            client.contact.delete(contact_id)
        if push_topic:
            logger.info('Deleting PushTopic with id %s ...', push_topic)
            client.PushTopic.delete(push_topic)
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline after possible failure ...')
            sdc_executor.stop_pipeline(pipeline)


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
        clean_up(sdc_executor, pipeline, client, contact_ids, hard_delete=True)


@salesforce
@sdc_min_version("5.3.0")
@pytest.mark.parametrize("api", ["bulk", "soap"])
@pytest.mark.parametrize(
    "input_value, converter_type, salesforce_type, field_name, expected_value",
    [
        ("2020-01-01 10:00:00Z", "DATETIME", "DateTime", "DateTime_field", "2020-01-01T10:00:00.000+0000"),
        ("", "DATETIME", "DateTime", "DateTime_field", None),
        ("2020-01-01Z", "DATE", "Date", "Date_field", "2020-01-01"),
        ("", "DATE", "Date", "Date_field", None),
        ("10:00:00Z", "TIME", "Time", "Time_field", "10:00:00.000Z"),
        ("", "TIME", "Time", "Time_field", None),
    ],
)
def test_salesforce_destination_null_datetime(
    sdc_builder, sdc_executor, salesforce, api, input_value, converter_type, salesforce_type, field_name, expected_value
):

    client = salesforce.client
    contact_name = get_random_string(string.ascii_letters, 10)

    # Define a custom field of the type determined by @salesforce_type
    custom_field_name = "{}__c".format(get_random_string(string.ascii_letters, 10))
    custom_field_label = field_name
    custom_field_type = salesforce_type
    parameters = "<defaultValue>NULL</defaultValue>"

    # Define the field mapping from SDC to Salesforce
    field_mapping = [
        {"sdcField": "/FirstName", "salesforceField": "FirstName"},
        {"sdcField": "/LastName", "salesforceField": "LastName"},
        {"sdcField": "/Email", "salesforceField": f"Email"},
        {"sdcField": f"/{custom_field_name}", "salesforceField": custom_field_name},
    ]

    with ExitStack() as on_exit:
        # Add the custom field to "Contact" and ensure it will be deleted when the test ends
        custom_field_name = add_custom_field_to_contact(
            salesforce, custom_field_name, custom_field_label, custom_field_type, parameters
        )
        on_exit.callback(delete_custom_field_from_contact, client, custom_field_name)

        # Add the stages
        pipeline_builder = sdc_builder.get_pipeline_builder()

        # Dev Raw Data Source
        dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
        dev_raw_data_source.data_format = "JSON"
        dev_raw_data_source.stop_after_first_batch = True
        dev_raw_data_source.raw_data = json.dumps(
            {
                "FirstName": contact_name,
                "LastName": "O' Smith",
                "Email": "matthewsmith@example.com",
                custom_field_name: input_value,
            }
        )

        # Field type converter
        other_date_format = None
        if converter_type == "DATE":
            other_date_format = "yyyy-MM-ddX"
        elif converter_type == "DATETIME":
            other_date_format = "yyyy-MM-dd HH:mm:ssX"
        elif converter_type == "TIME":
            other_date_format = "HH:mm:ssX"

        converter = pipeline_builder.add_stage("Field Type Converter")
        converter.conversion_method = "BY_FIELD"
        converter.field_type_converter_configs = [
            {
                "fields": [f"/{custom_field_name}"],
                "targetType": converter_type,
                "dataLocale": "en.US",
                "dateFormat": "OTHER",
                "otherDateFormat": other_date_format,
                "scale": 2,
                "inputFieldEmpty": "NULL",  # Convert empty strings to nulls to produce null datetime fields
            }
        ]

        # Salesforce Destination
        salesforce_destination = pipeline_builder.add_stage("Salesforce", type="destination")
        salesforce_destination.set_attributes(
            default_operation="INSERT",
            field_mapping=field_mapping,
            sobject_type="Contact",
            use_bulk_api=(api == "bulk"),
        )

        # Build and add the pipeline
        dev_raw_data_source >> converter >> salesforce_destination
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        # Run the pipeline
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Retrieve the results
        query_str = (
            f"SELECT Id, FirstName, {custom_field_name} FROM Contact WHERE FirstName = '{contact_name}' ORDER BY Id"
        )
        result = client.query(query_str)

        read_ids = get_ids(result["records"], "Id")
        if read_ids:
            on_exit.callback(client.bulk.Event.delete, read_ids)

        # Raw data source may sometimes produce multiple records even if we only want one
        assert len(result["records"]) == 1

        record = result["records"][0]
        assert (
            record[custom_field_name] == expected_value
        ), f"Expected record with {converter_type} value {expected_value}, got {record[custom_field_name]}"
        assert (
            record["FirstName"] == contact_name
        ), f"Expected record with FirstName {contact_name}, got {record['FirstName']}"

        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True)


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
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)


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
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              3,
                                              timeout_sec=SOAP_PIPELINE_TIMEOUT_SECONDS)

        verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)


@salesforce
@pytest.mark.parametrize('multiple_data', [False, True])
def test_einstein_analytics_destination(sdc_builder, sdc_executor, salesforce, multiple_data):
    """Basic test for Tableau CRM destination. Write some data and check that it's there

    The pipeline looks like:
        dev_raw_data_source >> delay >> einstein_analytics_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    client = salesforce.client

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['CSV_DATA_TO_INSERT'])
    records_per_batch = len(TEST_DATA['DATA_TO_INSERT'])
    minimum_total_records = 2 * records_per_batch
    if multiple_data:
        dev_raw_data_source.set_attributes(stop_after_first_batch=False)

    # Delay so that we can stop the pipeline after a single batch is processed
    delay = pipeline_builder.add_stage('Delay')
    delay.delay_between_batches = 5 * 1000

    # Name change for COLLECTOR-225
    try:
        analytics_destination = pipeline_builder.add_stage('Tableau CRM', type='destination')
    except:
        analytics_destination = pipeline_builder.add_stage('Einstein Analytics', type='destination')

    edgemart_alias = get_random_string(string.ascii_letters, 10).lower()
    analytics_destination.set_attributes(edgemart_alias=edgemart_alias,
                                         dataset_wait_time_in_secs=60)

    dev_raw_data_source >> delay >> analytics_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    # Now the pipeline will write data to Tableau CRM
    logger.info('Starting Tableau CRM destination pipeline and waiting for it to produce records ...')
    if multiple_data:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(minimum_total_records)
        sdc_executor.stop_pipeline(pipeline)
    else:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

    verify_analytics_data(client, edgemart_alias, TEST_DATA['DATA_TO_INSERT'], 'Email', multiple_data)


@salesforce
@sdc_min_version('4.2.0')
def test_recover_analytics_partial_upload(sdc_builder, sdc_executor, salesforce):
    """COLLECTOR-225. Create InsightsExternalData and an InsightsExternalDataPart to
    simulate a partial upload, then have SDC recover the data

    The pipeline looks like:
        dev_raw_data_source >> einstein_analytics_destination

    Don't actually send any data through the pipeline - we're only interested in
    the recovery
    """
    client = salesforce.client

    edgemart_alias = get_random_string(string.ascii_letters, 10).lower()

    insights_external_data = {
        'Format': 'Csv',
        'EdgemartAlias': edgemart_alias,
        'Action': 'None'
    }
    insights_external_data = client.InsightsExternalData.create(insights_external_data)
    assert insights_external_data['success']

    logger.info("Created InsightsExternalData for %s with id %s", edgemart_alias, insights_external_data['id'])

    csv_data = '\n'.join(TEST_DATA['CSV_DATA_TO_INSERT'])
    logger.info("Test data:\n%s", csv_data)

    insights_external_data_part = {
        'DataFile': base64.b64encode(csv_data.encode("utf-8")).decode("ascii"),
        'InsightsExternalDataId': insights_external_data['id'],
        'PartNumber': 1
    }
    insights_external_data_part = client.InsightsExternalDataPart.create(insights_external_data_part)

    assert insights_external_data_part['success']
    logger.info("Uploaded test data in InsightsExternalDataPart %s", insights_external_data_part['id'])

    # Now create the pipeline, start it, let it run for a little while, and stop it
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Just use CSV headers - we don't want to feed any actual data through
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, [TEST_DATA['CSV_DATA_TO_INSERT'][0]])
    dev_raw_data_source.set_attributes(stop_after_first_batch=True)

    analytics_destination = pipeline_builder.add_stage('Tableau CRM', type='destination')
    analytics_destination.set_attributes(edgemart_alias=edgemart_alias,
                                         dataset_wait_time_in_secs=60)

    dev_raw_data_source >> analytics_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    # Now the pipeline will recover the partial upload
    logger.info('Starting Tableau CRM destination pipeline and waiting for it to produce records ...')
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    verify_analytics_data(client, edgemart_alias, TEST_DATA['DATA_TO_INSERT'], 'Email')


@salesforce
@sdc_min_version('4.2.0')
def test_multiple_uploads_per_batch(sdc_builder, sdc_executor, salesforce):
    """
    COLLECTOR-225. Create a batch with more than 10MB of data to make the destination perform
    multiple uploads.

    The pipeline looks like:
        scripting_origin >> einstein_analytics_destination
    """

    client = salesforce.client

    pipeline_builder = sdc_builder.get_pipeline_builder()
    scripting_origin = pipeline_builder.add_stage('Jython Scripting', type='origin')
    scripting_origin.set_attributes(user_script=MULTIPLE_UPLOADS_PER_BATCH_SCRIPT,
                                    batch_size=MULTIPLE_UPLOADS_BATCH_SIZE)

    analytics_destination = pipeline_builder.add_stage('Tableau CRM', type='destination')
    edgemart_alias = get_random_string(string.ascii_letters, 10).lower()
    analytics_destination.set_attributes(edgemart_alias=edgemart_alias,
                                         dataset_wait_time_in_secs=0)

    scripting_origin >> analytics_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    # Now the pipeline will write data to Tableau CRM
    logger.info('Starting Tableau CRM destination pipeline and waiting for it to produce records ...')
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Math copied from the script!
    # With the default batch size of 1000 records, record_size is 26,214.4 bytes
    record_size = 25.0 * 1024.0 * 1024.0 / float(MULTIPLE_UPLOADS_BATCH_SIZE)
    # Default maximum length of a text value. Need to create metadata for more (up to 32,000 characters)
    field_size  = 255
    # With the default batch size, field_count is 103, giving an actual batch size of 103 * 255 * 1000 ~= 25MB
    field_count = int(math.ceil(record_size / field_size))

    expected_data = []
    for offset in range (MULTIPLE_UPLOADS_BATCH_SIZE):
        record = OrderedDict()
        for f in range(field_count):
            # Need to subtract width of numeric prefix (3) from field_size
            record['field' + str(f).zfill(3)] = str(offset).zfill(3) + ("x" * (field_size - 3))
        expected_data.append(record)

    verify_analytics_data(client, edgemart_alias, expected_data, list(expected_data[0].keys())[0])

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
    test_data = TEST_DATA['DATA_TO_INSERT']

    inserted_ids = get_ids(client.bulk.Contact.insert(test_data), 'id')
    logger.info('Created Contacts using Salesforce client with id(s) as %s', inserted_ids)
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        if subscription_type == PUSH_TOPIC:
            # note test_data 'LastName' is random unique data same across all records
            subscription_id, push_topic_name = create_push_topic(client, test_data[0]['LastName'])
        else:
            if Version(sdc_builder.version) < Version('3.7.0'):
                pytest.skip('CDC Feature requires minimum SDC version 3.7.0')

        query = ("SELECT Id, FirstName, LastName, Email, LeadSource FROM Contact "
                 "WHERE Id > '000000000000000' AND "
                 f"Email LIKE \'xtest%\' and LastName = '{TEST_DATA['STR_15_RANDOM']}'"
                 " ORDER BY Id")

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

        sdc_executor.start_pipeline(pipeline)
        timeout_sec = BULK_PIPELINE_TIMEOUT_SECONDS if api == 'bulk' else SOAP_PIPELINE_TIMEOUT_SECONDS
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(test_data), timeout_sec=timeout_sec)

        if subscription_type == PUSH_TOPIC:
            verify_wiretap_data(wiretap, test_data)
        else:
            # cannot verify CDC at this point as we are not replaying all events prior to pipeline start
            pass

        # Note, from Salesforce docs: "Updates performed by the Bulk API won’t generate notifications, since such
        # updates could flood a channel."
        # REST API in Simple Salesforce can only create one record at a time, so just update one Contact
        test_data[0]['FirstName'] = 'Updated FirstName'
        contact_id = inserted_ids[0]['Id']
        client.Contact.update(contact_id, test_data[0])
        logger.info('Updated a Contact using Salesforce client with id as %s', contact_id)

        logger.info('Capturing second batch of data ...')
        wiretap.reset()
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              len(test_data) + 1,
                                              timeout_sec=timeout_sec)

        if subscription_type == PUSH_TOPIC:
            verify_wiretap_data(wiretap, [test_data[0]])
        else:
            change_records = get_cdc_wiretap_records(wiretap, [contact_id])
            assert change_records
            change_record = change_records[0]
            assert change_record.header.values['salesforce.cdc.changeType'] == 'UPDATE'
            assert change_record.field['Name']['FirstName'] == test_data[0]['FirstName']
    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)
        if subscription_id and subscription_type == PUSH_TOPIC:
            logger.info('Deleting PushTopic with id %s ...', subscription_id)
            client.PushTopic.delete(subscription_id)


@salesforce
@sdc_min_version('3.12.0')
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
    account_id = None
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        salesforce_origin = pipeline_builder.add_stage('Salesforce', type='origin')
        # Since we are gonna retrieve all events, lets increase buffer size (from default 1048576 bytes)
        salesforce_origin.set_attributes(query_existing_data=False,
                                         subscribe_for_notifications=True,
                                         subscription_type=CDC,
                                         change_data_capture_object=ACCOUNT,
                                         replay_option=ALL_EVENTS,
                                         streaming_buffer_size=10485760)

        wiretap = pipeline_builder.add_wiretap()
        salesforce_origin >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting pipeline ...')
        sdc_executor.start_pipeline(pipeline)

        # create change data
        test_data = {'Name': 'Test1', 'Fax': 'testFax'}

        account = client.Account.create(test_data)
        account_id = account['id']
        logger.info('Created an Account using Salesforce client with id as %s', account_id)

        def failure(timeout):
            raise TimeoutError('Timed out after {} seconds waiting to get CDC record(s) for Account id {}'.format(
                               timeout, account_id))

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=300)
        current_count = None
        change_records = []

        def change_records_condition():
            nonlocal current_count
            # we make sure pipeline is running - it cannot if it has API issues with Salesforce (like timeout) and
            # we don't want to keep looping in the condition
            if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') != 'RUNNING':
                raise Exception('Pipeline is not in a RUNNING state. It possibly errored out')
            metrics = sdc_executor.get_pipeline_metrics(pipeline)
            if metrics:
                new_count = metrics.pipeline.output_record_count
                if not current_count or new_count > current_count:
                    change_records.extend([record for record in wiretap.output_records
                                           if record.header.values.get('salesforce.cdc.recordIds') == account_id])
                    if change_records:
                        return True
                current_count = new_count

        wait_for_condition(condition=change_records_condition, failure=failure, time_between_checks=10, timeout=300)

        assert change_records
        change_record = change_records[0]
        assert change_record.field['Name'] == 'Test1'
        assert change_record.field['Fax'] == 'testFax'

        logger.info('Stopping pipeline after success ...')
        sdc_executor.stop_pipeline(pipeline)
    finally:
        if account_id:
            logger.info('Deleting Contact with id %s ...', account_id)
            client.account.delete(account_id)
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline after possible failure ...')
            try:
                # An errored pipeline cannot be stopped as pipeline's Salesforce connection seems like it goes into
                # retry. Till SDC-16822 is resolved, we will have to force stop so as other tests which rely on
                # Salesforce API wont get impacted
                sdc_executor.stop_pipeline(pipeline, timeout_sec=120)
            except:
                sdc_executor.stop_pipeline(pipeline, force=True)
                raise


@salesforce
@sdc_min_version('4.2.0')
@pytest.mark.parametrize('api', ['soap', 'bulk'])
@pytest.mark.parametrize('delete_type', ['soft', 'hard'])
@pytest.mark.parametrize('set_permission', [True, False])
def test_salesforce_destination_delete(sdc_builder, sdc_executor, salesforce, api, delete_type, set_permission):
    """Insert records into Salesforce and try to delete them.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    if api == 'soap' and delete_type == 'hard':
        pytest.skip('Skipping... Hard delete is not supported in the Salesforce SOAP API')

    if delete_type == 'hard' and not set_permission:
        pytest.skip('This test cannot be run in automatic executions, since most Salesforce tests also use hard deletes'
                    'to clean up the environment and the permission is set at account level, so this scenario does not'
                    'fail as the test expects it to do. Test manually if needed.')

    pipeline_builder = sdc_builder.get_pipeline_builder()

    inserted_ids = None
    user_id = None
    permission_set_id = None
    permission_set_assignment_id = None
    client = salesforce.client
    try:
        # Ensure one test doesn't see records in the bin from another
        test_data = copy.deepcopy(TEST_DATA['DATA_TO_INSERT'])
        last_name = TEST_DATA   ['STR_15_RANDOM'] + api + delete_type
        for item in test_data:
            item['LastName'] = last_name

        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = get_ids(client.bulk.Contact.insert(test_data), 'id')

        # Make CSV list of record ids to delete
        raw_data = [record['Id'] for record in inserted_ids]
        raw_data.insert(0, 'Id')

        dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, raw_data)

        salesforce_destination = pipeline_builder.add_stage('Salesforce', type='destination')
        salesforce_destination.set_attributes(default_operation='DELETE',
                                              sobject_type=CONTACT,
                                              use_bulk_api=(api == 'bulk'),
                                              hard_delete_records=(delete_type == 'hard'))

        dev_raw_data_source >> salesforce_destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Starting Salesforce destination pipeline and waiting for it to delete records ...')
        if set_permission or delete_type == 'soft':
            # Start the pipeline as normal
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
        else:
            # Check hard delete fails if we didn't assign the permission
            with pytest.raises(StatusError) as run_error:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert 'FORCE_59' in str(run_error.value)

        logger.info('Querying for records...')
        query_str = (f"SELECT Id FROM Contact WHERE Lastname = '{last_name}'")
        result = client.query(query_str)

        if set_permission or delete_type == 'soft':
            # Are the records gone?
            assert 0 == len(result['records'])
        else:
            # Deletion failed as expected, records are still there
            verify_result_ids(inserted_ids, result)

        logger.info('Querying for deleted records...')
        query_str = ("SELECT Id FROM Contact"
                     f" WHERE LastName = '{last_name}' AND isDeleted = TRUE"
                     " ORDER BY Id")
        result = client.query(query_str, include_deleted=True)

        if set_permission or delete_type == 'soft':
            # Records should show up as deleted
            verify_result_ids(inserted_ids, result)
        else:
            # Delete should have failed
            assert 0 == len(result['records'])

        logger.info('Querying the recycle bin...')
        query_str = ("SELECT Record FROM DeleteEvent"
                     f" WHERE SobjectName = 'Contact' AND RecordName LIKE '% {last_name}'"
                     " ORDER BY Record")
        result = client.query(query_str)

        if delete_type == 'hard':
            # Nothing should be in the recycle bin, no matter how permissions were set
            assert 0 == len(result['records'])
        else:
            # Soft delete - our records should be in the bin
            verify_result_ids(inserted_ids, result, 'Record')

    finally:
        if set_permission:
            clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)
        else:
            clean_up(sdc_executor, pipeline, client, inserted_ids)
