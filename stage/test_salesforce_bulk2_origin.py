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

import logging
import string
from uuid import uuid4

import pytest
import requests
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

from .utils.utils_salesforce import (insert_data_and_verify_using_wiretap, verify_wiretap_data,
                                     clean_up, CONTACTS_FOR_NO_MORE_DATA, get_ids, set_up_random,
                                     TEST_DATA, TIMEOUT, check_ids, BULK_PIPELINE_TIMEOUT_SECONDS, assign_hard_delete,
                                     revoke_hard_delete)

ACCOUNTS_FOR_BULK_QUERY = 100

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def _set_up_random(salesforce):
    set_up_random(salesforce)


@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('prefixed_query', [True, False])  # Testing of SDC-9067
def test_salesforce_origin(sdc_builder, sdc_executor, salesforce, prefixed_query):
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

    salesforce_origin = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     incremental_mode=False)
    salesforce_origin >= pipeline_builder.add_stage("Pipeline Finisher Executor")

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, TEST_DATA['DATA_TO_INSERT'], salesforce,
                                         TEST_DATA['DATA_TO_INSERT'])


@salesforce
@sdc_min_version('5.0.0')
def test_salesforce_origin_nulls(sdc_builder, sdc_executor, salesforce):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using wiretap. This test checks that nulls are correctly
    read for fields that are not set in Salesforce (SDC-11086).

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
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

    salesforce_origin = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     incremental_mode=False)
    salesforce_origin >= pipeline_builder.add_stage("Pipeline Finisher Executor")

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, expected_data, salesforce, expected_data)


@salesforce
@sdc_min_version('5.0.0')
def test_salesforce_origin_datetime(sdc_builder, sdc_executor, salesforce):
    """Create data using Salesforce client and then check if Salesforce origin
    receives them using wiretap. This test checks that datetime fields can
    be used as offsets (SDC-10352).

    The pipeline looks like:
        salesforce_origin >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = ("SELECT Id, FirstName, LastName, Email, LeadSource, SystemModstamp "
             "FROM Contact WHERE SystemModstamp > ${OFFSET} and "
             f"Lastname = '{TEST_DATA['STR_15_RANDOM']}'"
             " ORDER BY SystemModstamp")

    salesforce_origin = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     incremental_mode=True,
                                     initial_offset='2018-10-16T00:00:00.000Z',
                                     offset_field='SystemModstamp')

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_origin_datetime')

        # Using Salesforce client, create rows in Contact.
        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = check_ids(get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id'))

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              3,
                                              timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
        sdc_executor.stop_pipeline(pipeline)

        verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

        wiretap.reset()
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              0,
                                              timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
        sdc_executor.stop_pipeline(pipeline)

        assert len([record.value['value'] for record in wiretap.output_records]) == 0
    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.0.0')
def test_salesforce_origin_session_timeout(sdc_builder, sdc_executor, salesforce):
    """Test that Salesforce origin correctly handles a session timing out while the pipeline
    is running

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
             "Email LIKE 'xtest%' "
             f"AND LastName = '{TEST_DATA['STR_15_RANDOM']}'"
             "ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     incremental_mode=False,
                                     query_interval=60)

    wiretap = pipeline_builder.add_wiretap()
    salesforce_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    inserted_ids = None
    try:
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_origin_session_timeout')

        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = check_ids(get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id'))

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              3,
                                              timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
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
                                              timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
        verify_wiretap_data(wiretap, TEST_DATA['DATA_TO_INSERT'])

    finally:
        clean_up(sdc_executor, pipeline, client, inserted_ids, hard_delete=True)
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.0.0')
def test_salesforce_origin_no_more_data(sdc_builder, sdc_executor, salesforce):
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
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Constrain the size of the result set with LIMIT clause; pipeline should still retrieve all the data before
    # triggering no-more-data
    query = ("SELECT Id, Name FROM Contact WHERE Id > '${OFFSET}' "
             f"AND LastName = \'{TEST_DATA['STR_15_RANDOM']}\' ORDER BY Id LIMIT 10")

    salesforce_origin = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     incremental_mode=True,
                                     query_interval='${1 * SECONDS}')

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
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_origin_no_more_data')

        # Make a load of Contacts
        data_to_insert = []
        for aux in range(CONTACTS_FOR_NO_MORE_DATA):
            data_to_insert.append({'FirstName': get_random_string(string.ascii_letters, 10).lower(),
                                   'LastName': TEST_DATA['STR_15_RANDOM'],
                                   'Email': f"{get_random_string(string.ascii_letters, 10).lower()}@example.com"})

        logger.info('Creating Contact records using Salesforce client ...')
        contact_ids = check_ids(get_ids(client.bulk.Contact.insert(data_to_insert), 'id'))

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
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.0.0')
def test_salesforce_origin_datetime_in_history(sdc_builder, sdc_executor, salesforce):
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
    """
    client = salesforce.client

    acc, con = None, None

    try:
        # Create an account
        acc = client.Account.create({'Name': str(uuid4())})
        assert acc['id'], "Error creating Salesforce data"

        # Create a contract for that account
        con = client.Contract.create({'AccountId': acc['id']})
        assert con['id'], "Error creating Salesforce data"

        # Update the contract status - this will have the side effect of updating ActivatedDate
        client.Contract.update(con['id'], {'Status': 'Activated'})

        query = f"SELECT Id, NewValue FROM ContractHistory WHERE Field = 'ActivatedDate' AND ContractId = '{con['id']}'"

        pipeline_builder = sdc_builder.get_pipeline_builder()

        salesforce_origin = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='origin')
        salesforce_origin.set_attributes(soql_query=query,
                                         incremental_mode=False,
                                         disable_query_validation=True)
        salesforce_origin >= pipeline_builder.add_stage("Pipeline Finisher Executor")

        wiretap = pipeline_builder.add_wiretap()
        salesforce_origin >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              1,
                                              timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
        sdc_executor.stop_pipeline(pipeline)

        # There should be a single row with Id and NewValue fields. For Bulk API it's a STRING
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['Id']
        assert wiretap.output_records[0].field['NewValue'].type == 'STRING'

    finally:
        if con and con['id']:
            client.Contract.delete(con['id'])
        if acc and acc['id']:
            client.Account.delete(acc['id'])


@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('threading', ['multithread', 'single_thread'])
@pytest.mark.parametrize('incremental_mode', ['incremental', 'full'])
def test_salesforce_origin_threading(sdc_builder, sdc_executor, salesforce, threading, incremental_mode):
    """Creates data using Salesforce client and then checks if Salesforce origin
    reads all of the records.

    Test creates 100 accounts, sets maximum_records_per_query_result_set to 10 and
    number_of_processor_threads to 3 (if threading is set to 'multithread'. The
    parameters ensure all the combinations of maintaining record order, threading
    and full vs incremental mode are tested.

    The test uses NumberOfEmployees to check record ordering, and puts a delay in
    the first batch so that, by default, data will be emitted from the origin out
    of order unless keep_results_in_order is enabled in the origin.

    Default pipeline looks like:
        salesforce_origin >> expression_evaluator >> wiretap
        salesforce_origin >= finisher

    Ordered pipeline looks like:
        salesforce_origin >> expression_evaluator >> stream_selector >>       >> wiretap
                                                                        delay >>
        salesforce_origin >= finisher

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    query = ("SELECT Id, Name, NumberOfEmployees FROM Account "
             "WHERE Id > '${OFFSET}' AND "
             f"Name LIKE '{TEST_DATA['STR_15_RANDOM']}%' "
             "ORDER BY Id")

    salesforce_origin = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    salesforce_origin.set_attributes(soql_query=query,
                                     incremental_mode=(incremental_mode == 'incremental'),
                                     query_interval='${1 * SECONDS}',
                                     number_of_threads=3 if threading == 'multithread' else 1,
                                     maximum_records_per_query_result_set=10)
    salesforce_origin >= pipeline_builder.add_stage("Pipeline Finisher Executor")

    wiretap = pipeline_builder.add_wiretap()

    salesforce_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    account_ids = None
    try:
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_origin_threading')

        # Using Salesforce client, create Account records
        logger.info(f'Creating {ACCOUNTS_FOR_BULK_QUERY} accounts using Salesforce client ...')
        expected_accounts = []
        account_ids = []
        for i in range(ACCOUNTS_FOR_BULK_QUERY):
            expected_accounts.append({
                'Name': f"{TEST_DATA['STR_15_RANDOM']} {i}",
                'NumberOfEmployees': i
            })

        account_ids = check_ids(get_ids(client.bulk.Account.insert(expected_accounts), 'id'))

        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        source_ids = {record.header['sourceId'] for record in wiretap.output_records}
        assert len(source_ids) == len(wiretap.output_records)

        rows_from_wiretap = [record.field for record in wiretap.output_records]
        data_from_wiretap = [{field: record[field] for field in record if field not in ['Id', 'SystemModstamp']}
                             for record in rows_from_wiretap]
        data_from_wiretap = sorted(data_from_wiretap,
                                   key=lambda k: k['NumberOfEmployees'].value)

        assert data_from_wiretap == expected_accounts

    finally:
        clean_up(sdc_executor, pipeline, client, account_ids, hard_delete=True, object_name='Account')
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('max_columns', [1, 512])
def test_salesforce_origin_max_columns(sdc_builder, sdc_executor, salesforce, max_columns):
    # The test tries to set up max query columns as 1 and as 512. Since the query is retrieving 2 columns (Id and
    # Firstname) the execution fails in the case of max_columns=1 with error FORCE_55.
    run_name = 'sale_bulk2_origin_max_columns_' + get_random_string(string.ascii_lowercase, 10)
    client = salesforce.client

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    query = (f"SELECT Id, FirstName FROM Contact "
             "WHERE Id > '${OFFSET}' "
             f"AND LastName = '{run_name}' "
             "ORDER BY Id")
    origin.set_attributes(soql_query=query,
                          incremental_mode=False,
                          maximum_query_columns=max_columns)

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    record_id = None

    try:
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_origin_max_columns')

        logger.info('Adding a Contact into Salesforce ...')

        result = client.Contact.create({
            'FirstName': '1',
            'LastName': run_name
        })
        record_id = {'Id': result['id']}

        execution = sdc_executor.start_pipeline(pipeline)

        if max_columns == 512:
            # Run the pipeline normally and expect to retrieve the Contact
            sdc_executor.wait_for_pipeline_metric(pipeline,
                                                  'input_record_count',
                                                  1,
                                                  timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
            sdc_executor.stop_pipeline(pipeline)

            # There should be no errors reported
            history = sdc_executor.get_pipeline_history(pipeline)
            assert history.latest.metrics.counter('stage.SalesforceBulkAPI20_01.errorRecords.counter').count == 0
            assert history.latest.metrics.counter('stage.SalesforceBulkAPI20_01.stageErrors.counter').count == 0

            assert len(wiretap.output_records) == 1
            assert wiretap.output_records[0].field['FirstName'] == '1'
        else:
            # This execution should fail as max_columns=1
            execution.wait_for_status('RUN_ERROR', timeout_sec=300, ignore_errors=True)

            # Check that the error is the one we expect
            status = sdc_executor.get_pipeline_status(pipeline).response.json()
            assert status.get('status') == 'RUN_ERROR'
            assert 'FORCE_55' in status.get('message')

    finally:
        clean_up(sdc_executor, pipeline, client, [record_id], hard_delete=True)
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('timeout', [0, 60])
def test_salesforce_origin_timeout(sdc_builder, sdc_executor, salesforce, timeout):
    # The test tries to set up Salesforce query timeout as 0 and as 60. Whith the timeout set to 0, the execution is
    # expected to fail with FORCE_59 error, otherwise it should execute just fine.

    if timeout == 0:
        pytest.skip("This test would take more than 25 minutes. Automatic test disabled. Test manually at will if you need so.")

    run_name = 'sale_bulk2_origin_timeout_' + get_random_string(string.ascii_lowercase, 10)
    client = salesforce.client

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    query = (f"SELECT Id, FirstName FROM Contact "
             "WHERE Id > '${OFFSET}' "
             f"AND LastName = '{run_name}' "
             "ORDER BY Id")
    origin.set_attributes(soql_query=query,
                          incremental_mode=False,
                          salesforce_query_timeout=timeout)

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    record_id = None

    try:
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_origin_timeout')

        logger.info('Adding a Contact into Salesforce ...')

        result = client.Contact.create({
            'FirstName': '1',
            'LastName': run_name
        })
        record_id = {'Id': result['id']}

        execution = sdc_executor.start_pipeline(pipeline)

        if timeout == 60:
            # Run the pipeline normally and expect to retrieve the Contact
            sdc_executor.wait_for_pipeline_metric(pipeline,
                                                  'input_record_count',
                                                  1,
                                                  timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
            sdc_executor.stop_pipeline(pipeline)

            # There should be no errors reported
            history = sdc_executor.get_pipeline_history(pipeline)
            assert history.latest.metrics.counter('stage.SalesforceBulkAPI20_01.errorRecords.counter').count == 0
            assert history.latest.metrics.counter('stage.SalesforceBulkAPI20_01.stageErrors.counter').count == 0

            assert len(wiretap.output_records) == 1
            assert wiretap.output_records[0].field['FirstName'] == '1'
        else:
            # This execution should fail as timeout=0
            execution.wait_for_status('RUN_ERROR', timeout_sec=3600, ignore_errors=True)

            # Check that the error is the one we expect
            status = sdc_executor.get_pipeline_status(pipeline).response.json()
            assert status.get('status') == 'RUN_ERROR'
            assert 'FORCE_60' in status.get('message')

    finally:
        clean_up(sdc_executor, pipeline, client, [record_id], hard_delete=True)
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)
