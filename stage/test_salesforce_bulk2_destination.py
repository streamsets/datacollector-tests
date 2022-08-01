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

import copy
import json
import logging
import string

import pytest
from streamsets.sdk.sdc_api import StatusError
from streamsets.sdk.utils import get_random_string
from streamsets.testframework.markers import salesforce, sdc_min_version

from .utils.utils_salesforce import (CASE_SUBJECT, clean_up, get_dev_raw_data_source,
                                     get_ids, set_up_random, TEST_DATA, assign_hard_delete,
                                     revoke_hard_delete, verify_result_ids, FORCE_60,
                                     check_ids)

CONTACT = 'Contact'
COLON = ':'
PERIOD = '.'

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def _set_up_random(salesforce):
    set_up_random(salesforce)


@salesforce
@sdc_min_version('5.0.0')
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

    field_mapping = [{'sdcField': '/FirstName', 'salesforceField': 'FirstName'},
                     {'sdcField': '/LastName', 'salesforceField': 'LastName'},
                     {'sdcField': '/Email', 'salesforceField': 'Email'},
                     {'sdcField': '/LeadSource', 'salesforceField': 'LeadSource'}]
    salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          sobject_type=CONTACT)

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    read_ids = None

    client = salesforce.client
    try:
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_destination')

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
        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True)
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.0.0')
def test_salesforce_destination_default_mapping(sdc_builder, sdc_executor, salesforce):
    """Send text to Salesforce destination from Dev Raw Data Source and confirm
    that Salesforce destination successfully reads them using Salesforce client.
    This test checks that field name mappings are correctly applied.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Replace field names in raw data, map them back in destination
    TEST_DATA['DATA_TO_INSERT'] = [item.replace('Email', 'em').replace('LeadSource', 'ls') for item in
                                   TEST_DATA['CSV_DATA_TO_INSERT']]
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['DATA_TO_INSERT'])

    salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    # FirstName and LastName should be mapped by default
    field_mapping = [{'sdcField': '/em', 'salesforceField': 'Email'},
                     {'sdcField': '/ls', 'salesforceField': 'LeadSource'}]
    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          sobject_type=CONTACT)

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)
    read_ids = None

    client = salesforce.client
    try:
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_destination_default_mapping')

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
        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True)
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.0.0')
def test_salesforce_destination_datetime(sdc_builder, sdc_executor, salesforce):
    """Test that datetimes are correctly written to Salesforce (SDC-12193).
    Create an Event record as this is one of the few standard objects with a
    settable Datetime

    The pipeline looks like:
        salesforce_origin >> trash

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
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

    salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    field_mapping = [{'sdcField': '/ActivityDateTime', 'salesforceField': 'ActivityDateTime'},
                     {'sdcField': '/IsAllDayEvent', 'salesforceField': 'IsAllDayEvent'},
                     {'sdcField': '/DurationInMinutes', 'salesforceField': 'DurationInMinutes'},
                     {'sdcField': '/Description', 'salesforceField': 'Description'},
                     {'sdcField': '/Location', 'salesforceField': 'Location'},
                     {'sdcField': '/Description', 'salesforceField': 'Description'}]

    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          sobject_type='Event')

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
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('separator', [COLON, PERIOD])
def test_salesforce_destination_relationship(sdc_builder, sdc_executor, salesforce, separator):
    """Test that we can write to related external ID fields (SDC-12636).

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

    # Email value should not exist in the database
    TEST_DATA['DATA_TO_INSERT'][1]["Email"] = f"{TEST_DATA['STR_15_RANDOM']}_1@example.com"
    TEST_DATA['DATA_TO_INSERT'][2]["Email"] = f"{TEST_DATA['STR_15_RANDOM']}_2@example.com"
    inserted_ids = check_ids(get_ids(client.bulk.Contact.insert(TEST_DATA['DATA_TO_INSERT']), 'id'))

    # Relate the created contacts to each other
    # first contact reports to second contact; second contact reports to third
    TEST_DATA['CSV_DATA_TO_INSERT'] = ['Id,ReportsTo.Email']
    TEST_DATA['CSV_DATA_TO_INSERT'].append(f"{inserted_ids[0]['Id']},{TEST_DATA['DATA_TO_INSERT'][1]['Email']}")
    TEST_DATA['CSV_DATA_TO_INSERT'].append(f"{inserted_ids[1]['Id']},{TEST_DATA['DATA_TO_INSERT'][2]['Email']}")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['CSV_DATA_TO_INSERT'])

    salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    field_mapping = [{'sdcField': '/Id', 'salesforceField': 'Id'},
                     {'sdcField': '/ReportsTo.Email', 'salesforceField': f'ReportsTo{separator}Email'}]
    salesforce_destination.set_attributes(default_operation='UPDATE',
                                          field_mapping=field_mapping,
                                          sobject_type=CONTACT)

    dev_raw_data_source >> salesforce_destination

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_destination_relationship')

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
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.0.0')
def test_salesforce_destination_polymorphic(sdc_builder, sdc_executor, salesforce):
    """Test that we can write to polymorphic external ID fields (SDC-13117).
    Create a case, since its owner can be a user or a group.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    client = salesforce.client

    # Using Salesforce client, create a Case
    logger.info('Creating rows using Salesforce client ...')
    result = client.Case.create({'Subject': CASE_SUBJECT})
    case_id = result['id']
    assert case_id, "Error creating Salesforce data"

    # Set the case owner. Even though we're not changing the owner, SDC-13117 would cause an error to
    # be thrown due to the bad syntax for the field name
    TEST_DATA['CSV_DATA_TO_INSERT'] = ['Id,Owner']
    TEST_DATA['CSV_DATA_TO_INSERT'].append(f'{case_id},{salesforce.username}')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, TEST_DATA['CSV_DATA_TO_INSERT'])

    salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    field_mapping = [{'sdcField': '/Id', 'salesforceField': 'Id'},
                     {'sdcField': '/Owner', 'salesforceField': 'User:Owner.Username'}]
    salesforce_destination.set_attributes(default_operation='UPDATE',
                                          field_mapping=field_mapping,
                                          sobject_type='Case')

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
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('delete_type', ['soft', 'hard'])
@pytest.mark.parametrize('set_permission', [True, False])
def test_salesforce_destination_delete(sdc_builder, sdc_executor, salesforce, delete_type, set_permission):
    """Insert records into Salesforce and try to delete them.

    The pipeline looks like:
        dev_raw_data_source >> salesforce_destination

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
    """
    if delete_type == 'hard' and not set_permission:
        pytest.skip('This test cannot be run in automatic executions, since most Salesforce tests also use hard deletes'
                    'to clean up the environment and the permission is set at account level, so this scenario does not'
                    'fail as the test expects it to do. Test manually if needed.')

    pipeline_builder = sdc_builder.get_pipeline_builder()

    inserted_ids = None
    pipeline = None
    client = salesforce.client
    try:
        # Ensure one test doesn't see records in the bin from another
        test_data = copy.deepcopy(TEST_DATA['DATA_TO_INSERT'])
        last_name = TEST_DATA   ['STR_15_RANDOM'] + delete_type
        for item in test_data:
            item['LastName'] = last_name

        logger.info('Creating rows using Salesforce client ...')
        inserted_ids = check_ids(get_ids(client.bulk.Contact.insert(test_data), 'id'))

        # Make CSV list of record ids to delete
        raw_data = [record['Id'] for record in inserted_ids]
        raw_data.insert(0, 'Id')

        dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, raw_data)

        salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
        salesforce_destination.set_attributes(default_operation='DELETE',
                                              sobject_type=CONTACT,
                                              hard_delete_records=(delete_type == 'hard'))

        dev_raw_data_source >> salesforce_destination

        pipeline = pipeline_builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)

        if set_permission:
            permission_set_id = assign_hard_delete(client, 'test_salesforce_destination_delete')

        logger.info('Starting Salesforce destination pipeline and waiting for it to delete records ...')
        if set_permission or delete_type == 'soft':
            # Start the pipeline as normal
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
        else:
            # Check hard delete fails if we didn't assign the permission
            with pytest.raises(StatusError) as e:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert FORCE_60 in str(e.value)

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
            revoke_hard_delete(client, permission_set_id)
        else:
            clean_up(sdc_executor, pipeline, client, inserted_ids)


@salesforce
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('timeout', [0, 60])
def test_salesforce_destination_timeout(sdc_builder, sdc_executor, salesforce, timeout):
    # The test tries to set up Salesforce query timeout as 0 and as 60. Whith the timeout set to 0, the execution is
    # expected to fail with FORCE_59, otherwise it should execute just fine.

    if timeout == 0:
        pytest.skip("This test would take more than 25 minutes. Automatic test disabled. Test manually at will if you need so.")

    test_name = 'sale_bulk2_dest_timeout_' + get_random_string(string.ascii_lowercase, 10)
    client = salesforce.client

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "FirstName" : 1 }}'
    source.stop_after_first_batch = True

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        'fieldToSet': '/LastName',
        'expression': test_name
    }]

    target = builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    target.sobject_type = 'Contact'
    target.field_mapping = []
    target.on_record_error = 'STOP_PIPELINE'
    target.salesforce_query_timeout = timeout

    source >> expression >> target
    pipeline = builder.build().configure_for_environment(salesforce)

    read_ids = []

    try:
        # Create a hard delete permission file for this client
        permission_set_id = assign_hard_delete(client, 'test_salesforce_destination_timeout')

        sdc_executor.add_pipeline(pipeline)

        execution = sdc_executor.start_pipeline(pipeline)

        if timeout == 60:
            # Run the pipeline normally and expect to retrieve the Contact
            execution.wait_for_finished()

            # Verify that the data were indeed inserted
            result = client.query(f"SELECT Id, FirstName FROM Contact WHERE LastName = '{test_name}'")
            read_ids = get_ids(result['records'], 'Id')

            assert len(result['records']) == 1
            assert result['records'][0]['FirstName'] == '1'
        else:
            # This execution should fail as timeout=0
            execution.wait_for_status('RUN_ERROR', timeout_sec=3600, ignore_errors=True)

            # Check that the error is the one we expect
            status = sdc_executor.get_pipeline_status(pipeline).response.json()
            assert status.get('status') == 'RUN_ERROR'
            assert 'FORCE_63' in status.get('message')
    finally:
        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True)
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client, permission_set_id)

@salesforce
@sdc_min_version('5.2.0')
@pytest.mark.parametrize('default_operation', ['INSERT',
                                               'UPDATE',
                                               'UPSERT'])
def test_salesforce_destination_error_records(sdc_builder, sdc_executor, salesforce, default_operation):

    test_token = get_random_string(string.ascii_letters, 16)

    test_data = [
        {'Id': '0',
         'FirstName': 'Georg',
         'LastName': 'Cantor',
         'Email': f'{test_token}.georg.cantor@saint.petesburg.mathematics.ru',
         'LeadSource': 'Set Theory'},
        {'Id': '1',
         'FirstName': 'Evariste',
         'LastName': 'Galois',
         'Email': f'{test_token}.evariste.galosi@bourg.la.reine.mathematics.fr',
         'LeadSource': 'Group Theory'},
        {'Id': '2',
         'FirstName': 'Kurt',
         'LastName': 'Gödel',
         'Email': f'{test_token}.kurt.godel@brunn.mathematics.at',
         'LeadSource': 'Mathematical Logic'},
        {'Id': '3',
         'FirstName': 'Sofya',
         'LastName': 'Kovalevskaya',
         'Email': f'{test_token}.sofya.kovalevskaya@moscow.mathematics.ru',
         'LeadSource': 'Complex Analysis'},
        {'Id': '4',
         'FirstName': 'Maryam',
         'LastName': 'Mirzakhani',
         'Email': f'{test_token}.maryamn.mirzakhani@teheran.mathematics.ir',
         'LeadSource': 'Teichmüller Theory'},
        {'Id': '5',
         'FirstName': 'Emmy',
         'LastName': 'Noether',
         'Email': f'{test_token}.emmy.noether@erlangen.mathematics.de',
         'LeadSource': 'Abstract Algebra'},
        {'Id': '6',
         'FirstName': 'Srinivasa',
         'LastName': 'Ramanujan',
         'Email': f'{test_token}.srinivasa.ramanujan@erode.mathematics.in',
         'LeadSource': 'Infinite Series'},
        {'Id': '7',
         'FirstName': 'Maryna',
         'LastName': 'Viazovska',
         'Email': f'{test_token}.maryna.viazovska@kyiv.mathematics.ua',
         'LeadSource': 'Number Theory'}
    ]

    input_data = [','.join(test_data[0].keys())] + [','.join(item.values()) for item in test_data]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, input_data)

    field_mapping = [{'sdcField': '/Id', 'salesforceField': 'Id'},
                     {'sdcField': '/FirstName', 'salesforceField': 'FirstName'},
                     {'sdcField': '/LastName', 'salesforceField': 'LastName'},
                     {'sdcField': '/Email', 'salesforceField': 'Email'},
                     {'sdcField': '/LeadSource', 'salesforceField': 'LeadSource'}]
    salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    salesforce_destination.set_attributes(default_operation=default_operation,
                                          external_id_field='Email',
                                          field_mapping=field_mapping,
                                          sobject_type='Contact')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [salesforce_destination, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    client = salesforce.client
    try:
        permission_set_id = assign_hard_delete(client, 'test_salesforce_destination')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        error_records = wiretap.error_records

        assert len(output_records) == 8
        assert len(error_records) == 8

    finally:
        query = ("   select Id,"
                 "          Email "
                 "     from Contact "
                 f"   where Email like '%{test_token}%' "
                 "      and Email like '%mathematics%'"
                 " order by Id")
        result = client.query(query)
        ids = get_ids(result['records'], 'Id')
        clean_up(sdc_executor, pipeline, client, ids, hard_delete=True)
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.2.0')
@pytest.mark.parametrize('for_insert, for_update, expected_errors', [('1', '1', 0),
                                                                     ('1', '3', 8),
                                                                     ('3', '1', 8),
                                                                     ('3', '3', 16)])
def test_salesforce_destination_header_operation(sdc_builder,
                                                 sdc_executor,
                                                 salesforce,
                                                 for_insert,
                                                 for_update,
                                                 expected_errors):

    test_token = get_random_string(string.ascii_letters, 16)

    test_data = [
        {'FirstName': 'Georg',
         'LastName': 'Cantor',
         'Email': f'1_{test_token}.georg.cantor@saint.petesburg.mathematics.ru',
         'LeadSource': 'Set Theory'},
        {'FirstName': 'Evariste',
         'LastName': 'Galois',
         'Email': f'1_{test_token}.evariste.galosi@bourg.la.reine.mathematics.fr',
         'LeadSource': 'Group Theory'},
        {'FirstName': 'Kurt',
         'LastName': 'Gödel',
         'Email': f'1_{test_token}.kurt.godel@brunn.mathematics.at',
         'LeadSource': 'Mathematical Logic'},
        {'FirstName': 'Sofya',
         'LastName': 'Kovalevskaya',
         'Email': f'1_{test_token}.sofya.kovalevskaya@moscow.mathematics.ru',
         'LeadSource': 'Complex Analysis'},
        {'FirstName': 'Maryam',
         'LastName': 'Mirzakhani',
         'Email': f'1_{test_token}.maryamn.mirzakhani@teheran.mathematics.ir',
         'LeadSource': 'Teichmüller Theory'},
        {'FirstName': 'Emmy',
         'LastName': 'Noether',
         'Email': f'1_{test_token}.emmy.noether@erlangen.mathematics.de',
         'LeadSource': 'Abstract Algebra'},
        {'FirstName': 'Srinivasa',
         'LastName': 'Ramanujan',
         'Email': f'1_{test_token}.srinivasa.ramanujan@erode.mathematics.in',
         'LeadSource': 'Infinite Series'},
        {'FirstName': 'Maryna',
         'LastName': 'Viazovska',
         'Email': f'1_{test_token}.maryna.viazovska@kyiv.mathematics.ua',
         'LeadSource': 'Number Theory'},
        {'FirstName': 'Georg',
         'LastName': 'Cantor',
         'Email': f'2_{test_token}.georg.cantor@saint.petesburg.mathematics.ru',
         'LeadSource': 'Set Theory'},
        {'FirstName': 'Evariste',
         'LastName': 'Galois',
         'Email': f'2_{test_token}.evariste.galosi@bourg.la.reine.mathematics.fr',
         'LeadSource': 'Group Theory'},
        {'FirstName': 'Kurt',
         'LastName': 'Gödel',
         'Email': f'2_{test_token}.kurt.godel@brunn.mathematics.at',
         'LeadSource': 'Mathematical Logic'},
        {'FirstName': 'Sofya',
         'LastName': 'Kovalevskaya',
         'Email': f'2_{test_token}.sofya.kovalevskaya@moscow.mathematics.ru',
         'LeadSource': 'Complex Analysis'},
        {'FirstName': 'Maryam',
         'LastName': 'Mirzakhani',
         'Email': f'2_{test_token}.maryamn.mirzakhani@teheran.mathematics.ir',
         'LeadSource': 'Teichmüller Theory'},
        {'FirstName': 'Emmy',
         'LastName': 'Noether',
         'Email': f'2_{test_token}.emmy.noether@erlangen.mathematics.de',
         'LeadSource': 'Abstract Algebra'},
        {'FirstName': 'Srinivasa',
         'LastName': 'Ramanujan',
         'Email': f'2_{test_token}.srinivasa.ramanujan@erode.mathematics.in',
         'LeadSource': 'Infinite Series'},
        {'FirstName': 'Maryna',
         'LastName': 'Viazovska',
         'Email': f'2_{test_token}.maryna.viazovska@kyiv.mathematics.ua',
         'LeadSource': 'Number Theory'}
    ]

    input_data = [','.join(test_data[0].keys())] + [','.join(item.values()) for item in test_data]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, input_data)

    header_attribute_expressions = f"${{str:startsWith(record:value('/Email'), '1')? '{for_insert}' : '{for_update}'}}"

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': header_attribute_expressions}
    ])

    field_mapping = [{'sdcField': '/FirstName', 'salesforceField': 'FirstName'},
                     {'sdcField': '/LastName', 'salesforceField': 'LastName'},
                     {'sdcField': '/Email', 'salesforceField': 'Email'},
                     {'sdcField': '/LeadSource', 'salesforceField': 'LeadSource'}]
    salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    salesforce_destination.set_attributes(default_operation='DELETE',
                                          field_mapping=field_mapping,
                                          sobject_type='Contact')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source  >> expression_evaluator >> [salesforce_destination, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    read_ids = None

    client = salesforce.client
    try:
        permission_set_id = assign_hard_delete(client, 'test_salesforce_destination')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        error_records = wiretap.error_records

        assert len(output_records) == 16
        assert len(error_records) == expected_errors

    finally:
        query = ("   select Id,"
                 "          Email "
                 "     from Contact "
                 f"   where Email like '%{test_token}%' "
                 "      and Email like '%mathematics%'"
                 " order by Id")
        result = client.query(query)
        ids = get_ids(result['records'], 'Id')
        clean_up(sdc_executor, pipeline, client, ids, hard_delete=True)
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.2.0')
def test_salesforce_destination_special_characters(sdc_builder, sdc_executor, salesforce):

    test_token = get_random_string(string.ascii_letters, 16)

    test_data = [
        {'FirstName': 'Pierre',
         'LastName': 'Fermat',
         'Email': f'{test_token}.pierre.fermat@beaumont.de.lomagne.fr',
         'Jigsaw': get_random_string(string.ascii_letters, 8),
         'LeadSource': 'Number Theory',
         'Description': '"º!""#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~€‚ƒ„…†‡ˆ‰Š‹ŒŽ‘’“”•–—˜™š›œžŸ ¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿĀāĂăĄąĆćĈĉĊċČčĎďĐđĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħĨĩĪīĬĭĮįİıĲĳĴĵĶķĸĹĺĻļĽľĿŀŁłŃńŅņŇňŉŊŋŌōŎŏŐőŒœŔŕŖŗŘřŚśŜŝŞşŠšŢţŤťŦŧŨũŪūŬŭŮůŰűŲųŴŵŶŷŸŹźŻżŽžſƀƁƂƃƄƅƆƇƈƉƊƋƌƍƎƏƐƑƒƓƔƕƖƗƘƙƚƛƜƝƞƟƠơƢƣƤƥƦƧƨƩƪƫƬƭƮƯưƱƲƳƴƵƶƷƸƹƺƻƼƽƾƿǀǁǂǃǄǅǆǇǈǉǊǋǌǍǎǏǐǑǒǓǔǕǖǗǘǙǚǛǜǝǞǟǠǡǢǣǤǥǦǧǨǩǪǫǬǭǮǯǰǱǲǳǴǵǶǷǸǹǺǻǼǽǾǿ23456789abcdefȀȁȂȃȄȅȆȇȈȉȊȋȌȍȎȏȐȑȒȓȔȕȖȗȘșȚțȜȝȞȟȠȡȢȣȤȥȦȧȨȩȪȫȬȭȮȯȰȱȲȳȴȵȶȷȸȹȺȻȼȽȾȿɀɁɂɃɄɅɆɇɈɉɊɋɌɍɎɏɐɑɒɓɔɕɖɗɘəɚɛɜɝɞɟɠɡɢɣɤɥɦɧɨɩɪɫɬɭɮɯɰɱɲɳɴɵɶɷɸɹɺɻɼɽɾɿʀʁʂʃʄʅʆʇʈʉʊʋʌʍʎʏʐʑʒʓʔʕʖʗʘʙʚʛʜʝʞʟʠʡʢʣʤʥʦʧʨʩʪʫʬʭʮʯʰʱʲʳʴʵʶʷʸʹʺʻʼʽʾʿˀˁ˂˃˄˅ˆˇˈˉˊˋˌˍˎˏːˑ˒˓˔˕˖˗˘˙˚˛˜˝˞˟ˠˡˢˣˤ˥˦˧˨˩˪˫ˬ˭ˮ˯˰˱˲˳˴˵˶˷˸˹˺˻˼˽˾˿̴̵̶̷̸̡̢̧̨̛̖̗̘̙̜̝̞̟̠̣̤̥̦̩̪̫̬̭̮̯̰̱̲̳̹̺̻̼͇͈͉͍͎̀́̂̃̄̅̆̇̈̉̊̋̌̍̎̏̐̑̒̓̔̽̾̿̀́͂̓̈́͆͊͋͌̕̚ͅ͏͓͔͕͖͙͚͐͑͒͗͛ͣͤͥͦͧͨͩͪͫͬͭͮͯ͘͜͟͢͝͞͠͡ͰͱͲͳʹ͵Ͷͷ͸͹ͺͻͼͽ;Ϳ΀΁΂΃΄΅Ά·ΈΉΊ΋Ό΍ΎΏΐΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡ΢ΣΤΥΦΧΨΩΪΫάέήίΰαβγδεζηθικλμνξοπρςστυφχψωϊϋόύώϏϐϑϒϓϔϕϖϗϘϙϚϛϜϝϞϟϠϡϢϣϤϥϦϧϨϩϪϫϬϭϮϯ0123456789abcdef⁰ⁱ⁲⁳⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾ⁿ₀₁₂₃₄₅₆₇₈₉₊₋₌₍₎₏ₐₑₒₓₔₕₖₗₘₙₚₛₜ₝₞₟₠₡₢₣₤₥₦₧₨₩₪₫€₭₮₯₰₱₲₳₴₵₶₷₸₹₺₻₼₽₾₿⃀⃁⃂⃃⃄⃅⃆⃇⃈⃉⃊⃋⃌⃍⃎⃏⃒⃓⃘⃙⃚⃐⃑⃔⃕⃖⃗⃛⃜⃝⃞⃟⃠⃡⃢⃣⃤⃥⃦⃪⃫⃨⃬⃭⃮⃯⃧⃩⃰⃱⃲⃳⃴⃵⃶⃷⃸⃹⃺⃻⃼⃽⃾⃿℀℁ℂ℃℄℅℆ℇ℈℉ℊℋℌℍℎℏℐℑℒℓ℔ℕ№℗℘ℙℚℛℜℝ℞℟℠℡™℣ℤ℥Ω℧ℨ℩KÅℬℭ℮ℯℰℱℲℳℴℵℶℷℸℹ℺℻ℼℽℾℿ⅀⅁⅂⅃⅄ⅅⅆⅇⅈⅉ⅊⅋⅌⅍ⅎ⅏⅐⅑⅒⅓⅔⅕⅖⅗⅘⅙⅚⅛⅜⅝⅞⅟ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫⅬⅭⅮⅯⅰⅱⅲⅳⅴⅵⅶⅷⅸⅹⅺⅻⅼⅽⅾⅿↀↁↂↃↄↅↆↇↈ↉↊↋↌↍↎↏←↑→↓↔↕↖↗↘↙↚↛↜↝↞↟↠↡↢↣↤↥↦↧↨↩↪↫↬↭↮↯↰↱↲↳↴↵↶↷↸↹↺↻↼↽↾↿⇀⇁⇂⇃⇄⇅⇆⇇⇈⇉⇊⇋⇌⇍⇎⇏⇐⇑⇒⇓⇔⇕⇖⇗⇘⇙⇚⇛⇜⇝⇞⇟⇠⇡⇢⇣⇤⇥⇦⇧⇨⇩⇪⇫⇬⇭⇮⇯⇰⇱⇲⇳⇴⇵⇶⇷⇸⇹⇺⇻⇼⇽⇾⇿23456789abcdef∀∁∂∃∄∅∆∇∈∉∊∋∌∍∎∏∐∑−∓∔∕∖∗∘∙√∛∜∝∞∟∠∡∢∣∤∥∦∧∨∩∪∫∬∭∮∯∰∱∲∳∴∵∶∷∸∹∺∻∼∽∾∿≀≁≂≃≄≅≆≇≈≉≊≋≌≍≎≏≐≑≒≓≔≕≖≗≘≙≚≛≜≝≞≟≠≡≢≣≤≥≦≧≨≩≪≫≬≭≮≯≰≱≲≳≴≵≶≷≸≹≺≻≼≽≾≿⊀⊁⊂⊃⊄⊅⊆⊇⊈⊉⊊⊋⊌⊍⊎⊏⊐⊑⊒⊓⊔⊕⊖⊗⊘⊙⊚⊛⊜⊝⊞⊟⊠⊡⊢⊣⊤⊥⊦⊧⊨⊩⊪⊫⊬⊭⊮⊯⊰⊱⊲⊳⊴⊵⊶⊷⊸⊹⊺⊻⊼⊽⊾⊿⋀⋁⋂⋃⋄⋅⋆⋇⋈⋉⋊⋋⋌⋍⋎⋏⋐⋑⋒⋓⋔⋕⋖⋗⋘⋙⋚⋛⋜⋝⋞⋟⋠⋡⋢⋣⋤⋥⋦⋧⋨⋩⋪⋫⋬⋭⋮⋯⋰⋱⋲⋳⋴⋵⋶⋷⋸⋹⋺⋻⋼⋽⋾⋿⌀⌁⌂⌃⌄⌅⌆⌇⌈⌉⌊⌋⌌⌍⌎⌏⌐⌑⌒⌓⌔⌕⌖⌗⌘⌙⌚⌛⌜⌝⌞⌟⌠⌡⌢⌣⌤⌥⌦⌧⌨〈〉⌫⌬⌭⌮⌯⌰⌱⌲⌳⌴⌵⌶⌷⌸⌹⌺⌻⌼⌽⌾⌿⍀⍁⍂⍃⍄⍅⍆⍇⍈⍉⍊⍋⍌⍍⍎⍏⍐⍑⍒⍓⍔⍕⍖⍗⍘⍙⍚⍛⍜⍝⍞⍟⍠⍡⍢⍣⍤⍥⍦⍧⍨⍩⍪⍫⍬⍭⍮⍯⍰⍱⍲⍳⍴⍵⍶⍷⍸⍹⍺⍻⍼⍽⍾⍿⎀⎁⎂⎃⎄⎅⎆⎇⎈⎉⎊⎋⎌⎍⎎⎏⎐⎑⎒⎓⎔⎕⎖⎗⎘⎙⎚⎛⎜⎝⎞⎟⎠⎡⎢⎣⎤⎥⎦⎧⎨⎩⎪⎫⎬⎭⎮⎯⎰⎱⎲⎳⎴⎵⎶⎷⎸⎹⎺⎻⎼⎽⎾⎿⏀⏁⏂⏃⏄⏅⏆⏇⏈⏉⏊⏋⏌⏍⏎⏏⏐⏑⏒⏓⏔⏕⏖⏗⏘⏙⏚⏛⏜⏝⏞⏟23456789abcdef␀␁␂␃␄␅␆␇␈␉␊␋␌␍␎␏␐␑␒␓␔␕␖␗␘␙␚␛␜␝␞␟␠␡␢␣␤␥␦␧␨␩␪␫␬␭␮␯␰␱␲␳␴␵␶␷␸␹␺␻␼␽␾␿⑀⑁⑂⑃⑄⑅⑆⑇⑈⑉⑊⑋⑌⑍⑎⑏⑐⑑⑒⑓⑔⑕⑖⑗⑘⑙⑚⑛⑜⑝⑞⑟①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽⑾⑿⒀⒁⒂⒃⒄⒅⒆⒇⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑⒒⒓⒔⒕⒖⒗⒘⒙⒚⒛⒜⒝⒞⒟⒠⒡⒢⒣⒤⒥⒦⒧⒨⒩⒪⒫⒬⒭⒮⒯⒰⒱⒲⒳⒴⒵ⒶⒷⒸⒹⒺⒻⒼⒽⒾⒿⓀⓁⓂⓃⓄⓅⓆⓇⓈⓉⓊⓋⓌⓍⓎⓏⓐⓑⓒⓓⓔⓕⓖⓗⓘⓙⓚⓛⓜⓝⓞⓟⓠⓡⓢⓣⓤⓥⓦⓧⓨⓩ⓪⓫⓬⓭⓮⓯⓰⓱⓲⓳⓴⓵⓶⓷⓸⓹⓺⓻⓼⓽⓾⓿─━│┃┄┅┆┇┈┉┊┋┌┍┎┏┐┑┒┓└┕┖┗┘┙┚┛├┝┞┟┠┡┢┣┤┥┦┧┨┩┪┫┬┭┮┯┰┱┲┳┴┵┶┷┸┹┺┻┼┽┾┿╀╁╂╃╄╅╆╇╈╉╊╋╌╍╎╏═║╒╓╔╕╖╗╘╙╚╛╜╝╞╟╠╡╢╣╤╥╦╧╨╩╪╫╬╭╮╯╰╱╲╳╴╵╶╷╸╹╺╻╼╽╾╿▀▁▂▃▄▅▆▇█▉▊▋▌▍▎▏▐░▒▓▔▕▖▗▘▙▚▛▜▝▞▟■□▢▣▤▥▦▧▨▩▪▫▬▭▮▯▰▱▲△▴▵▶▷▸▹►▻▼▽▾▿◀◁◂◃◄◅◆◇◈◉◊○◌◍◎●◐◑◒◓◔◕◖◗◘◙◚◛◜◝◞◟◠◡◢◣◤◥◦◧◨◩◪◫◬◭◮◯◰◱◲◳◴◵◶◷◸◹◺◻◼◽◾◿23456789abcdef☀☁☂☃☄★☆☇☈☉☊☋☌☍☎☏☐☑☒☓☔☕☖☗☘☙☚☛☜☝☞☟☠☡☢☣☤☥☦☧☨☩☪☫☬☭☮☯☰☱☲☳☴☵☶☷☸☹☺☻☼☽☾☿♀♁♂♃♄♅♆♇♈♉♊♋♌♍♎♏♐♑♒♓♔♕♖♗♘♙♚♛♜♝♞♟♠♡♢♣♤♥♦♧♨♩♪♫♬♭♮♯♰♱♲♳♴♵♶♷♸♹♺♻♼♽♾♿⚀⚁⚂⚃⚄⚅⚆⚇⚈⚉⚊⚋⚌⚍⚎⚏⚐⚑⚒⚓⚔⚕⚖⚗⚘⚙⚚⚛⚜⚝⚞⚟⚠⚡⚢⚣⚤⚥⚦⚧⚨⚩⚪⚫⚬⚭⚮⚯⚰⚱⚲⚳⚴⚵⚶⚷⚸⚹⚺⚻⚼⚽⚾⚿"'}
    ]

    input_data = [','.join(test_data[0].keys())] + [','.join(item.values()) for item in test_data]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, input_data)
    dev_raw_data_source.set_attributes(max_record_length_in_chars=1024 * 1204)

    field_mapping = [{'sdcField': '/FirstName', 'salesforceField': 'FirstName'},
                     {'sdcField': '/LastName', 'salesforceField': 'LastName'},
                     {'sdcField': '/Email', 'salesforceField': 'Email'},
                     {'sdcField': '/Jigsaw', 'salesforceField': 'Jigsaw'},
                     {'sdcField': '/LeadSource', 'salesforceField': 'LeadSource'},
                     {'sdcField': '/Description', 'salesforceField': 'Description'}]
    salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          sobject_type='Contact')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [salesforce_destination, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    read_ids = None

    client = salesforce.client
    try:
        permission_set_id = assign_hard_delete(client, 'test_salesforce_destination')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        error_records = wiretap.error_records

        assert len(output_records) == 1
        assert len(error_records) == 0

    finally:
        query = ("   select Id,"
                 "          Email "
                 "     from Contact "
                 f"   where Email like '%{test_token}%' "
                 "      and Email like '%mathematics%'"
                 " order by Id")
        result = client.query(query)
        ids = get_ids(result['records'], 'Id')
        clean_up(sdc_executor, pipeline, client, ids, hard_delete=True)
        revoke_hard_delete(client, permission_set_id)


@salesforce
@sdc_min_version('5.2.0')
def test_salesforce_destination_double_quotes(sdc_builder, sdc_executor, salesforce):

    test_token = get_random_string(string.ascii_letters, 16)

    test_data = [
        {'FirstName': 'Pierre',
         'LastName': 'Fermat',
         'Email': f'{test_token}.pierre.fermat@beaumont.de.lomagne.fr',
         'Jigsaw': '"x012345678901234567890x"",a"',
         'LeadSource': 'Number Theory',
         'Description': 'It is impossible for any number which is a power greater than the second to be written as the sum of two like powers. '
                        'I have a truly marvelous demonstration of this proposition which this margin is too narrow to contain.'}
    ]

    input_data = [','.join(test_data[0].keys())] + [','.join(item.values()) for item in test_data]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, input_data)
    dev_raw_data_source.set_attributes(max_record_length_in_chars=1024 * 1204)

    field_mapping = [{'sdcField': '/FirstName', 'salesforceField': 'FirstName'},
                     {'sdcField': '/LastName', 'salesforceField': 'LastName'},
                     {'sdcField': '/Email', 'salesforceField': 'Email'},
                     {'sdcField': '/Jigsaw', 'salesforceField': 'Jigsaw'},
                     {'sdcField': '/LeadSource', 'salesforceField': 'LeadSource'},
                     {'sdcField': '/Description', 'salesforceField': 'Description'}]
    salesforce_destination = pipeline_builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    salesforce_destination.set_attributes(default_operation='INSERT',
                                          field_mapping=field_mapping,
                                          sobject_type='Contact')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [salesforce_destination, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    read_ids = None

    client = salesforce.client
    try:
        permission_set_id = assign_hard_delete(client, 'test_salesforce_destination')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        error_records = wiretap.error_records

        assert len(output_records) == 1
        assert len(error_records) == 1

    finally:
        query = ("   select Id,"
                 "          Email "
                 "     from Contact "
                 f"   where Email like '%{test_token}%' "
                 "      and Email like '%mathematics%'"
                 " order by Id")
        result = client.query(query)
        ids = get_ids(result['records'], 'Id')
        clean_up(sdc_executor, pipeline, client, ids, hard_delete=True)
        revoke_hard_delete(client, permission_set_id)
