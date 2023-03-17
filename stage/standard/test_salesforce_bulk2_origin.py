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
import pytest
import string
import time
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string

from ..utils.utils_salesforce import (BULK_PIPELINE_TIMEOUT_SECONDS, clean_up,
                                      check_ids, get_ids, DATA_TYPES,
                                      compare_values, OBJECT_NAMES,
                                      set_up_random, create_custom_object, CUSTOM_OBJECT_NAME, delete_custom_object)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def _set_up_environment(salesforce):
    client = salesforce.client
    create_custom_object(client)

    yield

    delete_custom_object(client)

@pytest.fixture(autouse=True)
def _set_up_random(salesforce):
    set_up_random(salesforce)


@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('type_data', DATA_TYPES, ids=[datatype['type'] for datatype in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, salesforce, type_data):
    custom_field_type = type_data['type']
    test_name = 'sale_bulk2_origin_data_types_' + custom_field_type + '_' + \
                get_random_string(string.ascii_lowercase, 10)

    client = salesforce.client

    custom_object_name = CUSTOM_OBJECT_NAME + '__c'
    custom_field_name = custom_field_type + '_field__c'

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    fields = ",".join(type_data['expected_value'].keys()) \
        if type_data.get('compound_field') else custom_field_name
    query = (f"SELECT Id, {fields} FROM {custom_object_name} "
             "WHERE Id > '${OFFSET}' "
             f"AND TestName__c = '{test_name}' "
             "ORDER BY Id")
    origin.set_attributes(soql_query=query,
                          incremental_mode=False)

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    record_ids = []

    try:
        logger.info('Adding two records into Salesforce ...')
        record = {
            'Name': 1,
            'TestName__c': test_name,
        }
        # Not every data type wants data - e.g. auto number field
        if type_data.get('data_to_insert'):
            if type_data.get('compound_field'):
                record.update(type_data['data_to_insert'])
            else:
                record[fields] = type_data['data_to_insert']

        object_type = getattr(client, custom_object_name)

        result = object_type.create(record)
        record_ids.append({'Id': result['id']})

        # And a record without a value for the field
        result = object_type.create({
            'Name': 2,
            'TestName__c': test_name
        })
        record_ids.append({'Id': result['id']})

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2, timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 2
        record = wiretap.output_records[0]
        null_record = wiretap.output_records[1]

        if type_data.get('compound_field'):
            for key in type_data['expected_value'].keys():
                assert type_data['expected_type'] == record.field[key].type
            for key in type_data['expected_value'].keys():
                assert type_data['expected_value'][key] == record.field[key]._data['value']

            for key in type_data['null_value'].keys():
                assert type_data['expected_type'] == null_record.field[key].type
            for key in type_data['null_value'].keys():
                assert type_data['null_value'][key] == null_record.field[key]._data['value']
        else:
            assert type_data['expected_type'] == record.field[fields].type
            assert compare_values(type_data['expected_value'],
                                  record.field[fields]._data['value'],
                                  type_data['type'])

            assert type_data['expected_type'] == null_record.field[fields].type
            assert compare_values(type_data.get('null_value'),
                                  null_record.field[fields]._data['value'],
                                  type_data['type'])

    finally:
        clean_up(sdc_executor, pipeline, client, record_ids, hard_delete=True, object_name=custom_object_name)


@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('test_name,field_name', OBJECT_NAMES, ids=[i[0] for i in OBJECT_NAMES])
def test_object_names(sdc_builder, sdc_executor, salesforce, test_name, field_name):
    run_name = 'sale_bulk2_origin_object_names_' + test_name + '_' + get_random_string(string.ascii_lowercase, 10)
    client = salesforce.client

    custom_object_name = CUSTOM_OBJECT_NAME + '__c'
    custom_field_name = '{}__c'.format(field_name)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    query = (f"SELECT Id, {custom_field_name} FROM {custom_object_name} "
             "WHERE Id > '${OFFSET}' "
             f"AND TestName__c = '{run_name}' "
             "ORDER BY Id")
    origin.set_attributes(soql_query=query,
                          incremental_mode=False)

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    record_id = None

    try:
        logger.info('Adding a TestObject into Salesforce ...')

        object_type = getattr(client, custom_object_name)

        result = object_type.create({
            'Name': 1,
            'TestName__c': run_name,
            f'{field_name}__c': 1
        })
        record_id = {'Id': result['id']}

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              1,
                                              timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
        # We want to run for a few seconds to see if any errors show up (like that did in previous versions)
        time.sleep(10)
        sdc_executor.stop_pipeline(pipeline)

        # There should be no errors reported
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.SalesforceBulkAPI20_01.errorRecords.counter').count == 0
        assert history.latest.metrics.counter('stage.SalesforceBulkAPI20_01.stageErrors.counter').count == 0

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field[custom_field_name] == 1

    finally:
        clean_up(sdc_executor, pipeline, client, [record_id], hard_delete=True, object_name=custom_object_name)


@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('number_of_threads', [1, 10])
def test_multiple_batches(sdc_builder, sdc_executor, salesforce, number_of_threads):
    # Cap at 1000 records so we stay within Salesforce Developer Edition data limits
    max_batch_size = 20
    batches = 50

    test_name = 'sale_bulk2_origin_multiple_batches_' + str(number_of_threads) + '_' + \
                get_random_string(string.ascii_lowercase, 10)

    client = salesforce.client

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    query = (f"SELECT Id, FirstName FROM Contact "
             "WHERE Id > '${OFFSET}' "
             f"AND LastName = '{test_name}' "
             "ORDER BY Id")
    origin.set_attributes(soql_query=query,
                          incremental_mode=False,
                          max_batch_size_in_records=max_batch_size,
                          query_interval='${1 * SECONDS}',
                          number_of_threads=number_of_threads)
    origin >= builder.add_stage("Pipeline Finisher Executor")

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    record_ids = []

    try:
        logger.info('Inserting data into Contacts...')
        records = [{'FirstName': str(n), 'LastName': test_name} for n in range(1, max_batch_size * batches + 1)]
        record_ids = check_ids(get_ids(client.bulk.Contact.insert(records), 'id'))

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == max_batch_size * batches

        # Verify each record
        def sort_func(entry):
            return int(entry.field['FirstName'].value)

        records.sort(key=sort_func)

        expected_number = 1
        for record in records:
            assert int(record.field['FirstName'].value) == expected_number
            expected_number += 1

    finally:
        clean_up(sdc_executor, pipeline, client, record_ids, hard_delete=True)


@salesforce
@sdc_min_version('5.0.0')
def test_dataflow_events(sdc_builder, sdc_executor, salesforce):
    test_name = 'sale_bulk2_origin_dataflow_events_' + get_random_string(string.ascii_lowercase, 10)

    client = salesforce.client

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    query = (f"SELECT Id, FirstName FROM Contact "
             "WHERE Id > '${OFFSET}' "
             f"AND LastName = '{test_name}' "
             "ORDER BY Id")
    origin.set_attributes(soql_query=query,
                          query_interval='${1 * SECONDS}',
                          incremental_mode=True)

    trash = builder.add_stage('Trash')

    origin >> trash

    wiretap = builder.add_wiretap()
    origin >= wiretap.destination

    pipeline = builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    record_ids = []

    try:
        logger.info('Adding a record into Salesforce ...')
        result = client.Contact.create({
            'FirstName': '1',
            'LastName': test_name
        })
        record_ids.append({'Id': result['id']})


        # Start the pipeline
        status = sdc_executor.start_pipeline(pipeline)

        # Read one data record, generate 1 event, wiretap adds an extra record = 3 records
        status.wait_for_pipeline_output_records_count(3)

        # Event should be no more data
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values['sdc.event.type'] == 'no-more-data'

        wiretap.reset()

        # Second iteration - insert one new row
        logger.info('Inserting row into Contacts')
        result = client.Contact.create({
            'FirstName': '2',
            'LastName': test_name
        })
        record_ids.append({'Id': result['id']})

        # 1 data record, 1 event, 1 wiretap record more
        status.wait_for_pipeline_output_records_count(6)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values['sdc.event.type'] == 'no-more-data'

        # Now let's stop the pipeline and start it again
        sdc_executor.stop_pipeline(pipeline)

        # Portable truncate
        wiretap.reset()

        # Start the pipeline and wait for it to emit an event since there is no more
        # data to read
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values['sdc.event.type'] == 'no-more-data'
    finally:
        clean_up(sdc_executor, pipeline, client, record_ids, hard_delete=True)


@salesforce
@sdc_min_version('5.0.0')
def test_data_format(sdc_builder, sdc_executor, salesforce):
    pytest.skip("Salesforce Bulk 2.0 Origin doesn't deal with data formats")


@salesforce
@sdc_min_version('5.0.0')
def test_resume_offset(sdc_builder, sdc_executor, salesforce):
    iterations = 3
    records_per_iteration = 10
    test_name = 'sale_bulk2_origin_resume_offset_' + get_random_string(string.ascii_lowercase, 10)

    client = salesforce.client

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Salesforce Bulk API 2.0', type='origin')
    query = (f"SELECT Id, FirstName FROM Contact "
             "WHERE Id > '${OFFSET}' "
             f"AND LastName = '{test_name}' "
             "ORDER BY Id")
    origin.set_attributes(soql_query=query,
                          query_interval='${1 * SECONDS}',
                          incremental_mode=True)

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    record_ids = []

    try:
        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()

            logger.info('Inserting data into Contacts')
            records = [{'FirstName': str(n), 'LastName': test_name} for n in range(iteration * records_per_iteration + 1, iteration * records_per_iteration + 1 + records_per_iteration)]
            record_ids += check_ids(get_ids(client.bulk.Contact.insert(records), 'id'))


            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            expected_number = iteration * records_per_iteration + 1
            for record in records:
                assert int(record.field['FirstName'].value) == expected_number
                expected_number += 1

    finally:
        clean_up(sdc_executor, pipeline, client, record_ids, hard_delete=True)