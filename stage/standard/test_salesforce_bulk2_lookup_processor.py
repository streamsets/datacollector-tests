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

import logging
import string

import pytest
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string

from ..utils.utils_salesforce import (BULK_PIPELINE_TIMEOUT_SECONDS, clean_up,
                                      check_ids, get_ids, DATA_TYPES, compare_values, set_up_random,
                                      create_custom_object, delete_custom_object, CUSTOM_OBJECT_NAME)

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
    test_name = 'sale_bulk2_proc_data_types_' + custom_field_type + '_' + \
                get_random_string(string.ascii_lowercase, 10)

    client = salesforce.client

    custom_object_name = CUSTOM_OBJECT_NAME + '__c'
    custom_field_name = custom_field_type + '_field__c'

    fields = ",".join(type_data['expected_value'].keys()) \
        if type_data.get('compound_field') else custom_field_name

    record_ids = []

    try:
        logger.info('Adding two records into Salesforce ...')
        record = {
            'Name': '1',
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
            'Name': '2',
            'TestName__c': test_name
        })
        record_ids.append({'Id': result['id']})

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.data_format = 'JSON'
        origin.raw_data = '{"id": 1}\n{"id": 2}'
        origin.stop_after_first_batch = True

        lookup = builder.add_stage('Salesforce Bulk API 2.0 Lookup')
        lookup.soql_query = (f"SELECT {fields} FROM {custom_object_name} "
                             "WHERE Name = '${record:value(\"/id\")}'"
                             f" AND TestName__c = '{test_name}' ")
        lookup.field_mappings = [dict(dataType='USE_SALESFORCE_TYPE',
                                      salesforceField=f'{fields}',
                                      sdcField=f'/{fields}')]

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(salesforce)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)

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
def test_object_names(sdc_builder, sdc_executor, salesforce):
    pytest.skip("The Salesforce Bulk 2.0 Lookup Processor doesn't generate queries - it only takes user "
                "input, thus user is responsible to properly escape or enclose names and therefore there "
                "is not much for us to test here.")


@salesforce
@sdc_min_version('5.0.0')
def test_multiple_batches(sdc_builder, sdc_executor, salesforce):
    batch_size = 40
    batches = 5
    test_name = 'sale_bulk2_proc_multiple_batches_' + get_random_string(string.ascii_lowercase, 10)

    client = salesforce.client

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.records_to_be_generated = batch_size * batches
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [ {
        'fieldToSet': '/lookup',
        'expression': '${record:value("/seq") % 3 + 1}'
      } ]

    lookup = builder.add_stage('Salesforce Bulk API 2.0 Lookup')
    lookup.soql_query = (f"SELECT LastName FROM Contact "
                         "WHERE FirstName = '${record:value(\"/lookup\")}'"
                         f" AND Department = '{test_name}' ")
    lookup.field_mappings = [dict(dataType='USE_SALESFORCE_TYPE',
                                  salesforceField=f'LastName',
                                  sdcField=f'/Last Name')]

    wiretap = builder.add_wiretap()
    origin >> expression >> lookup >> wiretap.destination

    pipeline = builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    record_ids = []
    try:
        logger.info(f'Inserting data into Contacts ...')
        records = [{'FirstName': str(n), 'LastName': str(n * 10), 'Department': test_name} for n in range(1, 4)]
        record_ids = check_ids(get_ids(client.bulk.Contact.insert(records), 'id'))

        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=600)

        # Wiretap generates one extra record per batch meaning the pipeline will write an amount of records that will
        # be larger, so we get the precise count from metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        record_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Detected {record_count} output records")
        # Sanity check
        assert record_count >= batch_size * batches

        records = wiretap.output_records
        assert len(records) == record_count

        # Verify each record
        def sortFunc(entry):
            return entry.field['seq'].value

        records.sort(key=sortFunc)

        expected_number = 0
        for record in records:
            assert record.field['seq'] == expected_number
            assert record.field['lookup'] == expected_number % 3 + 1
            assert record.field['Last Name'] == str((expected_number % 3 + 1) * 10)
            expected_number += 1

    finally:
        clean_up(sdc_executor, pipeline, client, record_ids, hard_delete=True)


@salesforce
@sdc_min_version('5.0.0')
def test_data_format(sdc_builder, sdc_executor, salesforce):
    pytest.skip("Salesforce Bulk 2.0 Lookup Processor doesn't deal with data formats")


@salesforce
@sdc_min_version('5.0.0')
def test_dataflow_events(sdc_builder, sdc_executor, salesforce):
    pytest.skip("Salesforce Bulk 2.0 Lookup processor does not support events today")
