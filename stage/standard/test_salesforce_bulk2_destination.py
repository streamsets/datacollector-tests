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
import json

import pytest
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string

from ..utils.utils_salesforce import (BULK_PIPELINE_TIMEOUT_SECONDS, clean_up, get_ids, OBJECT_NAMES,
                                      compare_values, set_up_random, assign_hard_delete, revoke_hard_delete,
                                      create_custom_object, delete_custom_object, CUSTOM_OBJECT_NAME)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def _set_up_environment(salesforce):
    client = salesforce.client
    create_custom_object(client)

    # Having each test create and delete their own permissions file has various concurrency problems which make several
    # tests fail each execution. Additionally, if the cleanup is not properly done the account fills up with leftovers
    # and tests also start failing. Creating a single permissions file for the whole test file should alleviate both
    # problems.
    permission_set_id = assign_hard_delete(client, 'test_standard_salesforce_bulk2_destination')

    yield

    delete_custom_object(client)

    # Delete the hard delete permission file to keep the test account clean
    revoke_hard_delete(client, permission_set_id)


@pytest.fixture(autouse=True)
def _set_up_random(salesforce):
    set_up_random(salesforce)


LONG_TEXT_MIN_LENGTH = 256

DATA_TYPES = [
    # Boolean
    ('true', 'BOOLEAN', 'Text', 'Text_field', 'true'),
    ('true', 'BOOLEAN', 'EncryptedText', 'EncryptedText_field', 'XXXX'),
    ('true', 'BOOLEAN', 'TextArea', 'TextArea_field', 'true'),
    ('true', 'BOOLEAN', 'LongTextArea', 'LongTextArea_field', 'true'),
    ('true', 'BOOLEAN', 'Html', 'Html_field', 'true'),
    ('true', 'BOOLEAN', 'Checkbox', 'Checkbox_field', True),
    # # Byte
    ('65', 'BYTE', 'Text', 'Text_field', '65'),
    # # Char
    ('a', 'CHAR', 'Text', 'Text_field', 'a'),
    ('a', 'CHAR', 'EncryptedText', 'EncryptedText_field', 'X'),
    ('a', 'CHAR', 'TextArea', 'TextArea_field', 'a'),
    ('a', 'CHAR', 'LongTextArea', 'LongTextArea_field', 'a'),
    ('a', 'CHAR', 'Html', 'Html_field', 'a'),
    # # Short
    (120, 'SHORT', 'Number', 'Number_no_decimals_field', 120),
    (120, 'SHORT', 'Currency', 'Currency_field', 120.00),
    (120, 'SHORT', 'Number', 'Number_field', 120.00),
    (120, 'SHORT', 'Percent', 'Percent_field', 120.00),
    (120, 'SHORT', 'Text', 'Text_field', '120'),
    (120, 'SHORT', 'EncryptedText', 'EncryptedText_field', 'XXX'),
    (120, 'SHORT', 'TextArea', 'TextArea_field', '120'),
    (120, 'SHORT', 'LongTextArea', 'LongTextArea_field', '120'),
    (120, 'SHORT', 'Html', 'Html_field', '120'),
    # # Integer
    (120, 'INTEGER', 'Number', 'Number_no_decimals_field', 120),
    (120, 'INTEGER', 'Currency', 'Currency_field', 120.00),
    (120, 'INTEGER', 'Number', 'Number_field', 120.00),
    (120, 'INTEGER', 'Percent', 'Percent_field', 120.00),
    (120, 'INTEGER', 'Text', 'Text_field', '120'),
    (120, 'INTEGER', 'EncryptedText', 'EncryptedText_field', 'XXX'),
    (120, 'INTEGER', 'TextArea', 'TextArea_field', '120'),
    (120, 'INTEGER', 'LongTextArea', 'LongTextArea_field', '120'),
    (120, 'INTEGER', 'Html', 'Html_field', '120'),
    # # Long
    (120, 'LONG', 'Number', 'Number_no_decimals_field', 120),
    (120, 'LONG', 'Currency', 'Currency_field', 120.00),
    (120, 'LONG', 'Number', 'Number_field', 120.00),
    (120, 'LONG', 'Percent', 'Percent_field', 120.00),
    (120, 'LONG', 'Text', 'Text_field', '120'),
    (120, 'LONG', 'EncryptedText', 'EncryptedText_field', 'XXX'),
    (120, 'LONG', 'TextArea', 'TextArea_field', '120'),
    (120, 'LONG', 'LongTextArea', 'LongTextArea_field', '120'),
    (120, 'LONG', 'Html', 'Html_field', '120'),
    # # Float
    (120.0, 'FLOAT', 'Currency', 'Currency_field', 120.00),
    (120.0, 'FLOAT', 'Number', 'Number_field', 120.00),
    (120.0, 'FLOAT', 'Percent', 'Percent_field', 120.00),
    (120.0, 'FLOAT', 'Text', 'Text_field', '120.0'),
    (120.0, 'FLOAT', 'EncryptedText', 'EncryptedText_field', 'XXXXX'),
    (120.0, 'FLOAT', 'TextArea', 'TextArea_field', '120.0'),
    (120.0, 'FLOAT', 'LongTextArea', 'LongTextArea_field', '120.0'),
    (120.0, 'FLOAT', 'Html', 'Html_field', '120.0'),
    # # Double
    (120.0, 'DOUBLE', 'Currency', 'Currency_field', 120.00),
    (120.0, 'DOUBLE', 'Number', 'Number_field', 120.00),
    (120.0, 'DOUBLE', 'Percent', 'Percent_field', 120.00),
    (120.0, 'DOUBLE', 'Text', 'Text_field', '120.0'),
    (120.0, 'DOUBLE', 'EncryptedText', 'EncryptedText_field', 'XXXXX'),
    (120.0, 'DOUBLE', 'TextArea', 'TextArea_field', '120.0'),
    (120.0, 'DOUBLE', 'LongTextArea', 'LongTextArea_field', '120.0'),
    (120.0, 'DOUBLE', 'Html', 'Html_field', '120.0'),
    # # # Decimal
    (120, 'DECIMAL', 'Number', 'Number_no_decimals_field', 120),
    (120, 'DECIMAL', 'Currency', 'Currency_field', 120.00),
    (120, 'DECIMAL', 'Number', 'Number_field', 120.00),
    (120, 'DECIMAL', 'Percent', 'Percent_field', 120.00),
    (120, 'DECIMAL', 'Text', 'Text_field', '120.00'),
    (120, 'DECIMAL', 'EncryptedText', 'EncryptedText_field', 'XXXXXX'),
    (120, 'DECIMAL', 'TextArea', 'TextArea_field', '120.00'),
    (120, 'DECIMAL', 'LongTextArea', 'LongTextArea_field', '120.00'),
    (120, 'DECIMAL', 'Html', 'Html_field', '120.00'),
    # # Date
    ('2020-01-01Z', 'DATE', 'Date', 'Date_field', '2020-01-01'),
    ('2020-01-01Z', 'DATE', 'Text', 'Text_field', '2020-01-01'),
    ('2020-01-01Z', 'DATE', 'EncryptedText', 'EncryptedText_field', 'XXXXXXXXXX'),
    ('2020-01-01Z', 'DATE', 'TextArea', 'TextArea_field', '2020-01-01'),
    ('2020-01-01Z', 'DATE', 'LongTextArea', 'LongTextArea_field', '2020-01-01'),
    ('2020-01-01Z', 'DATE', 'Html', 'Html_field', '2020-01-01'),
    # # Time - Need to specify the timezone, otherwise Field Type Converter will create times in the local zone
    ('10:00:00Z', 'TIME', 'Time', 'Time_field', '10:00:00.000Z'),
    ('10:00:00Z', 'TIME', 'Text', 'Text_field', '10:00:00'),
    ('10:00:00Z', 'TIME', 'EncryptedText', 'EncryptedText_field', 'XXXXXXXX'),
    ('10:00:00Z', 'TIME', 'TextArea', 'TextArea_field', '10:00:00'),
    ('10:00:00Z', 'TIME', 'LongTextArea', 'LongTextArea_field', '10:00:00'),
    ('10:00:00Z', 'TIME', 'Html', 'Html_field', '10:00:00'),
    # # DateTime
    ('2020-01-01 10:00:00Z', 'DATETIME', 'Date', 'Date_field', '2020-01-01'),
    ('2020-01-01 10:00:00Z', 'DATETIME', 'DateTime', 'DateTime_field', '2020-01-01T10:00:00.000+0000'),
    ('2020-01-01 10:00:00Z', 'DATETIME', 'Text', 'Text_field', '2020-01-01T10:00:00.000Z'),
    ('2020-01-01 10:00:00Z', 'DATETIME', 'EncryptedText', 'EncryptedText_field', 'XXXXXXXXXXXXXXXXXXXXXXXX'),
    ('2020-01-01 10:00:00Z', 'DATETIME', 'TextArea', 'TextArea_field', '2020-01-01T10:00:00.000Z'),
    ('2020-01-01 10:00:00Z', 'DATETIME', 'LongTextArea', 'LongTextArea_field', '2020-01-01T10:00:00.000Z'),
    ('2020-01-01 10:00:00Z', 'DATETIME', 'Html', 'Html_field', '2020-01-01T10:00:00.000Z'),
    # # String
    ('120', 'STRING', 'Number', 'Number_no_decimals_field', 120),
    ('120', 'STRING', 'Currency', 'Currency_field', 120.00),
    ('120', 'STRING', 'Number', 'Number_field', 120.00),
    ('120', 'STRING', 'Percent', 'Percent_field', 120.00),
    ('120', 'STRING', 'Text', 'Text_field', '120'),
    ('120', 'STRING', 'EncryptedText', 'EncryptedText_field', 'XXX'),
    ('120', 'STRING', 'TextArea', 'TextArea_field', '120'),
    ('120', 'STRING', 'LongTextArea', 'LongTextArea_field', '120'),
    ('120', 'STRING', 'Html', 'Html_field', '120'),
    ('2003-04-12T04:05:06Z', 'STRING', 'DateTime', 'DateTime_field', '2003-04-12T04:05:06.000+0000'),
    ('2020-01-01', 'STRING', 'Date', 'Date_field', '2020-01-01'),
    ('10:00:00', 'STRING', 'Time', 'Time_field', '10:00:00.000Z'),
    ('true', 'STRING', 'Checkbox', 'Checkbox_field', True),
    ('a@b.com', 'STRING', 'Email', 'Email_field', 'a@b.com'),
    ('111-222-3333', 'STRING', 'Phone', 'Phone_field', '111-222-3333'),
    ('green', 'STRING', 'Picklist', 'Picklist_field', 'green'),
    ('green;blue', 'STRING', 'MultiselectPicklist', 'MultiselectPicklist_field', 'green;blue'),
    ('https://streamsets.com/', 'STRING', 'Url', 'Url_field', 'https://streamsets.com/'),
]
@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('input,converter_type,database_type,field_name,expected', DATA_TYPES, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, salesforce, input, converter_type, database_type, field_name, expected):
    test_name = 'sale_bulk2_origin_data_types_' + field_name + '_' + \
                get_random_string(string.ascii_lowercase, 10)

    client = salesforce.client

    custom_object_name = CUSTOM_OBJECT_NAME + '__c'
    custom_field_name = field_name + '__c'

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({custom_field_name: input })

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        'fieldToSet': '/TestName__c',
        'expression': test_name
    }]

    other_date_format = None
    zoned_date_time_format = None

    if converter_type == 'DATE':
        other_date_format = 'yyyy-MM-ddX'
    elif converter_type == 'DATETIME':
        other_date_format = 'yyyy-MM-dd HH:mm:ssX'
    elif converter_type == 'TIME':
        other_date_format = 'HH:mm:ssX'
    elif converter_type == 'ZONED_DATETIME':
        zoned_date_time_format = 'ISO_OFFSET_DATE_TIME'

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': [f'/{custom_field_name}'],
        'targetType': converter_type,
        'dataLocale': 'en.US',
        'dateFormat': 'OTHER',
        'otherDateFormat': other_date_format,
        'zonedDateTimeFormat': zoned_date_time_format,
        'scale': 2
    }]

    target = builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    target.sobject_type = custom_object_name
    target.field_mapping = []
    target.on_record_error = 'STOP_PIPELINE'

    origin >> expression >> converter >> target

    pipeline = builder.build().configure_for_environment(salesforce)

    sdc_executor.add_pipeline(pipeline)

    read_ids = []
    permission_set_id = None
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)

        query_str = f"SELECT Id, {custom_field_name} FROM {custom_object_name} WHERE TestName__c = '{test_name}'"
        result = client.query(query_str)
        logger.info(result['records'])
        read_ids = get_ids(result['records'], 'Id')

        assert len(result['records']) == 1
        assert compare_values(expected, result['records'][0][custom_field_name], database_type)
    finally:
        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True, object_name=custom_object_name)


@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('test_name,field_name', OBJECT_NAMES, ids=[i[0] for i in OBJECT_NAMES])
def test_object_names(sdc_builder, sdc_executor, salesforce, test_name, field_name):
    run_name = 'sale_bulk2_dest_object_names_' + test_name + '_' + get_random_string(string.ascii_lowercase, 10)
    client = salesforce.client

    custom_field_name = '{}__c'.format(field_name)
    custom_object_name = CUSTOM_OBJECT_NAME + '__c'

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "{custom_field_name}" : 1 }}'
    source.stop_after_first_batch = True

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        'fieldToSet': '/TestName__c',
        'expression': run_name
    }]

    target = builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    target.sobject_type = custom_object_name
    target.field_mapping = []
    target.on_record_error = 'STOP_PIPELINE'

    source >> expression >> target
    pipeline = builder.build().configure_for_environment(salesforce)

    read_ids = []

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)

        # Verify that the data were indeed inserted
        result = client.query(f"SELECT Id, {custom_field_name} FROM {custom_object_name} WHERE TestName__c = '{run_name}'")
        read_ids = get_ids(result['records'], 'Id')

        assert len(result['records']) == 1
        assert result['records'][0][f'{custom_field_name}'] == 1
    finally:
        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True, object_name=custom_object_name)


@salesforce
@sdc_min_version('5.0.0')
def test_multiple_batches(sdc_builder, sdc_executor, salesforce):
    test_name = 'sale_bulk2_dest_multiple_batches_' + get_random_string(string.ascii_lowercase, 10)
    # Cap at 1000 records so we stay within Salesforce Developer Edition data limits
    batch_size = 20
    batches = 10

    client = salesforce.client

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.fields_to_generate = [{"type": "LONG_SEQUENCE",
                                  "field": "FirstName"}]

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        'fieldToSet': '/LastName',
        'expression': test_name
    }]

    target = builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    target.sobject_type = 'Contact'
    target.field_mapping = []
    target.on_record_error = 'STOP_PIPELINE'

    origin >> expression >> target
    pipeline = builder.build().configure_for_environment(salesforce)

    sdc_executor.add_pipeline(pipeline)

    read_ids = []

    try:
        # Wiretap generates one extra record per batch
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count((batches + 1) * batch_size,
                                                                                     timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)
        sdc_executor.stop_pipeline(pipeline)

        # Now the pipeline will write some amount of records that will be larger, so we get precise count from metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Detected {records} output records")
        # Sanity check
        assert records >= batch_size * batches

        result = client.query(f"SELECT Id, FirstName FROM Contact WHERE LastName = '{test_name}'")
        read_ids = get_ids(result['records'], 'Id')
        data = sorted([record["FirstName"] for record in result['records']])

        expected = sorted(str(i) for i in range(0, records))
        assert data == expected
    finally:
        clean_up(sdc_executor, pipeline, client, read_ids, hard_delete=True)


@salesforce
@sdc_min_version('5.0.0')
def test_dataflow_events(sdc_builder, sdc_executor, salesforce):
    pytest.skip("No events supported in Salesforce Bulk API 2.0 destination at this time.")


@salesforce
@sdc_min_version('5.0.0')
def test_data_format(sdc_builder, sdc_executor, salesforce):
    pytest.skip("Salesforce Bulk API 2.0 doesn't deal with data formats")


@salesforce
@sdc_min_version('5.0.0')
def test_push_pull(sdc_builder, sdc_executor, salesforce):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is part of "
                "test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
