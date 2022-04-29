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

import datetime
import logging
import string
import json

import pytest
from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.utils import get_random_string

from ..utils.utils_salesforce import (BULK_PIPELINE_TIMEOUT_SECONDS, clean_up,
                                      get_ids, STANDARD_FIELDS, set_field_permissions,
                                      OBJECT_NAMES, compare_values, set_up_random, assign_hard_delete,
                                      revoke_hard_delete, add_custom_field_to_contact, delete_custom_field_from_contact)

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def _set_up_random(salesforce):
    set_up_random(salesforce)


LONG_TEXT_MIN_LENGTH = 256

# (input,converter_type,database_type,expected)
DATA_TYPES = [
    # Boolean
    ('true', 'BOOLEAN', {'type': 'Text', 'length': 4}, 'true'),
    ('true', 'BOOLEAN', {'type': 'EncryptedText', 'length': 4, 'maskChar': 'X', 'maskType': 'all'}, 'XXXX'),
    ('true', 'BOOLEAN', {'type': 'TextArea'}, 'true'),
    ('true', 'BOOLEAN', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, 'true'),
    ('true', 'BOOLEAN', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, 'true'),
    ('true', 'BOOLEAN', {'type': 'Checkbox', 'defaultValue': False}, True),
    # Byte
    ('65', 'BYTE', {'type': 'Text', 'length': 2}, '65'),
    # Char
    ('a', 'CHAR', {'type': 'Text', 'length': 1}, 'a'),
    ('a', 'CHAR', {'type': 'EncryptedText', 'length': 1, 'maskChar': 'X', 'maskType': 'all'}, 'X'),
    ('a', 'CHAR', {'type': 'TextArea'}, 'a'),
    ('a', 'CHAR', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, 'a'),
    ('a', 'CHAR', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, 'a'),
    # Short
    (120, 'SHORT', {'type': 'Number', 'precision': 5, 'scale': 0}, 120),
    (120, 'SHORT', {'type': 'Currency', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'SHORT', {'type': 'Number', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'SHORT', {'type': 'Percent', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'SHORT', {'type': 'Text', 'length': 3}, '120'),
    (120, 'SHORT', {'type': 'EncryptedText', 'length': 3, 'maskChar': 'X', 'maskType': 'all'}, 'XXX'),
    (120, 'SHORT', {'type': 'TextArea'}, '120'),
    (120, 'SHORT', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120'),
    (120, 'SHORT', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120'),
    # Integer
    (120, 'INTEGER', {'type': 'Number', 'precision': 5, 'scale': 0}, 120),
    (120, 'INTEGER', {'type': 'Currency', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'INTEGER', {'type': 'Number', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'INTEGER', {'type': 'Percent', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'INTEGER', {'type': 'Text', 'length': 3}, '120'),
    (120, 'INTEGER', {'type': 'EncryptedText', 'length': 3, 'maskChar': 'X', 'maskType': 'all'}, 'XXX'),
    (120, 'INTEGER', {'type': 'TextArea'}, '120'),
    (120, 'INTEGER', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120'),
    (120, 'INTEGER', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120'),
    # Long
    (120, 'LONG', {'type': 'Number', 'precision': 5, 'scale': 0}, 120),
    (120, 'LONG', {'type': 'Currency', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'LONG', {'type': 'Number', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'LONG', {'type': 'Percent', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'LONG', {'type': 'Text', 'length': 3}, '120'),
    (120, 'LONG', {'type': 'EncryptedText', 'length': 3, 'maskChar': 'X', 'maskType': 'all'}, 'XXX'),
    (120, 'LONG', {'type': 'TextArea'}, '120'),
    (120, 'LONG', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120'),
    (120, 'LONG', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120'),
    # Float
    (120.0, 'FLOAT', {'type': 'Currency', 'precision': 5, 'scale': 2}, 120.00),
    (120.0, 'FLOAT', {'type': 'Number', 'precision': 5, 'scale': 2}, 120.00),
    (120.0, 'FLOAT', {'type': 'Percent', 'precision': 5, 'scale': 2}, 120.00),
    (120.0, 'FLOAT', {'type': 'Text', 'length': 5}, '120.0'),
    (120.0, 'FLOAT', {'type': 'EncryptedText', 'length': 5, 'maskChar': 'X', 'maskType': 'all'}, 'XXXXX'),
    (120.0, 'FLOAT', {'type': 'TextArea'}, '120.0'),
    (120.0, 'FLOAT', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120.0'),
    (120.0, 'FLOAT', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120.0'),
    # Double
    (120.0, 'DOUBLE', {'type': 'Currency', 'precision': 5, 'scale': 2}, 120.00),
    (120.0, 'DOUBLE', {'type': 'Number', 'precision': 5, 'scale': 2}, 120.00),
    (120.0, 'DOUBLE', {'type': 'Percent', 'precision': 5, 'scale': 2}, 120.00),
    (120.0, 'DOUBLE', {'type': 'Text', 'length': 5}, '120.0'),
    (120.0, 'DOUBLE', {'type': 'EncryptedText', 'length': 5, 'maskChar': 'X', 'maskType': 'all'}, 'XXXXX'),
    (120.0, 'DOUBLE', {'type': 'TextArea'}, '120.0'),
    (120.0, 'DOUBLE', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120.0'),
    (120.0, 'DOUBLE', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120.0'),
    # # Decimal
    (120, 'DECIMAL', {'type': 'Number', 'precision': 5, 'scale': 0}, 120),
    (120, 'DECIMAL', {'type': 'Currency', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'DECIMAL', {'type': 'Number', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'DECIMAL', {'type': 'Percent', 'precision': 5, 'scale': 2}, 120.00),
    (120, 'DECIMAL', {'type': 'Text', 'length': 6}, '120.00'),
    (120, 'DECIMAL', {'type': 'EncryptedText', 'length': 6, 'maskChar': 'X', 'maskType': 'all'}, 'XXXXXX'),
    (120, 'DECIMAL', {'type': 'TextArea'}, '120.00'),
    (120, 'DECIMAL', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120.00'),
    (120, 'DECIMAL', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120.00'),
    # Date
    ('2020-01-01Z', 'DATE', {'type': 'Date'}, '2020-01-01'),
    ('2020-01-01Z', 'DATE', {'type': 'Text', 'length': 30}, '2020-01-01'),
    ('2020-01-01Z', 'DATE', {'type': 'EncryptedText', 'length': 30, 'maskChar': 'X', 'maskType': 'all'}, 'XXXXXXXXXX'),
    ('2020-01-01Z', 'DATE', {'type': 'TextArea'}, '2020-01-01'),
    ('2020-01-01Z', 'DATE', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '2020-01-01'),
    ('2020-01-01Z', 'DATE', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '2020-01-01'),
    # Time - Need to specify the timezone, otherwise Field Type Converter will create times in the local zone
    ('10:00:00Z', 'TIME', {'type': 'Time'}, '10:00:00.000Z'),
    ('10:00:00Z', 'TIME', {'type': 'Text', 'length': 30}, '10:00:00'),
    ('10:00:00Z', 'TIME', {'type': 'EncryptedText', 'length': 30, 'maskChar': 'X', 'maskType': 'all'}, 'XXXXXXXX'),
    ('10:00:00Z', 'TIME', {'type': 'TextArea'}, '10:00:00'),
    ('10:00:00Z', 'TIME', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '10:00:00'),
    ('10:00:00Z', 'TIME', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '10:00:00'),
    # DateTime
    ('2020-01-01 10:00:00Z', 'DATETIME', {'type': 'Date'}, '2020-01-01'),
    ('2020-01-01 10:00:00Z', 'DATETIME', {'type': 'DateTime'}, '2020-01-01T10:00:00.000+0000'),
    ('2020-01-01 10:00:00Z', 'DATETIME', {'type': 'Text', 'length': 30}, '2020-01-01T10:00:00.000Z'),
    ('2020-01-01 10:00:00Z', 'DATETIME', {'type': 'EncryptedText', 'length': 30, 'maskChar': 'X', 'maskType': 'all'}, 'XXXXXXXXXXXXXXXXXXXXXXXX'),
    ('2020-01-01 10:00:00Z', 'DATETIME', {'type': 'TextArea'}, '2020-01-01T10:00:00.000Z'),
    ('2020-01-01 10:00:00Z', 'DATETIME', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '2020-01-01T10:00:00.000Z'),
    ('2020-01-01 10:00:00Z', 'DATETIME', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '2020-01-01T10:00:00.000Z'),
    # String
    ('120', 'STRING', {'type': 'Number', 'precision': 5, 'scale': 0}, 120),
    ('120', 'STRING', {'type': 'Currency', 'precision': 5, 'scale': 2}, 120.00),
    ('120', 'STRING', {'type': 'Number', 'precision': 5, 'scale': 2}, 120.00),
    ('120', 'STRING', {'type': 'Percent', 'precision': 5, 'scale': 2}, 120.00),
    ('120', 'STRING', {'type': 'Text', 'length': 3}, '120'),
    ('120', 'STRING', {'type': 'EncryptedText', 'length': 3, 'maskChar': 'X', 'maskType': 'all'}, 'XXX'),
    ('120', 'STRING', {'type': 'TextArea'}, '120'),
    ('120', 'STRING', {'type': 'LongTextArea', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120'),
    ('120', 'STRING', {'type': 'Html', 'length': LONG_TEXT_MIN_LENGTH, 'visibleLines': 12}, '120'),
    ('2003-04-12T04:05:06Z', 'STRING', {'type': 'DateTime'}, '2003-04-12T04:05:06.000+0000'),
    ('2020-01-01', 'STRING', {'type': 'Date'}, '2020-01-01'),
    ('10:00:00', 'STRING', {'type': 'Time'}, '10:00:00.000Z'),
    ('true', 'STRING', {'type': 'Checkbox', 'defaultValue': False}, True),
    ('a@b.com', 'STRING', {'type': 'Email'}, 'a@b.com'),
    ('111-222-3333', 'STRING', {'type': 'Phone'}, '111-222-3333'),
    ('green',
     'STRING',
     {
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
     'green'),
    ('green;blue',
     'STRING',
     {
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
     'green;blue'),
    ('https://streamsets.com/', 'STRING', {'type': 'Url'}, 'https://streamsets.com/'),
]
@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES, ids=[f"{i[1]}-{i[2]['type']}" for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, salesforce, input, converter_type, database_type, expected):
    test_name = 'sale_bulk2_origin_data_types_' + database_type['type'] + '_' + \
                get_random_string(string.ascii_lowercase, 10)

    client = salesforce.client

    # Create a hard delete permission file for this client
    assign_hard_delete(client)

    metadata_client = salesforce.metadata_client

    custom_field_name = 'testField__c'
    custom_field_label = 'testField'
    custom_field_type = database_type['type']

    parameters = ''
    for param in database_type:
        if (param != 'type'):
            parameters += '<' + param + '>'
            parameters += str(database_type[param])
            parameters += '</' + param + '>'

    uses_value_set = (custom_field_type == 'Picklist') or (custom_field_type == 'MultiselectPicklist')
    if custom_field_type == 'Picklist':
        parameters = ''
    elif custom_field_type == 'MultiselectPicklist':
        parameters = '<visibleLines>3</visibleLines>'

    custom_field_name = add_custom_field_to_contact(salesforce, custom_field_name, custom_field_label,
                                                    custom_field_type, parameters, uses_value_set)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({custom_field_name: input })

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        'fieldToSet': '/LastName',
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
    target.sobject_type = 'Contact'
    target.field_mapping = []
    target.on_record_error = 'STOP_PIPELINE'

    origin >> expression >> converter >> target

    pipeline = builder.build().configure_for_environment(salesforce)

    sdc_executor.add_pipeline(pipeline)

    read_ids = []
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)

        query_str = f"SELECT Id, {custom_field_name} FROM Contact WHERE LastName = '{test_name}'"
        result = client.query(query_str)
        logger.info(result['records'])
        read_ids = get_ids(result['records'], 'Id')

        assert len(result['records']) == 1
        assert compare_values(expected, result['records'][0][custom_field_name], database_type['type'])
    finally:
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client)
        delete_custom_field_from_contact(metadata_client, custom_field_name)
        clean_up(sdc_executor, pipeline, client, read_ids)


@salesforce
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('test_name,object_name,field_name', OBJECT_NAMES, ids=[i[0] for i in OBJECT_NAMES])
def test_object_names(sdc_builder, sdc_executor, salesforce, test_name, object_name, field_name):
    run_name = 'sale_bulk2_dest_object_names_' + test_name + '_' + get_random_string(string.ascii_lowercase, 10)
    client = salesforce.client

    # Create a hard delete permission file for this client
    assign_hard_delete(client)

    metadata_client = salesforce.metadata_client

    custom_field_name = '{}__c'.format(field_name)
    custom_field_label = 'Value'
    custom_field_type = 'Number'
    custom_field_name = add_custom_field_to_contact(salesforce, custom_field_name, custom_field_label,
                                                    custom_field_type)

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "{custom_field_name}" : 1 }}'
    source.stop_after_first_batch = True

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        'fieldToSet': '/LastName',
        'expression': run_name
    }]

    target = builder.add_stage('Salesforce Bulk API 2.0', type='destination')
    target.sobject_type = 'Contact'
    target.field_mapping = []
    target.on_record_error = 'STOP_PIPELINE'

    source >> expression >> target
    pipeline = builder.build().configure_for_environment(salesforce)

    read_ids = []

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=BULK_PIPELINE_TIMEOUT_SECONDS)

        # Verify that the data were indeed inserted
        result = client.query(f"SELECT Id, {custom_field_name} FROM Contact WHERE LastName = '{run_name}'")
        read_ids = get_ids(result['records'], 'Id')

        assert len(result['records']) == 1
        assert result['records'][0][f'{custom_field_name}'] == 1
    finally:
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client)
        delete_custom_field_from_contact(metadata_client, custom_field_name)
        clean_up(sdc_executor, pipeline, client, read_ids)


@salesforce
@sdc_min_version('5.0.0')
def test_multiple_batches(sdc_builder, sdc_executor, salesforce):
    test_name = 'sale_bulk2_dest_multiple_batches_' + get_random_string(string.ascii_lowercase, 10)
    # Cap at 1000 records so we stay within Salesforce Developer Edition data limits
    batch_size = 20
    batches = 10

    client = salesforce.client

    # Create a hard delete permission file for this client
    assign_hard_delete(client)

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
        # Delete the hard delete permission file to keep the test account clean
        revoke_hard_delete(client)
        clean_up(sdc_executor, pipeline, client, read_ids)


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
