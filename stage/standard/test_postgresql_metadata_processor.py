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
import decimal
import datetime

import pytest
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DATA_TYPES = [
    ('true', 'BOOLEAN', True, 'boolean'),
    ('a', 'CHAR', 'a', 'character'),
#    ('a', 'BYTE', None, 'something'), # Not supported
    (120, 'SHORT', 120, 'smallint'),
    (120, 'INTEGER', 120, 'integer'),
    (120, 'LONG', 120, 'bigint'),
    (20.1, 'FLOAT', 20.1, 'real'),
    (20.1, 'DOUBLE', 20.1, 'double precision'),
    (20.1, 'DECIMAL', decimal.Decimal('20.10'), 'numeric'),
    ('2020-01-01 10:00:00', 'DATE', datetime.datetime(2020, 1, 1, 10, 0), 'date'),
    ('2020-01-01 10:00:00', 'TIME', datetime.datetime(2020, 1, 1, 10, 0), 'time without time zone'),
    ('2020-01-01 10:00:00', 'DATETIME', datetime.datetime(2020, 1, 1, 10, 0), 'timestamp without time zone'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', '2020-01-01T10:00Z', 'timestamp with time zone'),
    ('string', 'STRING', 'string', 'character varying'),
    ('string', 'BYTE_ARRAY', b'string', 'bytea'),
]
@database('postgresql')
@pytest.mark.parametrize('input,converter_type,expected_value,expected_type', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, database, input, converter_type, expected_value, expected_type, keep_data):
    connection = database.engine.connect()
    table_name = get_random_string(string.ascii_letters, 10).lower()

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"value": input })

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/value'],
        'targetType': converter_type,
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 2
    }]

    expression = builder.add_stage('Expression Evaluator')
    expression.field_attribute_expressions = [{
        "fieldToSet": "/value",
        "attributeToSet": "precision",
        "fieldAttributeExpression": "5"
    },{
        "fieldToSet": "/value",
        "attributeToSet": "scale",
        "fieldAttributeExpression": "5"
    }]

    processor = builder.add_stage('PostgreSQL Metadata')
    processor.table_name = table_name

    wiretap = builder.add_wiretap()

    origin >> converter >> expression >> processor >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    pipeline.configuration["shouldRetry"] = False

    sdc_executor.add_pipeline(pipeline)

    try:
        # 1) Run the pipeline for the first time
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # 1.1) We should create table in PostgreSQL
        rs = connection.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
        rows = [row for row in rs]
        assert len(rows) == 1
        assert rows[0][0] == 'value'
        assert rows[0][1] == expected_type

        # 1.2) The pipeline should output one record that is unchanged
        output = wiretap.output_records
        assert len(output) == 1
        assert output[0].field['value'] == expected_value

        # Intermezzo - need to reset wiretap
        wiretap.reset()

        # 2) Let's run the pipeline again, this time the table already exists in the database
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # 2.1) So the pipeline should just output the same (unchanged) record and be done
        output = wiretap.output_records
        assert len(output) == 1
        assert output[0].field['value'] == expected_value
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")


# Rules: https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html
# The processor was written to automatically lowercase all tables names, why? I have no clue, but can't easily
# change it without breaking backward compatibility.
OBJECT_NAMES = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_lowercase, 63), get_random_string(string.ascii_lowercase, 20)),
    ('max_column_name', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 63)),
    ('numbers', get_random_string(string.ascii_lowercase, 5) + "0123456789", get_random_string(string.ascii_lowercase, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_lowercase, 5) + "$_", get_random_string(string.ascii_lowercase, 5) + "$_"),
]
@database('postgresql')
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES, ids=[i[0] for i in OBJECT_NAMES])
def test_object_names(sdc_builder, sdc_executor, test_name, table_name, column_name, database, keep_data):
    connection = database.engine.connect()
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "{column_name}" : 1 }}'
    source.stop_after_first_batch = True

    processor = builder.add_stage('PostgreSQL Metadata')
    processor.table_name = table_name

    trash = builder.add_stage('Trash')

    source >> processor >> trash
    pipeline = builder.build().configure_for_environment(database)

    try:
        sdc_executor.add_pipeline(pipeline)

        # Run the pipeline for the first time
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # 1.1) We should create table in PostgreSQL
        rs = connection.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
        rows = [row for row in rs]
        assert len(rows) == 1
        assert rows[0][0] == column_name
        assert rows[0][1] == 'integer'

        # Run the pipeline for the second time
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # 2.1) No errors should be generated
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
    finally:
        if not keep_data:
                logger.info('Dropping table %s in %s database ...', table_name, database.type)
                connection.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")


@database('postgresql')
def test_multiple_batches(sdc_builder, sdc_executor, database, keep_data):
    table_prefix = get_random_string(string.ascii_letters, 10).lower()
    connection = database.engine.connect()
    builder = sdc_builder.get_pipeline_builder()
    batch_size = 100
    batches = 100

    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.delay_between_batches = 0
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    # We create 100 tables and iterate over them over and over again
    processor = builder.add_stage('PostgreSQL Metadata')
    processor.table_name = table_prefix + "_${record:value('/seq') % 100}"

    trash = builder.add_stage('Trash')

    origin >> processor >> trash
    pipeline = builder.build().configure_for_environment(database)

    try:
        sdc_executor.add_pipeline(pipeline)

        # Run the pipeline for the first time
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', batch_size * batches, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        # 1.1) We should have 100 tables all with a single column
        rs = connection.execute(f"SELECT table_name column_name FROM information_schema.columns WHERE table_name LIKE '{table_prefix}_%%' order by table_name ASC")
        rows = [row[0] for row in rs]
        assert len(rows) == 100

        expected = sorted([f"{table_prefix}_{i}" for i in range(0, 100)])
        assert expected == rows
    finally:
        if not keep_data:
            for i in range(0, 100):
                table_name = table_prefix + "_" + str(i)
                logger.info('Dropping table %s in %s database ...', table_name, database.type)
                connection.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")


@database('postgresql')
def test_dataflow_events(sdc_builder, sdc_executor, database):
    pytest.skip("No events supported in PostgreSQL Metadata Processor at this time.")


@database('postgresql')
def test_push_pull(sdc_builder, sdc_executor, database):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is art of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
