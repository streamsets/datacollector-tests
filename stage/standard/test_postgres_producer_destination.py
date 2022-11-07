# Copyright 2022 StreamSets Inc.
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
import sqlalchemy
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.sdc_min_version('5.4.0'), pytest.mark.database('postgresql')]


DATA_TYPES_POSTGRESQL = [
    # Boolean
    ('true', 'BOOLEAN', 'char(4)', 'true'),
    ('true', 'BOOLEAN', 'int', 1),
    ('true', 'BOOLEAN', 'boolean', True),
    # Byte
    ('65', 'BYTE', 'char(2)', '65'),
    # Char
    ('a', 'CHAR', 'char(1)', 'a'),
    ('a', 'CHAR', 'varchar(1)', 'a'),
    ('a', 'CHAR', 'text', 'a'),
    # Short
    (120, 'SHORT', 'smallint', 120),
    (120, 'SHORT', 'integer', 120),
    (120, 'SHORT', 'bigint', 120),
    (120, 'SHORT', 'decimal(5,2)', 120),
    (120, 'SHORT', 'numeric(5,2)', 120),
    (120, 'SHORT', 'real', 120),
    (120, 'SHORT', 'double precision', 120),
    (120, 'SHORT', 'char(3)', '120'),
    (120, 'SHORT', 'varchar(3)', '120'),
    (120, 'SHORT', 'text', '120'),
    # Integer
    (120, 'INTEGER', 'smallint', 120),
    (120, 'INTEGER', 'integer', 120),
    (120, 'INTEGER', 'bigint', 120),
    (120, 'INTEGER', 'decimal(5,2)', 120),
    (120, 'INTEGER', 'numeric(5,2)', 120),
    (120, 'INTEGER', 'real', 120),
    (120, 'INTEGER', 'double precision', 120),
    (120, 'INTEGER', 'char(3)', '120'),
    (120, 'INTEGER', 'varchar(3)', '120'),
    (120, 'INTEGER', 'text', '120'),
    # Long
    (120, 'LONG', 'smallint', 120),
    (120, 'LONG', 'integer', 120),
    (120, 'LONG', 'bigint', 120),
    (120, 'LONG', 'decimal(5,2)', 120),
    (120, 'LONG', 'numeric(5,2)', 120),
    (120, 'LONG', 'real', 120),
    (120, 'LONG', 'double precision', 120),
    (120, 'LONG', 'char(3)', '120'),
    (120, 'LONG', 'varchar(3)', '120'),
    (120, 'LONG', 'text', '120'),
    # Float
    (120.0, 'FLOAT', 'decimal(5,2)', 120.0),
    (120.0, 'FLOAT', 'numeric(5,2)', 120.0),
    (120.0, 'FLOAT', 'real', 120.0),
    (120.0, 'FLOAT', 'double precision', 120.0),
    (120.0, 'FLOAT', 'char(5)', '120.0'),
    (120.0, 'FLOAT', 'varchar(5)', '120.0'),
    (120.0, 'FLOAT', 'text', '120.0'),
    # Double
    (120.0, 'DOUBLE', 'decimal(5,2)', 120.0),
    (120.0, 'DOUBLE', 'numeric(5,2)', 120.0),
    (120.0, 'DOUBLE', 'real', 120.0),
    (120.0, 'DOUBLE', 'double precision', 120.0),
    (120.0, 'DOUBLE', 'char(5)', '120.0'),
    (120.0, 'DOUBLE', 'varchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'text', '120.0'),
    # Decimal
    (120, 'DECIMAL', 'smallint', 120),
    (120, 'DECIMAL', 'integer', 120),
    (120, 'DECIMAL', 'bigint', 120),
    (120, 'DECIMAL', 'decimal(5,2)', 120.0),
    (120, 'DECIMAL', 'numeric(5,2)', 120.0),
    (120, 'DECIMAL', 'real', 120.0),
    (120, 'DECIMAL', 'double precision', 120.0),
    (120, 'DECIMAL', 'char(6)', '120.00'),
    (120, 'DECIMAL', 'varchar(6)', '120.00'),
    (120, 'DECIMAL', 'text', '120.00'),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATE', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'timestamp with time zone', datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)),
    ('2020-01-01 10:00:00', 'DATE', 'char(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'DATE', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    # Time
    ('2020-01-01 10:00:00', 'TIME', 'time', datetime.time(10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'char(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'TIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'timestamp with time zone', datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)),
    ('2020-01-01 10:00:00', 'DATETIME', 'char(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'DATETIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    # Zoned DateTime
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'char(25)', '2020-01-01 10:00:00+00   '),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'varchar(25)', '2020-01-01 10:00:00+00'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'text', '2020-01-01 10:00:00+00'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'timestamp with time zone', datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)),
    # String
    ('120', 'STRING', 'smallint', 120),
    ('120', 'STRING', 'integer', 120),
    ('120', 'STRING', 'bigint', 120),
    ('120', 'STRING', 'decimal(5,2)', 120.0),
    ('120', 'STRING', 'numeric(5,2)', 120.0),
    ('120', 'STRING', 'real', 120.0),
    ('120', 'STRING', 'double precision', 120.0),
    ('120', 'STRING', 'char(5)', '120  '),
    ('120', 'STRING', 'varchar(5)', '120'),
    ('120', 'STRING', 'text', '120'),
    ('2003-04-12 04:05:06', 'STRING', 'timestamp', datetime.datetime(2003, 4, 12, 4, 5, 6)),
    ('2020-01-01', 'STRING', 'date', datetime.date(2020, 1, 1)),
    ('10:00:00', 'STRING', 'time', datetime.time(10, 0)),
    ('true', 'STRING', 'boolean', True),
    ('{"a": "b"}', 'STRING', 'json', {'a': 'b'}),
    ('{"a": "b"}', 'STRING', 'jsonb', {'a': 'b'}),
    # Byte array
    ('string', 'BYTE_ARRAY', 'bytea', b'string'),
]
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES_POSTGRESQL, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_POSTGRESQL])
def test_data_types_postgresql(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    # Build pipeline
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

    target = builder.add_stage('PostgreSQL Producer')
    target.table_name = table_name
    target.enclose_object_names = True
    target.field_to_column_mapping = []
    target.on_record_error = 'STOP_PIPELINE'
    target.ssl_mode = 'DISABLED'

    origin >> converter >> target

    pipeline = builder.build().configure_for_environment(database)
    # Workarounds for STE,STF specific stuff
    target.table_name = table_name

    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE "{table_name}"(
                "value" {database_type} NULL
            )
        """)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if database_type == 'xmltype':
            rs = connection.execute(f'select XMLTYPE.GETCLOBVAL("value") from "{table_name}"')
        else:
            rs = connection.execute(f'select "value" from "{table_name}"')

        rows = [row for row in rs]
        assert len(rows) == 1
        actual = rows[0][0]

        # Coercions that can't be directly asserted
        if type(actual) == memoryview:
            actual = actual.tobytes()

        assert actual == expected
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f'DROP TABLE "{table_name}"')


# Rules: https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html
OBJECT_NAMES_POSTGRESQL = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 63), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 63)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "$_", get_random_string(string.ascii_letters, 5) + "$_"),
]
@pytest.mark.parametrize('use_multi_row_operation', [True, False])
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES_POSTGRESQL, ids=[i[0] for i in OBJECT_NAMES_POSTGRESQL])
def test_object_names_postgresql(sdc_builder, sdc_executor, database, test_name, table_name, column_name, use_multi_row_operation, keep_data):
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "{column_name}" : 1 }}'
    source.stop_after_first_batch = True

    target = builder.add_stage('PostgreSQL Producer')
    target.table_name = table_name
    target.enclose_object_names = True
    target.field_to_column_mapping = []
    target.use_multi_row_operation = use_multi_row_operation
    target.on_record_error = 'STOP_PIPELINE'
    target.ssl_mode = 'DISABLED'

    source >> target
    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    target.table_name = table_name

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(column_name, sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify that the data were indeed inserted
        result = database.engine.execute(table.select())
        rows = result.fetchall()
        result.close()

        assert len(rows) == 1
        assert rows[0][0] == 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


@pytest.mark.parametrize('use_multi_row_operation', [True, False])
def test_multiple_batches(sdc_builder, sdc_executor, use_multi_row_operation, database, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    batch_size = 100
    batches = 10

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    target = builder.add_stage('PostgreSQL Producer')
    target.table_name = table_name
    target.enclose_object_names = True
    target.field_to_column_mapping = []
    target.use_multi_row_operation = use_multi_row_operation
    target.on_record_error = 'STOP_PIPELINE'
    target.ssl_mode = 'DISABLED'

    origin >> target
    pipeline = builder.build().configure_for_environment(database)

    # Work-arounding STF behavior of upper-casing table name configuration
    target.table_name = table_name

    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('seq', sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        # Now the pipeline will write some amount of records that will be larger, so we get precise count from metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Detected {records} output records")
        # Sanity check
        assert records >= batch_size * batches

        result = database.engine.execute(table.select())
        data = sorted([row[0] for row in result.fetchall()])
        result.close()

        assert data == [i for i in range(0, records)]
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


def test_dataflow_events(sdc_builder, sdc_executor, database):
    pytest.skip("No events supported in PostgreSQL Producer destination at this time.")


def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("PostgreSQL Producer doesn't deal with data formats")


def test_push_pull(sdc_builder, sdc_executor, database):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is art of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
