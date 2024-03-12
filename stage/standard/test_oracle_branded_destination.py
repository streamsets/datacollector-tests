# Copyright 2024 StreamSets Inc.
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
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)

pytestmark = [database("oracle"), sdc_min_version('5.10.0')]


DATA_TYPES_ORACLE = [
    # Oracle actually doesn't have boolean as a type, it does accept boolen insert into char(1) fields though
    ('true', 'BOOLEAN', 'char(1)', '1'),
    ('true', 'BOOLEAN', 'varchar(1)', '1'),
    ('true', 'BOOLEAN', 'nchar(1)', '1'),
    ('true', 'BOOLEAN', 'nvarchar2(1)', '1'),
    # Byte
    ('65', 'BYTE', 'char(2)', '65'),
    # Char
    ('a', 'CHAR', 'char(1)', 'a'),
    ('a', 'CHAR', 'varchar(1)', 'a'),
    ('a', 'CHAR', 'nchar(1)', 'a'),
    ('a', 'CHAR', 'nvarchar2(1)', 'a'),
    # Short
    (120, 'SHORT', 'number', 120),
    (120, 'SHORT', 'char(5)', '120  '),
    (120, 'SHORT', 'varchar(5)', '120'),
    (120, 'SHORT', 'nchar(5)', '120  '),
    (120, 'SHORT', 'nvarchar2(5)', '120'),
    (120, 'SHORT', 'long', '120'),
    # Integer
    (120, 'INTEGER', 'number', 120),
    (120, 'INTEGER', 'char(5)', '120  '),
    (120, 'INTEGER', 'varchar(5)', '120'),
    (120, 'INTEGER', 'nchar(5)', '120  '),
    (120, 'INTEGER', 'nvarchar2(5)', '120'),
    (120, 'INTEGER', 'long', '120'),
    # Long
    (120, 'LONG', 'number', 120),
    (120, 'LONG', 'char(5)', '120  '),
    (120, 'LONG', 'varchar(5)', '120'),
    (120, 'LONG', 'nchar(5)', '120  '),
    (120, 'LONG', 'nvarchar2(5)', '120'),
    (120, 'LONG', 'long', '120'),
    # Float
    (120.0, 'FLOAT', 'number', 120.0),
    (120.0, 'FLOAT', 'char(5)', '120.0'),
    (120.0, 'FLOAT', 'varchar(5)', '120.0'),
    (120.0, 'FLOAT', 'nchar(5)', '120.0'),
    (120.0, 'FLOAT', 'nvarchar2(5)', '120.0'),
    (120.0, 'FLOAT', 'long', '120.0'),
    # Double
    (120.0, 'DOUBLE', 'number', 120.0),
    (120.0, 'DOUBLE', 'char(5)', '120.0'),
    (120.0, 'DOUBLE', 'varchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'nchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'nvarchar2(5)', '120.0'),
    (120.0, 'DOUBLE', 'long', '120.0'),
    # Decimal
    (120.0, 'DECIMAL', 'number', 120.0),
    (120.0, 'DECIMAL', 'char(6)', '120.00'),
    (120.0, 'DECIMAL', 'varchar(6)', '120.00'),
    (120.0, 'DECIMAL', 'nchar(6)', '120.00'),
    (120.0, 'DECIMAL', 'nvarchar2(6)', '120.00'),
    (120.0, 'DECIMAL', 'long', '120.00'),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'char(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'DATE', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'varchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'nchar(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'DATE', 'nvarchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'long', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # Time
    ('2020-01-01 10:00:00', 'TIME', 'char(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'TIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'varchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'nchar(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'TIME', 'nvarchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'long', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'char(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'DATETIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'varchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'nchar(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'DATETIME', 'nvarchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'long', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # Zoned DateTime
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'char(50)', '2020-01-01 10:00:00.000+0000                      '),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'varchar(50)', '2020-01-01 10:00:00.000+0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'varchar2(50)', '2020-01-01 10:00:00.000+0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'nchar(50)', '2020-01-01 10:00:00.000+0000                      '),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'nvarchar2(50)', '2020-01-01 10:00:00.000+0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'long', '2020-01-01 10:00:00.000+0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
#  This test is disabled because it's timezone dependent (you run on local host it will return different value then
#  on Jenkins, ...). Which is actually correct - the type is after all "local timezone" and is always converted by
#  the Oracle database. We can however insert the data to this type.
#    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'timestamp with local time zone', datetime.datetime(2020, 1, 1, 3, 0)),
    # String
    ('string', 'STRING', 'char(15)', 'string         '),
    ('string', 'STRING', 'varchar(15)', 'string'),
    ('string', 'STRING', 'nchar(15)', 'string         '),
    ('string', 'STRING', 'nvarchar2(15)', 'string'),
    ('55', 'STRING', 'number', 55),
    ('1998-1-1 6:22:33', 'STRING', 'date', datetime.datetime(1998, 1, 1, 6, 22, 33)),
    ('1998-1-1 6:22:33', 'STRING', 'timestamp', datetime.datetime(1998, 1, 1, 6, 22, 33)),
    ('55', 'STRING', 'long', '55'),
    ('55', 'STRING', 'blob', b'U'),
    ('55', 'STRING', 'clob', '55'),
    ('55', 'STRING', 'nclob', '55'),
    ('<a></a>', 'STRING', 'xmltype', '<a/>\n'),
    # Byte array
    ('string', 'BYTE_ARRAY', 'char(15)', 'string         '),
    ('string', 'BYTE_ARRAY', 'varchar(15)', 'string'),
    ('string', 'BYTE_ARRAY', 'varchar2(15)', 'string'),
    ('string', 'BYTE_ARRAY', 'nchar(15)', 'string         '),
    ('string', 'BYTE_ARRAY', 'nvarchar2(15)', 'string'),
    ('string', 'BYTE_ARRAY', 'long', 'string'),
    ('string', 'BYTE_ARRAY', 'blob', b'string'),
]
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES_ORACLE, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_ORACLE])
def test_data_types_oracle(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):
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

    target = builder.add_stage('Oracle (Preview)')
    target.table_name = table_name
    target.use_quoted_identifiers = True
    target.field_to_column_mapping = []
    target.on_record_error = 'STOP_PIPELINE'

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


# Rules: https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements008.htm
# Max Lenght: https://stackoverflow.com/questions/756558/what-is-the-maximum-length-of-a-table-name-in-oracle
OBJECT_NAMES_ORACLE = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 30), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 30)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "!@#$%^&*()_+=-?<>", get_random_string(string.ascii_letters, 5) + "!@#$%^&*()_+=-?<>"),
]
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES_ORACLE, ids=[i[0] for i in OBJECT_NAMES_ORACLE])
def test_object_names_oracle(sdc_builder, sdc_executor, database, test_name, table_name, column_name, keep_data):
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "{column_name}" : 1 }}'
    source.stop_after_first_batch = True

    target = builder.add_stage('Oracle (Preview)')
    target.table_name = table_name
    target.use_quoted_identifiers = True
    target.field_to_column_mapping = []
    target.on_record_error = 'STOP_PIPELINE'

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


def test_multiple_batches(sdc_builder, sdc_executor, database, keep_data):
    table_name = get_random_string(string.ascii_uppercase, 20)
    batch_size = 100
    batches = 10

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "SEQ"
    }]

    target = builder.add_stage('Oracle (Preview)')
    target.table_name = table_name
    target.use_quoted_identifiers = True
    target.field_to_column_mapping = []
    target.on_record_error = 'STOP_PIPELINE'

    origin >> target
    pipeline = builder.build().configure_for_environment(database)

    # Work-arounding STF behavior of upper-casing table name configuration
    target.table_name = table_name

    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('SEQ', sqlalchemy.Integer, primary_key=False, quote=True),
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
    pytest.skip("No events supported in Oracle destination at this time.")


def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("Oracle Destination doesn't deal with data formats")


def test_push_pull(sdc_builder, sdc_executor, database):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is part of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
