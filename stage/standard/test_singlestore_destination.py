# Copyright 2023 StreamSets Inc.
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
from streamsets.testframework.markers import singlestore, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.singlestore, sdc_min_version('5.5.0')]

# Reference here: https://docs.singlestore.com/managed-service/en/reference/sql-reference/data-types.html
DATA_TYPES_SINGLESTORE = [
    # Boolean
    ('true', 'BOOLEAN', 'char(1)', '1'),
    ('true', 'BOOLEAN', 'char(5)', '1'),
    ('true', 'BOOLEAN', 'int', 1),
    # Byte
    ('65', 'BYTE', 'char(2)', '65'),
    # Char
    ('a', 'CHAR', 'char(1)', 'a'),
    ('a', 'CHAR', 'varchar(1)', 'a'),
    ('a', 'CHAR', 'text', 'a'),
    # Short
    (120, 'SHORT', 'tinyint', 120),
    (120, 'SHORT', 'tinyint unsigned', 120),
    (120, 'SHORT', 'smallint', 120),
    (120, 'SHORT', 'smallint unsigned', 120),
    (120, 'SHORT', 'mediumint', 120),
    (120, 'SHORT', 'mediumint unsigned', 120),
    (120, 'SHORT', 'int', 120),
    (120, 'SHORT', 'int unsigned', 120),
    (120, 'SHORT', 'bigint', 120),
    (120, 'SHORT', 'bigint unsigned', 120),
    (120, 'SHORT', 'decimal(5,2)', 120.0),
    (120, 'SHORT', 'numeric(5,2)', 120.0),
    (120, 'SHORT', 'char(5)', '120'),
    (120, 'SHORT', 'varchar(5)', '120'),
    (120, 'SHORT', 'binary(5)', b'120\x00\x00'),
    (120, 'SHORT', 'varbinary(5)', b'120'),
    (120, 'SHORT', 'text', '120'),
    (120, 'SHORT', 'blob', b'120'),
    # Integer
    (120, 'INTEGER', 'tinyint', 120),
    (120, 'INTEGER', 'tinyint unsigned', 120),
    (120, 'INTEGER', 'smallint', 120),
    (120, 'INTEGER', 'smallint unsigned', 120),
    (120, 'INTEGER', 'mediumint', 120),
    (120, 'INTEGER', 'mediumint unsigned', 120),
    (120, 'INTEGER', 'int', 120),
    (120, 'INTEGER', 'int unsigned', 120),
    (120, 'INTEGER', 'bigint', 120),
    (120, 'INTEGER', 'bigint unsigned', 120),
    (120, 'INTEGER', 'decimal(5,2)', 120.0),
    (120, 'INTEGER', 'numeric(5,2)', 120.0),
    (120, 'INTEGER', 'char(5)', '120'),
    (120, 'INTEGER', 'varchar(5)', '120'),
    (120, 'INTEGER', 'binary(5)', b'120\x00\x00'),
    (120, 'INTEGER', 'varbinary(5)', b'120'),
    (120, 'INTEGER', 'text', '120'),
    (120, 'INTEGER', 'blob', b'120'),
    # Long
    (120, 'LONG', 'tinyint', 120),
    (120, 'LONG', 'tinyint unsigned', 120),
    (120, 'LONG', 'smallint', 120),
    (120, 'LONG', 'smallint unsigned', 120),
    (120, 'LONG', 'mediumint', 120),
    (120, 'LONG', 'mediumint unsigned', 120),
    (120, 'LONG', 'int', 120),
    (120, 'LONG', 'int unsigned', 120),
    (120, 'LONG', 'bigint', 120),
    (120, 'LONG', 'bigint unsigned', 120),
    (120, 'LONG', 'decimal(5,2)', 120.0),
    (120, 'LONG', 'numeric(5,2)', 120.0),
    (120, 'LONG', 'char(5)', '120'),
    (120, 'LONG', 'varchar(5)', '120'),
    (120, 'LONG', 'binary(5)', b'120\x00\x00'),
    (120, 'LONG', 'varbinary(5)', b'120'),
    (120, 'LONG', 'text', '120'),
    (120, 'LONG', 'blob', b'120'),
    # Float
    (120.0, 'FLOAT', 'numeric(5,2)', 120.0),
    (120.0, 'FLOAT', 'decimal(5,2)', 120.0),
    (120.0, 'FLOAT', 'float', 120.0),
    (120.0, 'FLOAT', 'double', 120.0),
    (120.0, 'FLOAT', 'char(5)', '120.0'),
    (120.0, 'FLOAT', 'varchar(5)', '120.0'),
    (120.0, 'FLOAT', 'binary(5)', b'120.0'),
    (120.0, 'FLOAT', 'varbinary(5)', b'120.0'),
    (120.0, 'FLOAT', 'text', '120.0'),
    (120.0, 'FLOAT', 'blob', b'120.0'),
    # Double
    (120.0, 'DOUBLE', 'numeric(5,2)', 120.0),
    (120.0, 'DOUBLE', 'decimal(5,2)', 120.0),
    (120.0, 'DOUBLE', 'float', 120.0),
    (120.0, 'DOUBLE', 'double', 120.0),
    (120.0, 'DOUBLE', 'char(5)', '120.0'),
    (120.0, 'DOUBLE', 'varchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'binary(5)', b'120.0'),
    (120.0, 'DOUBLE', 'varbinary(5)', b'120.0'),
    (120.0, 'DOUBLE', 'text', '120.0'),
    (120.0, 'DOUBLE', 'blob', b'120.0'),
    # Decimal
    (120.0, 'DECIMAL', 'numeric(5,2)', 120.00),
    (120.0, 'DECIMAL', 'decimal(5,2)', 120.00),
    (120.0, 'DECIMAL', 'float', 120.0),
    (120.0, 'DECIMAL', 'double', 120.0),
    (120.0, 'DECIMAL', 'char(6)', '120.00'),
    (120.0, 'DECIMAL', 'varchar(6)', '120.00'),
    (120.0, 'DECIMAL', 'binary(6)', b'120.00'),
    (120.0, 'DECIMAL', 'varbinary(6)', b'120.00'),
    (120.0, 'DECIMAL', 'text', '120.00'),
    (120.0, 'DECIMAL', 'blob', b'120.00'),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATE', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # Time
    ('2020-01-01 10:00:00', 'TIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'TIME', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATETIME', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # String
    ('120', 'STRING', 'tinyint', 120),
    ('120', 'STRING', 'tinyint unsigned', 120),
    ('120', 'STRING', 'smallint', 120),
    ('120', 'STRING', 'smallint unsigned', 120),
    ('120', 'STRING', 'mediumint', 120),
    ('120', 'STRING', 'mediumint unsigned', 120),
    ('120', 'STRING', 'int', 120),
    ('120', 'STRING', 'int unsigned', 120),
    ('120', 'STRING', 'bigint', 120),
    ('120', 'STRING', 'bigint unsigned', 120),
    ('120.0', 'STRING', 'decimal(5,2)', 120.0),
    ('120.0', 'STRING', 'numeric(5,2)', 120.0),
    ('120.0', 'STRING', 'float', 120.0),
    ('120.0', 'STRING', 'double', 120.0),
    ('1998-01-01', 'STRING', 'date', datetime.date(1998, 1, 1)),
    ('1998-01-01 06:11:22', 'STRING', 'datetime', datetime.datetime(1998, 1, 1, 6, 11, 22)),
    ('1998-01-01 06:11:22', 'STRING', 'timestamp', datetime.datetime(1998, 1, 1, 6, 11, 22)),
    ('06:11:22', 'STRING', 'time', datetime.timedelta(0, 22282)),
    ('string', 'STRING', 'char(6)', 'string'),
    ('string', 'STRING', 'varchar(6)', 'string'),
    ('string', 'STRING', 'binary(6)', b'string'),
    ('string', 'STRING', 'varbinary(6)', b'string'),
    ('string', 'STRING', 'text', 'string'),
    ('string', 'STRING', 'blob', b'string'),
    ('a', 'STRING', "enum('a', 'b')", 'a'),
    ('a', 'STRING', "set('a', 'b')", 'a'),
]
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES_SINGLESTORE, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_SINGLESTORE])
def test_data_types(sdc_builder, sdc_executor, singlestore, input, converter_type, database_type, expected, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = singlestore.engine.connect()
    connection.execute("SET sql_mode=ANSI_QUOTES")

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

    target = builder.add_stage('SingleStore')
    target.table_name = table_name
    target.field_to_column_mapping = []
    target.on_record_error = 'STOP_PIPELINE'
    target.use_fast_load = False

    origin >> converter >> target

    pipeline = builder.build().configure_for_environment(singlestore)
    # Workarounds for STE,STF specific stuff
    target.table_name = table_name
    target.init_query = "SET sql_mode=ANSI_QUOTES"

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
            logger.info('Dropping table %s ...', table_name)
            connection.execute(f'DROP TABLE "{table_name}"')



# Rules: https://docs.singlestore.com/managed-service/en/reference/configuration-reference/system-limits.html#naming-constraints
# Rules: https://docs.singlestore.com/managed-service/en/reference/sql-reference/restricted-keywords.html
OBJECT_NAMES_SINGLESTORE = [
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 64), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 64)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "$_", get_random_string(string.ascii_letters, 5) + "$_"),
]
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES_SINGLESTORE, ids=[i[0] for i in OBJECT_NAMES_SINGLESTORE])
def test_object_names(sdc_builder, sdc_executor, singlestore, test_name, table_name, column_name, keep_data):
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "{column_name}" : 1 }}'
    source.stop_after_first_batch = True

    target = builder.add_stage('SingleStore')
    target.table_name = table_name
    target.field_to_column_mapping = []
    target.on_record_error = 'STOP_PIPELINE'
    target.use_fast_load = False

    source >> target
    pipeline = builder.build().configure_for_environment(singlestore)
    # Work-arounding STF behavior of upper-casing table name configuration
    target.table_name = table_name
    target.init_query = "SET sql_mode=ANSI_QUOTES"

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(column_name, sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s ...', table_name)
        table.create(singlestore.engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify that the data were indeed inserted
        result = singlestore.engine.execute(table.select())
        rows = result.fetchall()
        result.close()

        assert len(rows) == 1
        assert rows[0][0] == 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s...', table_name)
            table.drop(singlestore.engine)


def test_multiple_batches(sdc_builder, sdc_executor, singlestore, keep_data):
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

    target = builder.add_stage('SingleStore')
    target.table_name = table_name
    target.field_to_column_mapping = []
    target.on_record_error = 'STOP_PIPELINE'
    target.use_fast_load = False

    origin >> target
    pipeline = builder.build().configure_for_environment(singlestore)

    # Work-arounding STF behavior of upper-casing table name configuration
    target.table_name = table_name
    target.init_query = "SET sql_mode=ANSI_QUOTES"

    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('seq', sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s ...', table_name)
        table.create(singlestore.engine)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        # Now the pipeline will write some amount of records that will be larger, so we get precise count from metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Detected {records} output records")
        # Sanity check
        assert records >= batch_size * batches

        result = singlestore.engine.execute(table.select())
        data = sorted([row[0] for row in result.fetchall()])
        result.close()

        assert data == [i for i in range(0, records)]
    finally:
        if not keep_data:
            logger.info('Dropping table %s...', table_name)
            table.drop(singlestore.engine)


def test_dataflow_events(sdc_builder, sdc_executor, singlestore):
    pytest.skip("No events supported in SingleStore destination at this time.")


def test_data_format(sdc_builder, sdc_executor, singlestore, keep_data):
    pytest.skip("SingleStore destination doesn't deal with data formats")


def test_push_pull(sdc_builder, sdc_executor, singlestore):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is art of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
