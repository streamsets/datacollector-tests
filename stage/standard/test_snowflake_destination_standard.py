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
import json
import logging
import string
import pytest
import pytz

from streamsets.sdk.utils import Version
from streamsets.testframework.markers import snowflake, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# AWS S3 bucket in case of AWS or Azure blob storage container in case of Azure.
STORAGE_BUCKET_CONTAINER = 'snowflake'

DATA_TYPES_SHORT = [
    {'field': 'field0', 'type': 'STRING'},
    {'field': 'fiedl1', 'type': 'INTEGER'}
]

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Roger Federer'},
    {'id': 2, 'name': 'Rafael Nadal'},
    {'id': 3, 'name': 'Alexander Zverev'}
]

DATA_TYPES_SNOWFLAKE = [
    # Boolean
    ('true', 'BOOLEAN', 'BOOLEAN', True),
    ('true', 'BOOLEAN', 'VARCHAR(4)', 'true'),
    # Byte
    ('65', 'BYTE', 'CHAR(2)', '65'),
    # Short
    (120, 'SHORT', 'NUMBER', 120),
    (120, 'SHORT', 'CHAR(5)', '120'),
    (120, 'SHORT', 'VARCHAR(5)', '120'),
    (120, 'SHORT', 'NCHAR(5)', '120'),
    (120, 'SHORT', 'NVARCHAR2(5)', '120'),
    (120, 'SHORT', 'INTEGER', 120),
    (120, 'SHORT', 'INT', 120),
    (120, 'SHORT', 'BIGINT', 120),
    # Integer
    (120, 'INTEGER', 'NUMBER', 120),
    (120, 'INTEGER', 'CHAR(5)', '120'),
    (120, 'INTEGER', 'VARCHAR(5)', '120'),
    (120, 'INTEGER', 'NCHAR(5)', '120'),
    (120, 'INTEGER', 'NVARCHAR2(5)', '120'),
    (120, 'INTEGER', 'BIGINT', 120),
    (120, 'INTEGER', 'INTEGER', 120),
    (120, 'INTEGER', 'INT', 120),
    # Long
    (120, 'LONG', 'NUMBER', 120),
    (120, 'LONG', 'CHAR(5)', '120'),
    (120, 'LONG', 'VARCHAR(5)', '120'),
    (120, 'LONG', 'NCHAR(5)', '120'),
    (120, 'LONG', 'NVARCHAR2(5)', '120'),
    (120, 'LONG', 'BIGINT', 120),
    (120, 'LONG', 'INTEGER', 120),
    (120, 'LONG', 'INT', 120),
    # Float
    (120.0, 'FLOAT', 'NUMBER', 120.0),
    (120.0, 'FLOAT', 'CHAR(5)', '120.0'),
    (120.0, 'FLOAT', 'VARCHAR(5)', '120.0'),
    (120.0, 'FLOAT', 'NCHAR(5)', '120.0'),
    (120.0, 'FLOAT', 'NVARCHAR2(5)', '120.0'),
    (120.0, 'FLOAT', 'BIGINT', 120),
    (120.0, 'FLOAT', 'INTEGER', 120),
    (120.0, 'FLOAT', 'INT', 120),
    # Double
    (120.0, 'DOUBLE', 'NUMBER', 120.0),
    (120.0, 'DOUBLE', 'CHAR(5)', '120.0'),
    (120.0, 'DOUBLE', 'VARCHAR(5)', '120.0'),
    (120.0, 'DOUBLE', 'NCHAR(5)', '120.0'),
    (120.0, 'DOUBLE', 'NVARCHAR2(5)', '120.0'),
    (120.0, 'DOUBLE', 'BIGINT', 120),
    (120.0, 'DOUBLE', 'INTEGER', 120),
    (120.0, 'DOUBLE', 'INT', 120),
    (120.0, 'DOUBLE', 'DOUBLE', 120.0),
    (120.0, 'DOUBLE', 'DOUBLE PRECISION', 120.0),
    (120.0, 'DOUBLE', 'REAL', 120.0),
    # Decimal
    (120.0, 'DECIMAL', 'NUMBER', 120.0),
    (120.0, 'DECIMAL', 'CHAR(6)', '120.00'),
    (120.0, 'DECIMAL', 'VARCHAR(6)', '120.00'),
    (120.0, 'DECIMAL', 'NCHAR(6)', '120.00'),
    (120.0, 'DECIMAL', 'NVARCHAR2(6)', '120.00'),
    (120.0, 'DECIMAL', 'BIGINT', 120),
    (120.0, 'DECIMAL', 'INTEGER', 120),
    (120.0, 'DECIMAL', 'INT', 120),
    (120.0, 'DECIMAL', 'DOUBLE', 120.0),
    (120.0, 'DECIMAL', 'DOUBLE PRECISION', 120.0),
    (120.0, 'DECIMAL', 'REAL', 120.0),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'CHAR(50)', '2020-01-01'),
    ('2020-01-01 10:00:00', 'DATE', 'VARCHAR(50)', '2020-01-01'),
    ('2020-01-01 10:00:00', 'DATE', 'VARCHAR2(50)', '2020-01-01'),
    ('2020-01-01 10:00:00', 'DATE', 'NCHAR(50)', '2020-01-01'),
    ('2020-01-01 10:00:00', 'DATE', 'NVARCHAR2(50)', '2020-01-01'),
    ('2020-01-01 10:00:00', 'DATE', 'DATE', datetime.date(2020, 1, 1)),
    # Time
    ('10:00:00', 'TIME', 'TIME', datetime.time(10, 0, 0)),
    ('10:00:00', 'TIME', 'CHAR(50)', '10:00:00.000'),
    ('10:00:00', 'TIME', 'VARCHAR(50)', '10:00:00.000'),
    ('10:00:00', 'TIME', 'VARCHAR2(50)', '10:00:00.000'),
    ('10:00:00', 'TIME', 'NCHAR(50)', '10:00:00.000'),
    ('10:00:00', 'TIME', 'NVARCHAR2(50)', '10:00:00.000'),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'TIMESTAMP_NTZ', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'TIMESTAMP_LTZ', datetime.datetime(2020, 1, 1, 10, 0)), # The expected will be modified by the test due to a bug in datetime
    ('2020-01-01 10:00:00', 'DATETIME', 'CHAR(50)', '2020-01-01 10:00:00.000'),
    ('2020-01-01 10:00:00', 'DATETIME', 'VARCHAR(50)', '2020-01-01 10:00:00.000'),
    ('2020-01-01 10:00:00', 'DATETIME', 'VARCHAR2(50)', '2020-01-01 10:00:00.000'),
    ('2020-01-01 10:00:00', 'DATETIME', 'NCHAR(50)', '2020-01-01 10:00:00.000'),
    ('2020-01-01 10:00:00', 'DATETIME', 'NVARCHAR2(50)', '2020-01-01 10:00:00.000'),
    # Zoned DateTime
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'TIMESTAMP_TZ',
     datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'CHAR(50)', '2020-01-01 10:00:00.000 +0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'VARCHAR(50)', '2020-01-01 10:00:00.000 +0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'VARCHAR2(50)', '2020-01-01 10:00:00.000 +0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'NCHAR(50)', '2020-01-01 10:00:00.000 +0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'NVARCHAR2(50)', '2020-01-01 10:00:00.000 +0000'),
    # String
    ('string', 'STRING', 'CHAR(15)', 'string'),
    ('string', 'STRING', 'VARCHAR(15)', 'string'),
    ('string', 'STRING', 'NCHAR(15)', 'string'),
    ('string', 'STRING', 'NVARCHAR2(15)', 'string'),
    # Byte array - bytearray.fromhex('737472696e67').decode() == 'string'
    ('string', 'BYTE_ARRAY', 'CHAR(15)', '737472696e67'),
    ('string', 'BYTE_ARRAY', 'VARCHAR(15)', '737472696e67'),
    ('string', 'BYTE_ARRAY', 'VARCHAR2(15)', '737472696e67'),
    ('string', 'BYTE_ARRAY', 'NCHAR(15)', '737472696e67'),
    ('string', 'BYTE_ARRAY', 'NVARCHAR2(15)', '737472696e67'),
]

AUTO_CREATED_DATA_TYPES_SNOWFLAKE = [
    # Boolean
    ('true', 'BOOLEAN', 'BOOLEAN', True),
    # Byte
    ('65', 'BYTE', 'NUMBER(38,0)', 65),
    # Short
    (120, 'SHORT', 'NUMBER(38,0)', 120),
    # Integer
    (120, 'INTEGER', 'NUMBER(38,0)', 120),
    # Long
    (120, 'LONG', 'NUMBER(38,0)', 120),
    # Float
    (120.0, 'FLOAT', 'FLOAT', 120.0),
    # Double
    (120.0, 'DOUBLE', 'FLOAT', 120.0),
    # Decimal
    (120.0, 'DECIMAL', 'NUMBER(38,0)', 120.0),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'DATE', datetime.date(2020, 1, 1)),
    # Time
    ('10:00:00', 'TIME', 'TIME(9)', datetime.time(10, 0, 0)),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'TIMESTAMP_NTZ(9)', datetime.datetime(2020, 1, 1, 10, 0)),
    # Zoned DateTime
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'TIMESTAMP_TZ(9)',
     datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)),
    # String
    ('string', 'STRING', 'VARCHAR(16777216)', 'string'),
    # Byte array
    ('byte_array', 'BYTE_ARRAY', 'BINARY(8388608)', bytearray(b'byte_array'))
]

DATA_MAP_SNOWFLAKE = [
    ({"VALUE": {"fullDocument": {"uno": "dos"}}}, {'fullDocument': {'uno': 'dos'}}),
    ({"VALUE": {"fullDocument": ["uno", "dos"]}}, {'fullDocument': ['uno', 'dos']}),
    ({"VALUE": {"uno": "dos"}}, {'uno': 'dos'})
]

DATABASE_OBJECTS_NAMES = [
    ('random', f'STF_{get_random_string(string.ascii_uppercase, 10)}'),
    ('table', 'TABLE'),
    ('select', 'SELECT'),
    ('from', 'FROM'),
    ('table*', 'TABLE*'),
    ('ta$ble', 'TA$BLE'),
    ('max_size', get_random_string(string.ascii_uppercase, 255)),
    ('plus', get_random_string(string.ascii_uppercase, 5) + '+' + get_random_string(string.ascii_uppercase, 5)),
    ('underscore', get_random_string(string.ascii_uppercase, 5) + '_' + get_random_string(string.ascii_uppercase, 5)),
    ('comma', get_random_string(string.ascii_uppercase, 5) + ',' + get_random_string(string.ascii_uppercase, 5)),
    ('short', 'A'),
]


@snowflake
def test_dataflow_events(sdc_builder, sdc_executor, snowflake):
    pytest.skip("No events supported in snowflake destination at this time.")


@snowflake
def test_push_pull(sdc_builder, sdc_executor, snowflake):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is part of test_multiple_batches "
                "and Dev Raw Data Source (pull) is part of test_object_names.")


@snowflake
@pytest.mark.parametrize('input, converter_type, database_type, expected', DATA_TYPES_SNOWFLAKE,
                         ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_SNOWFLAKE])
def test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, snowflake, keep_data):
    if Version(sdc_executor.version) < Version('5.11.0') and database_type == 'TIMESTAMP_LTZ':
        pytest.skip("Support for TIMESTAMP_LTZ added in 5.11.0.")

    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 20)}'
    engine = snowflake.engine

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"VALUE": input})

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/VALUE'],
        'targetType': converter_type,
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 2
    }]
    if converter_type == 'TIME':
        converter.field_type_converter_configs = [{
            'fields': ['/VALUE'],
            'targetType': converter_type,
            'dataLocale': 'en,US',
            'dateFormat': 'OTHER',
            'otherDateFormat': 'HH:MM:SS',
            'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
            'scale': 2
        }]

    if database_type == 'TIMESTAMP_LTZ': # this is needed due to a strange behavior (bug?) in datetime
        timezone = pytz.timezone('America/Los_Angeles')
        expected = datetime.datetime(2020, 1, 1, 10, 0)
        expected = timezone.localize(expected)

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.

    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    snowflake_destination = builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         on_record_error='STOP_PIPELINE')

    origin >> converter >> snowflake_destination
    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        rs = engine.execute(f"""
            CREATE TABLE {table_name} (
                "VALUE" {database_type} NULL
            )
        """)
        logger.info("Create Table Result")
        logger.info(rs.fetchall())

        # Run pipeline and read from Snowflake to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if database_type == 'xmltype':
            rs = engine.execute(f'select XMLTYPE.GETCLOBVAL("VALUE") from "{table_name}"')
        else:
            rs = engine.execute(f'select VALUE from "{table_name}"')

        rows = [row for row in rs]
        assert len(rows) == 1
        assert rows[0][0] == expected
    finally:
        if not keep_data:
            try:
                _delete_table(snowflake, table_name, stage_name)
                _stop_pipeline(sdc_executor, pipeline)
            except Exception as ex:
                logger.error(ex)


@snowflake
@pytest.mark.parametrize('input, converter_type, database_type, expected', AUTO_CREATED_DATA_TYPES_SNOWFLAKE,
                         ids=[f"{i[1]}-{i[2]}" for i in AUTO_CREATED_DATA_TYPES_SNOWFLAKE])
def test_auto_create_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, snowflake,
                                keep_data):
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 20)}'
    engine = snowflake.engine

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"VALUE": input})

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/VALUE'],
        'targetType': converter_type,
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 2
    }]
    if converter_type == 'TIME':
        converter.field_type_converter_configs = [{
            'fields': ['/VALUE'],
            'targetType': converter_type,
            'dataLocale': 'en,US',
            'dateFormat': 'OTHER',
            'otherDateFormat': 'HH:MM:SS',
            'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
            'scale': 2
        }]

    # we use this to fix decimal precision to 38,0
    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(field_attribute_expressions=[
        {
            'fieldToSet': '/VALUE',
            'attributeToSet': 'precision',
            'fieldAttributeExpression': '38'
        },
        {
            'fieldToSet': '/VALUE',
            'attributeToSet': 'scale',
            'fieldAttributeExpression': '0'
        }]
    )

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.

    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    snowflake_destination = builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         table_auto_create=True,
                                         on_record_error='STOP_PIPELINE')

    origin >> converter >> expression_evaluator >> snowflake_destination
    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Run pipeline and read from Snowflake to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if database_type == 'xmltype':
            rs = engine.execute(f'select XMLTYPE.GETCLOBVAL("VALUE") from "{table_name}"')
        else:
            rs = engine.execute(f'select VALUE from "{table_name}"')

        rows = [row for row in rs]
        assert len(rows) == 1
        assert rows[0][0] == expected

        table_def = engine.execute(f'describe table "{table_name}"')
        assert table_def.fetchall() == [
            ('VALUE', database_type, 'COLUMN', 'Y', None, 'N', 'N', None, None, None, None, None)
        ]
    finally:
        if not keep_data:
            try:
                _delete_table(snowflake, table_name, stage_name)
                _stop_pipeline(sdc_executor, pipeline)
            except Exception as ex:
                logger.error(ex)


@snowflake
@pytest.mark.parametrize('data,expected_value', DATA_MAP_SNOWFLAKE)
@pytest.mark.parametrize('column_type', ['VARIANT', 'OBJECT'])
def test_data_types_map(sdc_builder, sdc_executor, snowflake, keep_data, data, expected_value, column_type):

    if Version(sdc_executor.version) < Version('6.2.0') and column_type == 'OBJECT':
        pytest.skip('Support for OBJECT introduced in 6.2.0')

    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 20)}'
    engine = snowflake.engine

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps(data)

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.

    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    snowflake_destination = builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         on_record_error='STOP_PIPELINE',
                                         data_drift_enabled=False)

    origin >> snowflake_destination
    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        rs = engine.execute(f"""
            CREATE TABLE {table_name} (
                "VALUE" {column_type} NULL
            )
        """)
        logger.info("Create Table Result")
        logger.info(rs.fetchall())

        # Run pipeline and read from Snowflake to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rs = engine.execute(f'select VALUE from "{table_name}"')
        rows = [row for row in rs]
        assert len(rows) == 1
        assert json.loads(rows[0][0]) == expected_value
    finally:
        if not keep_data:
            try:
                _delete_table(snowflake, table_name, stage_name)
                _stop_pipeline(sdc_executor, pipeline)
            except Exception as ex:
                logger.error(ex)


@sdc_min_version("6.2.0")
@snowflake
@pytest.mark.parametrize('data,expected_value', DATA_MAP_SNOWFLAKE)
def test_data_types_map_using_data_drift(sdc_builder, sdc_executor, snowflake, keep_data, data, expected_value):
    data["EXTRA_VALUE"] = 1
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 20)}'
    engine = snowflake.engine

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps(data)

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.

    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    snowflake_destination = builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         on_record_error='STOP_PIPELINE',
                                         data_drift_enabled=True)

    origin >> snowflake_destination
    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        rs = engine.execute(f"""
            CREATE TABLE {table_name} (
                "EXTRA_VALUE" NUMBER NULL
            )
        """)
        logger.info("Create Table Result")
        logger.info(rs.fetchall())

        # Run pipeline and read from Snowflake to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rs = engine.execute(f'select VALUE from "{table_name}"')
        rows = [row for row in rs]
        assert len(rows) == 1
        assert json.loads(rows[0][0]) == expected_value

        rs = engine.execute(f'describe table "{table_name}"')
        rows = [row for row in rs]
        assert len(rows) == 2
        has_value_field = False
        for row in rows:
            if row[0] == 'VALUE':
                assert row[1] == 'VARIANT'
                has_value_field = True
        assert has_value_field, "VALUE field was not created by the pipeline using data drift"

    finally:
        if not keep_data:
            try:
                _delete_table(snowflake, table_name, stage_name)
                _stop_pipeline(sdc_executor, pipeline)
            except Exception as ex:
                logger.error(ex)


@sdc_min_version("6.2.0")
@snowflake
@pytest.mark.parametrize('data,expected_value', DATA_MAP_SNOWFLAKE)
def test_data_types_map_using_table_auto_create(sdc_builder, sdc_executor, snowflake, keep_data, data, expected_value):
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 20)}'
    engine = snowflake.engine

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps(data)

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.

    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    snowflake_destination = builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         on_record_error='STOP_PIPELINE',
                                         table_auto_create=True)

    origin >> snowflake_destination
    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Run pipeline and read from Snowflake to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rs = engine.execute(f'select VALUE from "{table_name}"')
        rows = [row for row in rs]
        assert len(rows) == 1
        assert json.loads(rows[0][0]) == expected_value

        rs = engine.execute(f'describe table "{table_name}"')
        rows = [row for row in rs]
        assert len(rows) == 1
        assert rows[0][1] == 'VARIANT'

    finally:
        if not keep_data:
            try:
                _delete_table(snowflake, table_name, stage_name)
                _stop_pipeline(sdc_executor, pipeline)
            except Exception as ex:
                logger.error(ex)


@snowflake
@pytest.mark.parametrize(
    'data',
    (
            [],
            ['Cyndaquil', 'Quilava', 'Typhlosion'],
            [1, 2, 3],
            [2, 1, "Ash Ketchum", {'a': '1', 'b': '2'}, 3.91, ['Pichu', 'Pikachu', 'Raichu']]
    )
)
def test_data_types_array(sdc_builder, sdc_executor, snowflake, keep_data, data):
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 20)}'
    engine = snowflake.engine

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"VALUE": data})

    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # Path inside the bucket in case of AWS S3 or the path inside the container in case of Azure Blob Storage container
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    snowflake_destination = builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         on_record_error='STOP_PIPELINE')

    origin >> snowflake_destination
    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        rs = engine.execute(f"""CREATE TABLE {table_name} ("VALUE" {'ARRAY'} NULL)""")
        logger.info(f"Result from creating the table: {rs.fetchall()}")

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        rs = engine.execute(f'select VALUE from "{table_name}"')
        rows = [row for row in rs]
        assert len(rows) == 1
        assert json.loads(rows[0][0]) == data
    finally:
        if not keep_data:
            try:
                _delete_table(snowflake, table_name, stage_name)
                _stop_pipeline(sdc_executor, pipeline)
            except Exception as ex:
                logger.error(ex)


@snowflake
@sdc_min_version('3.14.0')
@pytest.mark.parametrize('database_name_category,table_name', DATABASE_OBJECTS_NAMES,
                         ids=[i[0] for i in DATABASE_OBJECTS_NAMES])
def test_object_names(sdc_builder, sdc_executor, snowflake, database_name_category, table_name):
    """Test for Snowflake destination stage. We do so by running a dev raw data source to
    Snowflake destination and then reading data from SQL Server database for assertion.
    Standard reserved word for domain object test.

    The pipeline looks like:
    dev_raw_data_source >> snowflake_destination
    """

    # We will use a random schema so tables with same names in different executions do not collide
    # (so some extra queries are needed)
    schema_name = f'STF_SCHEMA_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # Create a table and stage in Snowflake.
    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    # The random schema is created to stage the files, we will reuse it for the table as well
    snowflake.create_stage(stage_name, storage_path, schema_name)
    # We need to create the schema manually (this is the only way for now, but might be updated later on
    # if STF ever supports creating other schemas, or if the table template changes)
    stmt = f'CREATE TABLE "{schema_name}"."{table_name}" (name string, id integer, PRIMARY KEY (id))'
    snowflake.engine.execute(stmt)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    raw_data = '\n'.join(json.dumps(row) for row in ROWS_IN_DATABASE)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         stage_schema=schema_name,
                                         table=table_name)

    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    # We override the default schema with our random one
    snowflake_destination = pipeline.stages.get(label=snowflake_destination.label)
    snowflake_destination.set_attributes(schema=schema_name)
    sdc_executor.update_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        # Again, we need to take into account our custom schema
        stmt = f'SELECT * FROM "{schema_name}"."{table_name}"'
        result = engine.execute(stmt)
        data_from_database = sorted(result.fetchall(), key=lambda row: row[1])  # order by id
        result.close()
        assert data_from_database == [(row['name'], row['id']) for row in ROWS_IN_DATABASE]
    finally:
        logger.debug('Staged files will be deleted from %s ...', stage_name)
        stmt = f'DROP TABLE "{schema_name}"."{table_name}"'
        engine.execute(stmt)
        snowflake.drop_entities(stage_name=stage_name, schema_name=schema_name)
        # And also delete the custom schema
        stmt = f'DROP SCHEMA "{schema_name}"'
        engine.execute(stmt)
        engine.dispose()


@sdc_min_version('3.14.0')
@snowflake
def test_multiple_batches(sdc_builder, sdc_executor, snowflake):
    """Generate records in multiple batches and assert that the data is in Snowflake.
    Pipeline structure:
    dev_data_source >> snowflake_destination
    dev_raw_generator >> wiretap
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the first pipeline with created entities in Snowflake stage configurations.
    builder = sdc_builder.get_pipeline_builder()

    # Build Dev Raw
    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=10,
                                      delay_between_batches=1,
                                      fields_to_generate=DATA_TYPES_SHORT)

    # Build Snow Flake
    snowflake_destination = builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name,
                                         data_drift_enabled=True,
                                         table_auto_create=True)

    wiretap = builder.add_wiretap()
    dev_data_generator >> [wiretap.destination, snowflake_destination]

    # Build first pipeline
    pipeline = builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Run regular pipeline
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(30, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        # Assert data from snowflake and data from wiretap
        stmt = f'select * from "{table_name}"'
        result = snowflake.engine.execute(stmt)
        records = [record.field.values() for record in wiretap.output_records]
        records_from_database = result.fetchall()
        assert len(records) == len(records_from_database)
        for i in range(len(records)):
            assert len(records[i]) == len(records_from_database[i])
            assert all(element in records_from_database[i].values() for element in records[i])
            assert all(element in records[i] for element in records_from_database[i])

    finally:
        try:
            _delete_table(snowflake, table_name, stage_name)
            _stop_pipeline(sdc_executor, pipeline)
        except Exception as ex:
            logger.error(ex)


def _get_columns(engine, table_name):
    logger.info('Getting columns info for tables = %s...', table_name)
    result = engine.execute(f'describe table "{table_name}"')
    column_info = sorted(result.fetchall(), key=lambda row: row[0])
    result.close()
    column_info_two_cols = [(elem[0], elem[1]) for elem in column_info]
    return column_info_two_cols


def _delete_table(snowflake, table_name, stage_name):
    logger.info('Deleting table with name = %s...', table_name)
    query = f'DROP TABLE "{table_name}";'
    snowflake.drop_entities(stage_name=stage_name)
    snowflake.engine.execute(query)
    snowflake.engine.dispose()


def _stop_pipeline(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)
