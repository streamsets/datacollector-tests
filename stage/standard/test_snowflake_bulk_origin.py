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

import pytest
from streamsets.testframework.markers import snowflake, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)

BULK_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_snowflake_SnowflakeBulkDSource'

pytestmark = [snowflake, sdc_min_version('5.7.0')]

DEFAULT_DATABASE = 'STF_DB'
DEFAULT_SCHEMA = 'STF_SCHEMA'

DATABASE_RECORD_HEADER_ATTRIBUTE_NAME = 'snowflake.db'
SCHEMA_RECORD_HEADER_ATTRIBUTE_NAME = 'snowflake.schema'
TABLE_RECORD_HEADER_ATTRIBUTE_NAME = 'snowflake.table'

DEFAULT_COLUMNS = [{'name': 'ID', 'type': 'NUMBER', 'primary_key': True},
                   {'name': 'NAME', 'type': 'STRING', 'primary_key': False}]

DEFAULT_RECORDS = [(1, 'Roger Federer'), (2, 'Rafael Nadal'), (3, 'Dominic Thiem')]

# Very similar to destination types, but removing useless redundancy for the origin,
# as we do not upload different types into others
DATA_TYPES_SNOWFLAKE = [
    # db type             inserted value                expected type     expected data                               expected string data
    ('BOOLEAN',           True,                         'BOOLEAN',        True,                                       'true'),
    ('NUMBER',            123412341,                    'LONG',           123412341,                                  '123412341'),
    ('BIGINT',            123412341,                    'LONG',           123412341,                                  '123412341'),
    ('INTEGER',           123412341,                    'LONG',           123412341,                                  '123412341'),
    ('INT',               123412341,                    'LONG',           123412341,                                  '123412341'),
    ('FLOAT',             12312.1231,                   'DOUBLE',         12312.1231,                                 '12312.1231'),
    ('REAL',              12312.1231,                   'DOUBLE',         12312.1231,                                 '12312.1231'),
    ('DOUBLE',            12312.1231,                   'DOUBLE',         12312.1231,                                 '12312.1231'),
    ('DOUBLE PRECISION',  12312.1231,                   'DOUBLE',         12312.1231,                                 '12312.1231'),
    ('DECIMAL',           12312,                        'LONG',           12312,                                      '12312'),
    ('NUMBER(38,0)',      12312,                        'LONG',           12312,                                      '12312'),
    ('NUMBER(30,8)',      12312.1233,                   'DOUBLE',         12312.1233,                                 '12312.12330000'),
    ('DATE',              '2020-02-03',                 'DATE',           datetime.datetime(2020, 2, 3, 0, 0),        '2020-02-03'),
    ('TIME',              '10:02:06',                   'TIME',           datetime.datetime(1970, 1, 1, 10, 2, 6),    '10:02:06'),
    ('TIMESTAMP_NTZ',     '2020-02-03 11:12:13',        'DATETIME',       datetime.datetime(2020, 2, 3, 11, 12, 13),  '2020-02-03 11:12:13.000'),
    ('TIMESTAMP_TZ',      '2011-02-04T09:23:45+00:00',  'ZONED_DATETIME', '2011-02-04T09:23:45Z',                     '2011-02-04 09:23:45.000 Z'),
    ('TIMESTAMP_TZ',      '2020-01-01T10:00:00+12:00',  'ZONED_DATETIME', '2020-01-01T10:00:00+12:00',                '2020-01-01 10:00:00.000 +1200'),
    ('TIMESTAMP_TZ',      '2020-01-01T10:00:00+00:12',  'ZONED_DATETIME', '2020-01-01T10:00:00+00:12',                '2020-01-01 10:00:00.000 +0012'),
    ('CHAR(1)',           's',                          'STRING',         's',                                        's'),
    ('CHAR(15)',          'string',                     'STRING',         'string',                                   'string'),
    ('VARCHAR(15)',       'string',                     'STRING',         'string',                                   'string'),
    ('NCHAR(15)',         'string',                     'STRING',         'string',                                   'string'),
    ('NVARCHAR2(15)',     'string',                     'STRING',         'string',                                   'string'),
    ('BINARY',            '737472696e67',               'BYTE_ARRAY',     b'string',                                  '737472696E67'),
    ('GEOGRAPHY',         'LINESTRING(-124.20 42.0, -120.01 41.99)', 'STRING', 'LINESTRING(-124.2 42,-120.01 41.99)', 'LINESTRING(-124.2 42,-120.01 41.99)'),
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

# AWS S3 bucket in case of AWS or Azure blob storage container in case of Azure.
STORAGE_BUCKET_CONTAINER = 'snowflake'


def create_table(engine, table_name, column_definitions, primary_keys_clause):
    logger.info(f'Creating table {table_name}...')
    engine.execute(f'CREATE TABLE {DEFAULT_DATABASE}.{DEFAULT_SCHEMA}."{table_name}" '
                   f'({",".join(column_definitions)}{primary_keys_clause});')


def insert_values(engine, table_name, column_names, values, records_per_block=1000):
    def split(a, n):
        for i in range(0, len(a), n):
            yield a[i:i + n]

    total_records = len(values)
    split_values = list(split(values, records_per_block))
    logger.info(f'Adding {total_records} records to the table {table_name} divided into {len(split_values)} blocks of '
                f'at most {records_per_block} records')

    block_num = 1
    total_records_added = 0
    for values_block in split_values:
        logger.info(f'{total_records_added} / {total_records} have been added. '
                    f'Adding block number {block_num} containing {len(values_block)} records...')

        engine.execute(f'INSERT INTO {DEFAULT_DATABASE}.{DEFAULT_SCHEMA}."{table_name}" ({",".join(column_names)}) '
                       f'VALUES {str(values_block)[1:-1]};')

        block_num += 1
        total_records_added += len(values_block)


def get_columns_information(columns):
    column_names = []
    column_definitions = []
    primary_keys = []
    for column in columns:
        column_names.append(f'{column["name"]}')
        column_definitions.append(f'{column["name"]} {column["type"]}')
        if "primary_key" in column and column["primary_key"]:
            primary_keys.append(f'{column["name"]}')

    primary_keys_clause = f', PRIMARY KEY({",".join(primary_keys)})' if primary_keys else ""

    return column_names, column_definitions, primary_keys_clause


def create_table_and_insert_values(engine, table_name, columns, values):
    column_names, column_definitions, primary_keys_clause = get_columns_information(columns)
    create_table(engine, table_name, column_definitions, primary_keys_clause)
    insert_values(engine, table_name, column_names, values)
    return column_names


def drop_table(engine, table_name):
    logger.info(f'Dropping table {table_name}...')
    engine.execute(f'DROP TABLE IF EXISTS "{table_name}"')


@pytest.mark.parametrize('read_values_as_string', [True, False])
def test_data_types(sdc_builder, sdc_executor, snowflake, read_values_as_string):
    """
    Create a snowflake table and stage and populates data with specific data types. Check types
    inferred are correct.
    Uses Snowflake Bulk Origin to read from the table using the same stage.
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # ID column is so that commenting all types except one still works
    columns = [{'name': 'ID', 'type': 'NUMBER'}]
    inserted_record = [(1,)]
    if read_values_as_string:
        expected_record = [('1', 'STRING')]
    else:
        expected_record = [(1, 'LONG')]

    for i in range(0, len(DATA_TYPES_SNOWFLAKE)):
        db_type = DATA_TYPES_SNOWFLAKE[i][0]
        if Version(sdc_builder.version) < Version('5.8.0') and db_type == 'GEOGRAPHY':
            continue
        db_input_data = DATA_TYPES_SNOWFLAKE[i][1]
        if read_values_as_string:
            expected_type = 'STRING'
            expected_data = DATA_TYPES_SNOWFLAKE[i][4]
        else:
            expected_type = DATA_TYPES_SNOWFLAKE[i][2]
            expected_data = DATA_TYPES_SNOWFLAKE[i][3]
        columns.append({'name': f'VALUE_{i}', 'type': f'{db_type}'})
        inserted_record[0] = inserted_record[0] + (db_input_data,)
        expected_record.append((expected_data, expected_type))

    column_names = create_table_and_insert_values(engine, table_name, columns, inserted_record)

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location='INTERNAL')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location='INTERNAL',
                                    snowflake_stage_name=stage_name,
                                    read_values_as_string=read_values_as_string)

    if Version(sdc_builder.version) >= Version('5.8.0'):
        snowflake_origin.set_attributes(table_or_view_configuration=[{'inclusionPattern': table_name}])
    else:
        snowflake_origin.set_attributes(table_config=[{'inclusionPattern': table_name}])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        records = wiretap.output_records

        # check that the number of records is equal to what we expect
        assert len(records) == 1, f'{1} record should have been processed but {len(records)} were found'

        record = records[0]
        for i in range(0, len(column_names)):
            # assert that the value contained is what we expect
            assert expected_record[i][0] == record.field[column_names[i]], \
                f'The value of the field {column_names[i]} should have been {expected_record[i][0]},' \
                f' but it is {record.field[column_names[i]]}'
            # and that the type is what we expect
            assert expected_record[i][1] == record.field[column_names[i]].type, \
                f'The type of the field {column_names[i]} should have been {expected_record[i][1]},' \
                f' but it is {record.field[column_names[i]].type}'
    finally:
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        drop_table(engine, table_name)
        engine.dispose()


@pytest.mark.parametrize('read_values_as_string', [True, False])
def test_data_types_null_values(sdc_builder, sdc_executor, snowflake, read_values_as_string):
    """
    Similar to test_data_types, but making every inserted data type be null. Ensure data is read properly.
    Uses Snowflake Bulk Origin to read from the table using the same stage.
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # ID column is so that commenting all types except one still works
    columns = [{'name': 'ID', 'type': 'NUMBER'}]
    not_null_column_name = ['ID']
    not_null_inserted_record = '[(1)]'
    if read_values_as_string:
        expected_record = [('1', 'STRING')]
    else:
        expected_record = [(1, 'LONG')]

    for i in range(0, len(DATA_TYPES_SNOWFLAKE)):
        db_type = DATA_TYPES_SNOWFLAKE[i][0]
        if Version(sdc_builder.version) < Version('5.8.0') and db_type == 'GEOGRAPHY':
            continue
        expected_data = None
        if read_values_as_string:
            expected_type = 'STRING'
        else:
            expected_type = DATA_TYPES_SNOWFLAKE[i][2]
        columns.append({'name': f'VALUE_{i}', 'type': f'{db_type}'})
        expected_record.append((expected_data, expected_type))

    column_names, column_definitions, primary_keys_clause = get_columns_information(columns)
    create_table(engine, table_name, column_definitions, primary_keys_clause)
    insert_values(engine, table_name, not_null_column_name, not_null_inserted_record)

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location='INTERNAL')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location='INTERNAL',
                                    snowflake_stage_name=stage_name,
                                    read_values_as_string=read_values_as_string)

    if Version(sdc_builder.version) >= Version('5.8.0'):
        snowflake_origin.set_attributes(table_or_view_configuration=[{'inclusionPattern': table_name}])
    else:
        snowflake_origin.set_attributes(table_config=[{'inclusionPattern': table_name}])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        records = wiretap.output_records

        # check that the number of records is equal to what we expect
        assert len(records) == 1, f'{1} record should have been processed but {len(records)} were found'

        record = records[0]
        for i in range(0, len(column_names)):
            # assert that the value contained is what we expect
            assert expected_record[i][0] == record.field[column_names[i]], \
                f'The value of the field {column_names[i]} should have been {expected_record[i][0]},' \
                f' but it is {record.field[column_names[i]]}'
            # and that the type is what we expect
            assert expected_record[i][1] == record.field[column_names[i]].type, \
                f'The type of the field {column_names[i]} should have been {expected_record[i][1]},' \
                f' but it is {record.field[column_names[i]].type}'
    finally:
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        drop_table(engine, table_name)
        engine.dispose()


@pytest.mark.parametrize('database_name_category,table_name', DATABASE_OBJECTS_NAMES,
                         ids=[i[0] for i in DATABASE_OBJECTS_NAMES])
def test_object_names(sdc_builder, sdc_executor, snowflake, database_name_category, table_name):
    """
    Tests that the Snowflake Bulk Origin can process different valid object names.

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location='INTERNAL')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location='INTERNAL',
                                    snowflake_stage_name=stage_name)

    if Version(sdc_builder.version) >= Version('5.8.0'):
        snowflake_origin.set_attributes(table_or_view_configuration=[{'inclusionPattern': table_name}])
    else:
        snowflake_origin.set_attributes(table_config=[{'inclusionPattern': table_name}])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        column_names = create_table_and_insert_values(engine, table_name, DEFAULT_COLUMNS, DEFAULT_RECORDS)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        records = wiretap.output_records
        expected_records = DEFAULT_RECORDS

        # Check that the number of records is equal to what we expect
        assert len(records) == len(expected_records), \
            f'{len(expected_records)} records should have been processed but only {len(records)} were found'

        for record, expected_record in zip(records, expected_records):
            assert record.header.values[DATABASE_RECORD_HEADER_ATTRIBUTE_NAME] == DEFAULT_DATABASE
            assert record.header.values[SCHEMA_RECORD_HEADER_ATTRIBUTE_NAME] == DEFAULT_SCHEMA
            assert record.header.values[TABLE_RECORD_HEADER_ATTRIBUTE_NAME] == table_name

            for i in range(0, len(column_names)):
                # Check that each row has the needed columns ...
                assert column_names[i] in record.field, f'The record should have a column named {column_names[i]}'
                # ... and that the value contained is what we expect
                assert expected_record[i] == record.field[column_names[i]], \
                    f'The value of the field {column_names[i]} should have been {expected_record[i]},' \
                    f' but it is {record.field[column_names[i]]}'
    finally:
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        drop_table(engine, table_name)
        engine.dispose()


@pytest.mark.parametrize('stage_location', ["INTERNAL", "AWS_S3", "GCS", "BLOB_STORAGE"])
@pytest.mark.parametrize('compressed_file', [True, False])
def test_empty_table(sdc_builder, sdc_executor, snowflake, stage_location, compressed_file):
    """
    Tests that the Snowflake Bulk Origin correctly process empty tables, using compressed files or not.

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=stage_location)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location=stage_location,
                                    snowflake_stage_name=stage_name,
                                    compressed_file=compressed_file)

    if Version(sdc_builder.version) >= Version('5.8.0'):
        snowflake_origin.set_attributes(table_or_view_configuration=[{'inclusionPattern': table_name}])
    else:
        snowflake_origin.set_attributes(table_config=[{'inclusionPattern': table_name}])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        column_names, column_definitions, primary_keys_clause = get_columns_information(DEFAULT_COLUMNS)
        create_table(engine, table_name, column_definitions, primary_keys_clause)
        # we do not insert any records in the table

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        records = wiretap.output_records

        assert len(records) == 0, f'0 records should have been processed but {len(records)} were found'

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
    finally:
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        drop_table(engine, table_name)
        engine.dispose()


@pytest.mark.parametrize('read_values_as_string', [True, False])
def test_null_values(sdc_builder, sdc_executor, snowflake, read_values_as_string):
    """
    Tests that the Snowflake Bulk Origin correctly process empty tables, using compressed files or not.

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location='INTERNAL')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location='INTERNAL',
                                    snowflake_stage_name=stage_name,
                                    read_values_as_string=read_values_as_string)

    if Version(sdc_builder.version) >= Version('5.8.0'):
        snowflake_origin.set_attributes(table_or_view_configuration=[{'inclusionPattern': table_name}])
    else:
        snowflake_origin.set_attributes(table_config=[{'inclusionPattern': table_name}])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    try:
        columns = [{'name': 'ID', 'type': 'STRING', 'primary_key': True},
                   {'name': 'NAME', 'type': 'STRING', 'primary_key': False},
                   {'name': 'NULLABLE', 'type': 'STRING', 'primary_key': False}]

        column_names, column_definitions, primary_keys_clause = get_columns_information(columns)
        create_table(engine, table_name, column_definitions, primary_keys_clause)
        # we do not insert anything for NULLABLE column (3rd column)
        inserted_records = [('nullable', 'Joaquin')]
        insert_values(engine, table_name, column_names[:-1], inserted_records)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        records = wiretap.output_records
        # we expect the records we insert and a NULL value in the last one
        expected_records = [inserted_records[0] + (None,)]

        # Check that the number of records is equal to what we expect
        assert len(records) == len(expected_records), \
            f'{len(expected_records)} records should have been processed but only {len(records)} were found'

        for record, expected_record in zip(records, expected_records):
            assert record.header.values[DATABASE_RECORD_HEADER_ATTRIBUTE_NAME] == DEFAULT_DATABASE
            assert record.header.values[SCHEMA_RECORD_HEADER_ATTRIBUTE_NAME] == DEFAULT_SCHEMA
            assert record.header.values[TABLE_RECORD_HEADER_ATTRIBUTE_NAME] == table_name

            for i in range(0, len(column_names)):
                # Check that each row has the needed columns ...
                assert column_names[i] in record.field, f'The record should have a column named {column_names[i]}'
                # ... and that the value contained is what we expect
                assert expected_record[i] == record.field[column_names[i]], \
                    f'The value of the field {column_names[i]} should have been {expected_record[i]},' \
                    f' but it is {record.field[column_names[i]]}'
    finally:
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        drop_table(engine, table_name)
        engine.dispose()


def test_dataflow_events(sdc_builder, sdc_executor, snowflake):
    pytest.skip("No events supported in snowflake bulk source at this time.")


def test_multiple_batches(sdc_builder, sdc_executor, snowflake):
    pytest.skip("Test not reimplemented as stage test_big_amounts_of_records already tests this and also sending "
                "incomplete batches.")


def test_multiple_threads(sdc_builder, sdc_executor, snowflake):
    pytest.skip("Test not reimplemented as stage test_big_amounts_of_records already tests this.")
