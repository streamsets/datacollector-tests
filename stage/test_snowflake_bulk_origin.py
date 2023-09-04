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

import math
import pytest
import logging
import string

from streamsets.testframework.markers import snowflake, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

BULK_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_snowflake_SnowflakeBulkDSource'

pytestmark = [snowflake, sdc_min_version('5.7.0')]

DATABASE_RECORD_HEADER_ATTRIBUTE_NAME = 'snowflake.db'
SCHEMA_RECORD_HEADER_ATTRIBUTE_NAME = 'snowflake.schema'
TABLE_RECORD_HEADER_ATTRIBUTE_NAME = 'snowflake.table'

DEFAULT_DATABASE = 'STF_DB'
DEFAULT_SCHEMA = 'STF_SCHEMA'

DEFAULT_COLUMNS = [{'name': 'ID', 'type': 'NUMBER', 'primary_key': True},
                   {'name': 'NAME', 'type': 'STRING', 'primary_key': False}]

DEFAULT_RECORDS = [(1, 'Roger Federer'), (2, 'Rafael Nadal'), (3, 'Dominic Thiem')]

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


@pytest.mark.parametrize('stage_location', ["INTERNAL", "AWS_S3", "GCS", "BLOB_STORAGE"])
@pytest.mark.parametrize('compressed_file', [True, False])
def test_basic(sdc_builder, sdc_executor, snowflake, stage_location, compressed_file):
    """
    Tests that the Snowflake Bulk Origin can be used to read compressed and un-compressed data using different
    stage locations.

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
                                    table_config=[{'inclusionPattern': table_name}],
                                    compressed_file=compressed_file)

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


@pytest.mark.parametrize('where_clause, expected_records', [
    ("id = 2", [DEFAULT_RECORDS[1]]),
    ("id != 2", [DEFAULT_RECORDS[0], DEFAULT_RECORDS[2]]),
    ("name = 'Roger Federer'", [DEFAULT_RECORDS[0]]),
    ("name != 'Pikachu'", DEFAULT_RECORDS),
])
def test_where_clause(sdc_builder, sdc_executor, snowflake, where_clause, expected_records):
    """
    Tests that the Where Clause can be used to filter out some rows from the tables included.

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location="INTERNAL")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location="INTERNAL",
                                    snowflake_stage_name=stage_name,
                                    table_config=[{
                                        'inclusionPattern': table_name,
                                        'whereClause': where_clause
                                    }])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        column_names = create_table_and_insert_values(engine, table_name, DEFAULT_COLUMNS, DEFAULT_RECORDS)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        records = wiretap.output_records

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


@pytest.mark.parametrize('number_of_tables, number_of_excluded_tables', [(3, 1), (10,  4), (2, 2)])
def test_exclusion_pattern(sdc_builder, sdc_executor, snowflake, number_of_tables, number_of_excluded_tables):
    """
    Tests that the Exclusion Pattern can be used to filter out some tables.

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    table_names_prefix = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    excluded_tables_mark = 'exclude'

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location="INTERNAL")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location="INTERNAL",
                                    snowflake_stage_name=stage_name,
                                    table_config=[{
                                        'inclusionPattern': f'{table_names_prefix}_%',
                                        'exclusionPattern': f'(\\S)*_{excluded_tables_mark}_(\\S)*'
                                    }])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    included_tables = []
    excluded_tables = []
    try:
        for table_id in range(number_of_tables):
            # Create a table and stage in Snowflake. The first #number_of_excluded_tables tables will be excluded
            if table_id < number_of_excluded_tables:
                table_name = f'{table_names_prefix}_{excluded_tables_mark}_' \
                             f'{get_random_string(string.ascii_uppercase, 5)}'
                excluded_tables.append(
                    (table_name, create_table_and_insert_values(engine, table_name, DEFAULT_COLUMNS, DEFAULT_RECORDS))
                )
            else:
                table_name = f'{table_names_prefix}_{get_random_string(string.ascii_uppercase, 5)}'
                included_tables.append(
                    (table_name, create_table_and_insert_values(engine, table_name, DEFAULT_COLUMNS, DEFAULT_RECORDS))
                )

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        records = wiretap.output_records
        expected_records_per_table = DEFAULT_RECORDS
        expected_records = expected_records_per_table * (number_of_tables - number_of_excluded_tables)

        # Check that the number of records is equal to what we expect
        assert len(records) == len(expected_records), \
            f'{len(expected_records)} records should have been processed but only {len(records)} were found'

        # Check that none of the records belongs to a table that should have been excluded
        for record in records:
            assert excluded_tables_mark not in record.header.values[TABLE_RECORD_HEADER_ATTRIBUTE_NAME], \
                f'The table {record.header.values[TABLE_RECORD_HEADER_ATTRIBUTE_NAME]} should have been excluded'
    finally:
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        for table_name, column_names in (included_tables + excluded_tables):
            drop_table(engine, table_name)
        engine.dispose()


def test_multiple_table_config_patterns(sdc_builder, sdc_executor, snowflake):
    """
    Tests that the multiple inclusion and exclusion patterns and where clauses can be used to define which tables should
    be used and that they do not interfere between each other.

    The test uses 4 tables following 2 distinct name patterns ( 2 + 2 ) and a fifth table whose name matches one of the
    exclusion patterns. This last table will be added in a separate table config to check that the exclusion patterns do
    not affect other inclusion patterns. Out of each pair of tables one will be excluded and a different where clause
    will be applied to check the where clauses do not affect other table configs.

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    table_names_prefix = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    excluded_tables_mark = 'exclude'

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location="INTERNAL")

    table_1 = f'{table_names_prefix}_p1_{get_random_string(string.ascii_uppercase, 5)}'
    table_2 = f'{table_names_prefix}_p1_{excluded_tables_mark}_{get_random_string(string.ascii_uppercase, 5)}'
    table_3 = f'{table_names_prefix}_{excluded_tables_mark}_{get_random_string(string.ascii_uppercase, 5)}'
    table_4 = f'{table_names_prefix}_{get_random_string(string.ascii_uppercase, 5)}_p2'
    table_5 = f'{table_names_prefix}_{excluded_tables_mark}_{get_random_string(string.ascii_uppercase, 5)}_p2'
    tables = [table_1, table_2, table_3, table_4, table_5]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location="INTERNAL",
                                    snowflake_stage_name=stage_name,
                                    table_config=[{
                                        'inclusionPattern': f'{table_names_prefix}_p1_%',
                                        'exclusionPattern': f'(\\S)*_{excluded_tables_mark}_(\\S)*',
                                        'whereClause': "id = 1"
                                    }, {
                                        'inclusionPattern': table_3,
                                        'whereClause': "id = 2"
                                    }, {
                                        'inclusionPattern': f'{table_names_prefix}_%_p2',
                                        'exclusionPattern': f'(\\S)*_{excluded_tables_mark}_(\\S)*',
                                        'whereClause': "id = 3"
                                    }])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        create_table_and_insert_values(engine, table_1, DEFAULT_COLUMNS, DEFAULT_RECORDS)
        create_table_and_insert_values(engine, table_2, DEFAULT_COLUMNS, DEFAULT_RECORDS)
        create_table_and_insert_values(engine, table_3, DEFAULT_COLUMNS, DEFAULT_RECORDS)
        create_table_and_insert_values(engine, table_4, DEFAULT_COLUMNS, DEFAULT_RECORDS)
        create_table_and_insert_values(engine, table_5, DEFAULT_COLUMNS, DEFAULT_RECORDS)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        records = wiretap.output_records

        # We should find the default records but coming each from a different table as each table includes only 1 record
        assert len(records) == len(DEFAULT_RECORDS), \
            f'{len(DEFAULT_RECORDS)} records should have been processed but only {len(records)} were found'

        id_field = DEFAULT_COLUMNS[0]['name']

        def sort_records(record):
            return record.field[id_field]

        records.sort(key=sort_records)
        assert records[0].header.values[TABLE_RECORD_HEADER_ATTRIBUTE_NAME] == table_1
        assert records[0].field[id_field] == 1, \
            f'Table {table_1} should have the record with id 1 but id {records[0].field[id_field]} was found'

        assert records[1].header.values[TABLE_RECORD_HEADER_ATTRIBUTE_NAME] == table_3
        assert records[1].field[id_field] == 2, \
            f'Table {table_1} should have the record with id 2 but id {records[1].field[id_field]} was found'

        assert records[2].header.values[TABLE_RECORD_HEADER_ATTRIBUTE_NAME] == table_4
        assert records[2].field[id_field] == 3, \
            f'Table {table_1} should have the record with id 3 but id {records[2].field[id_field]} was found'

    finally:
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        for table_name in tables:
            drop_table(engine, table_name)
        engine.dispose()


def test_table_included_in_multiple_table_config_rows(sdc_builder, sdc_executor, snowflake):
    """
    Tests that when a table's configuration is added twice, the last configuration found overwrites the previous ones.
    The test adds twice the same table with different where clauses, and the checks that records returned belong to the
    seconds where clause.

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    table_names_prefix = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location="INTERNAL")

    table_name = f'{table_names_prefix}_1_{get_random_string(string.ascii_uppercase, 5)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location="INTERNAL",
                                    snowflake_stage_name=stage_name,
                                    table_config=[{
                                        'inclusionPattern': table_name,
                                        'whereClause': "id = 1"
                                    }, {
                                        'inclusionPattern': table_name,
                                        'whereClause': "id != 1"
                                    }])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        column_names = create_table_and_insert_values(engine, table_name, DEFAULT_COLUMNS, DEFAULT_RECORDS)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        records = wiretap.output_records

        expected_records = DEFAULT_RECORDS[1:]

        assert len(records) == len(expected_records), \
            f'{len(expected_records)} records should have been processed but only {len(records)} were found'

        for record, expected_record in zip(records, expected_records):
            for i in range(0, len(column_names)):
                # check that each row has the needed columns
                assert column_names[i] in record.field, f'The record should have a column named {column_names[i]}'
                # and that the value contained is what we expect
                assert expected_record[i] == record.field[column_names[i]], \
                    f'The value of the field {column_names[i]} should have been {expected_record[i]},' \
                    f' but it is {record.field[column_names[i]]}'

    finally:
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        drop_table(engine, table_name)
        engine.dispose()


@pytest.mark.parametrize('stage_location', ["INTERNAL", "AWS_S3", "GCS", "BLOB_STORAGE"])
@pytest.mark.parametrize('num_tables, num_records, batch_size', [(1, 500_000, 10000), (3, 100_000, 3333)])
def test_big_amounts_of_records(sdc_builder, sdc_executor, snowflake, stage_location, num_tables, num_records,
                                batch_size):
    """
    Tests that the stage can work with large amounts of rows and with several tables at the same time.

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    base_table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    tables = [f'{base_table_name}_{i}' for i in range(0, num_tables)]

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=stage_location)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location=stage_location,
                                    max_batch_size=batch_size,
                                    snowflake_stage_name=stage_name,
                                    table_config=[{'inclusionPattern': f'{base_table_name}%'}])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info(f'Creating {num_tables} tables with {num_records} per table...')
        column_names, column_definitions, primary_keys_clause = get_columns_information(DEFAULT_COLUMNS)
        records_to_insert = [(i, get_random_string(string.ascii_uppercase, 5)) for i in range(0, num_records)]
        for table in tables:
            create_table(engine, table, column_definitions, primary_keys_clause)
            insert_values(engine, table, column_names, records_to_insert, 10000)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        records = wiretap.output_records
        num_expected_records = num_records * num_tables
        # each thread creates its own batches, and we have 1 thread per table
        num_expected_batches = int(math.ceil(num_records / batch_size)) * num_tables

        # check that the number of records is equal to what we expect
        assert len(records) == num_expected_records, \
            f'{num_expected_records} records should have been processed but only {len(records)} were found'

        # and we also test the number of batches is what is to be expected
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchCount.counter').count == num_expected_batches
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == num_expected_records
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        for table in tables:
            drop_table(engine, table)
        engine.dispose()


@pytest.mark.parametrize('num_tables, num_records, batch_size, reader_threads, processor_threads',
                         [(1, 500_000, 10000, 5, 5), (3, 100_000, 10000, 2, 2)])
def test_stage_file_reader_multithreading(sdc_builder, sdc_executor, snowflake, num_tables, num_records, batch_size,
                                          reader_threads, processor_threads):
    """
    Test to ensure multithreading at downloading and processing staging files works as expected

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    base_table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    tables = [f'{base_table_name}_{i}' for i in range(0, num_tables)]

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location='INTERNAL')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location='INTERNAL',
                                    max_batch_size=batch_size,
                                    maximum_stage_file_reader_threads=reader_threads,
                                    maximum_stage_file_processing_threads=processor_threads,
                                    snowflake_stage_name=stage_name,
                                    table_config=[{'inclusionPattern': f'{base_table_name}%'}])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info(f'Creating {num_tables} tables with {num_records} per table...')
        column_names, column_definitions, primary_keys_clause = get_columns_information(DEFAULT_COLUMNS)
        records_to_insert = [(i, get_random_string(string.ascii_uppercase, 5)) for i in range(0, num_records)]
        for table in tables:
            create_table(engine, table, column_definitions, primary_keys_clause)
            insert_values(engine, table, column_names, records_to_insert, 10000)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)

        records = wiretap.output_records
        expected_records = records_to_insert * num_tables
        # each thread creates its own batches, and we have 1 thread per table
        num_expected_batches = int(math.ceil(num_records / batch_size)) * num_tables

        # check that the number of records is equal to what we expect
        assert len(records) == len(expected_records), \
            f'{len(expected_records)} records should have been processed but only {len(records)} were found'

        # and also data is correct
        for record, expected_record in zip(records, expected_records):
            for i in range(0, len(column_names)):
                # Check that each row has the needed columns ...
                assert column_names[i] in record.field, f'The record should have a column named {column_names[i]}'
                # ... and that the value contained is what we expect
                assert expected_record[i] == record.field[column_names[i]], \
                    f'The value of the field {column_names[i]} should have been {expected_record[i]},' \
                    f' but it is {record.field[column_names[i]]}'

        # and we also test the number of batches is what is to be expected
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchCount.counter').count == num_expected_batches
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == len(expected_records)
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        for table in tables:
            drop_table(engine, table)
        engine.dispose()


@pytest.mark.parametrize('parallel_transfers', [5, 10])
def test_parallel_transfers(sdc_builder, sdc_executor, snowflake, parallel_transfers):
    """
    Tests that the stage can work with large amounts of rows and with several tables at the same time,
    using parallel transfers and INTERNAL staging.

    The pipeline created looks like:
        Snowflake Bulk Origin >> Wiretap
    """
    num_tables = 5
    num_records = 50_000
    base_table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    tables = [f'{base_table_name}_{i}' for i in range(0, num_tables)]

    engine = snowflake.engine

    # Path inside a bucket in case of AWS S3 or path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location='INTERNAL')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage(name=BULK_STAGE_NAME)
    snowflake_origin.set_attributes(stage_location='INTERNAL',
                                    snowflake_stage_name=stage_name,
                                    parallel_transfers=parallel_transfers,
                                    table_config=[{'inclusionPattern': f'{base_table_name}%'}])

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info(f'Creating {num_tables} tables with {num_records} per table...')
        column_names, column_definitions, primary_keys_clause = get_columns_information(DEFAULT_COLUMNS)
        records_to_insert = [(i, get_random_string(string.ascii_uppercase, 5)) for i in range(0, num_records)]
        for table in tables:
            create_table(engine, table, column_definitions, primary_keys_clause)
            insert_values(engine, table, column_names, records_to_insert, 10000)

        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=300)

        records = wiretap.output_records
        num_expected_records = num_records * num_tables

        # check that the number of records is equal to what we expect
        assert len(records) == num_expected_records, \
            f'{num_expected_records} records should have been processed but only {len(records)} were found'
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        for table in tables:
            drop_table(engine, table)
        engine.dispose()
