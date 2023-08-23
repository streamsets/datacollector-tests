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

import pytest
import logging
import string

from streamsets.testframework.markers import snowflake, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

BULK_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_snowflake_SnowflakeBulkDSource'

pytestmark = [snowflake, sdc_min_version('5.7.0')]

DEFAULT_DATABASE = 'STF_DB'
DEFAULT_SCHEMA = 'STF_SCHEMA'

DEFAULT_COLUMNS = [{'name': 'ID', 'type': 'NUMBER', 'primary_key': True},
                   {'name': 'NAME', 'type': 'STRING', 'primary_key': False}]

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


@pytest.mark.parametrize('num_tables, parallel_transfers, num_records, batch_size, reader_threads, processor_threads', [
    (1, 1, 100_000, 1_000, 1, 1),
    (1, 1, 100_000, 1_000, 5, 5),
    (10, 20, 1_000_000, 10_000, 1, 1)
])
def test_multithread_batch_size(sdc_builder, sdc_executor, snowflake, num_tables, parallel_transfers, num_records,
                                batch_size, reader_threads, processor_threads):
    """
    Performance test with large amounts of rows and with several tables at the same time.
    We just test it for INTERNAL.

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

        # benchmark the pipeline
        sdc_executor.benchmark_pipeline(pipeline, record_count=num_records)
    finally:
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        for table in tables:
            drop_table(engine, table)
        engine.dispose()
