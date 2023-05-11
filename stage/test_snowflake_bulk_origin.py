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
import json
import logging
import string
from random import randrange

from streamsets.testframework.markers import snowflake, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Roger Federer'},
    {'id': 2, 'name': 'Rafael Nadal'},
    {'id': 3, 'name': 'Dominic Thiem'}
]

# AWS S3 bucket in case of AWS or Azure blob storage container in case of Azure.
STORAGE_BUCKET_CONTAINER = 'snowflake'

def _create_table(sdc_builder, sdc_executor, snowflake, stage_location, stage_name, table_name, data):
    table = snowflake.create_table(table_name.lower())

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join(json.dumps(row) for row in data)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(stage_location=stage_location,
                                         purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name)
    dev_raw_data_source >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

    return table

@snowflake
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('stage_location', ["INTERNAL", "AWS_S3", "GCS", "AZURE"])
@pytest.mark.parametrize('compressed_file', [True, False])
def test_basic(sdc_builder, sdc_executor, snowflake, stage_location, compressed_file):
    """
    Create a snowflake table and stage and populates with some data.
    Uses Snowflake Bulk Origin to read from the table using the same stage.
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=stage_location)

    table = _create_table(sdc_builder, sdc_executor, snowflake, stage_location, stage_name, table_name, ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage('Snowflake Bulk', type='origin')
    snowflake_origin.set_attributes(stage_location=stage_location,
                                    snowflake_stage_name=stage_name,
                                    table=table_name,
                                    compressed_file=compressed_file,
                                    data_format="DELIMITED")

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        records = wiretap.output_records
        assert len(records) == len(ROWS_IN_DATABASE), "Wrong number of records processed"
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('stage_location', ["INTERNAL"])
def test_where(sdc_builder, sdc_executor, snowflake, stage_location):
    """
    Create a snowflake table and stage and populates with some data.
    Uses Snowflake Bulk Origin to read from the table using the same stage.
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=stage_location)

    table = _create_table(sdc_builder, sdc_executor, snowflake, stage_location, stage_name, table_name, ROWS_IN_DATABASE)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage('Snowflake Bulk', type='origin')
    snowflake_origin.set_attributes(stage_location=stage_location,
                                    snowflake_stage_name=stage_name,
                                    table=table_name,
                                    where_clause="id = 2",
                                    data_format="DELIMITED")

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        records = wiretap.output_records
        assert len(records) == 1, "Wrong number of records processed"
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()

@snowflake
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('stage_location', ["INTERNAL"])
@pytest.mark.parametrize('number_of_tables', [3])
@pytest.mark.parametrize('exclusion', [True])  # exclude one random table
def test_multiple_tables(sdc_builder, sdc_executor, snowflake, stage_location, number_of_tables, exclusion):
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=stage_location)

    table_list = []
    tables = []
    excluded_table_id = randrange(number_of_tables)
    excluded_table = ""
    table_group = get_random_string(string.ascii_uppercase, 5)
    for table_id in range(number_of_tables):
        # Create a table and stage in Snowflake.
        table_name = f'STF_TABLE_{table_group}_{get_random_string(string.ascii_uppercase, 5)}'
        table_list.append({"pattern": table_name.replace('STF_TABLE_', '%')})
        if exclusion and excluded_table_id == table_id:
            excluded_table = table_name.replace('STF_TABLE_', '^.*')
        table = _create_table(sdc_builder, sdc_executor, snowflake, stage_location, stage_name, table_name, ROWS_IN_DATABASE)
        tables.append(table)

    # Add all tables twice, SDC should only take each table only once
    table_list.append({"pattern": f'STF_TABLE_{table_group}%'})

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage('Snowflake Bulk', type='origin')
    snowflake_origin.set_attributes(stage_location=stage_location,
                                    snowflake_stage_name=stage_name,
                                    enable_table_name_list=True,
                                    table_list=table_list,
                                    table_exclusion_pattern=excluded_table,
                                    data_format="DELIMITED")

    wiretap = pipeline_builder.add_wiretap()
    snowflake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    total_tables = (number_of_tables -1) if exclusion else number_of_tables
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        records = wiretap.output_records
        assert len(records) == total_tables * len(ROWS_IN_DATABASE), \
            "Wrong number of records processed"
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        for table in tables:
            table.drop(engine)
        engine.dispose()


@snowflake
@sdc_min_version('5.6.0')
@pytest.mark.parametrize('stage_location', ["INTERNAL", "AWS_S3", "GCS", "AZURE"])
@pytest.mark.parametrize('records_to_be_generated', [500000])
def test_large(sdc_builder, sdc_executor, snowflake, stage_location, records_to_be_generated):
    """
    Create a snowflake table and stage and populates with large random data forcing to stage data in several files.
    Uses Snowflake Bulk Origin to read from the table using the same stage.
    """
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=stage_location)

    table = snowflake.create_table(table_name.lower())

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(fields_to_generate=[
                                          {
                                              "type": "INTEGER",
                                              "precision": 10,
                                              "scale": 2,
                                              "fieldAttributes": [],
                                              "field": "id"
                                          },
                                          {
                                              "type": "BOOK_AUTHOR",
                                              "precision": 10,
                                              "scale": 2,
                                              "fieldAttributes": [],
                                              "field": "name"
                                          }
                                      ],
                                      delay_between_batches=0,
                                      records_to_be_generated=records_to_be_generated,
                                      batch_size=50000)
    snowflake_destination = pipeline_builder.add_stage('Snowflake', type='destination')
    snowflake_destination.set_attributes(stage_location=stage_location,
                                         purge_stage_file_after_ingesting=True,
                                         snowflake_stage_name=stage_name,
                                         table=table_name)
    dev_data_generator >> snowflake_destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    snowflake_origin = pipeline_builder.add_stage('Snowflake Bulk', type='origin')
    snowflake_origin.set_attributes(stage_location=stage_location,
                                    snowflake_stage_name=stage_name,
                                    table=table_name,
                                    data_format="DELIMITED")

    trash = pipeline_builder.add_stage('Trash')
    snowflake_origin >> trash

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished(timeout_sec=600)
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == records_to_be_generated,\
            "Wrong number of records processed"
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.delete_staged_files(storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        table.drop(engine)
        engine.dispose()
