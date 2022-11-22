# Copyright (c) 2021 StreamSets Inc.

import json
import logging
import os
import pytest
import string
import tempfile
from streamsets.testframework.markers import snowflake, sdc_enterprise_lib_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Iron Man'},
    {'id': 2, 'name': 'Captain America'},
    {'id': 3, 'name': 'Spidey'}
]

# AWS S3 bucket in case of AWS or Azure blob storage container in case of Azure.
STORAGE_BUCKET_CONTAINER = 'snowflake'


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.9.0'})
@pytest.mark.parametrize('with_table', [True, False])
@pytest.mark.parametrize('with_el', [True, False])
@pytest.mark.parametrize('with_valid_query', [True, False])
def test_snowflake_executor_basic(sdc_builder, sdc_executor, snowflake, with_el, with_table, with_valid_query):

    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data='{"value": 42}')
    dev_raw_data_source.stop_after_first_batch = True

    snowflake_executor = pipeline_builder.add_stage('Snowflake', type='executor')

    if with_valid_query:
        query = "insert into"
    else:
        query = "this query is obviously invalid"

    if with_el:
        el = "${record:value('/value')}"
        query_str = f"{query} STF_DB.STF_SCHEMA.{table_name} values ('test', {el})"
    else:
        query_str = f"{query} STF_DB.STF_SCHEMA.{table_name} values ('test', 42)"

    snowflake_executor.set_attributes(sql_queries=[query_str])

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> snowflake_executor
    snowflake_executor >= wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    if with_table:
        engine.execute(f"create or replace table STF_DB.STF_SCHEMA.{table_name} (id string, value int)")
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        if with_table and with_valid_query:
            result = engine.execute(f"Select $1, cast($2 as integer) from STF_DB.STF_SCHEMA.{table_name}")
            data_from_database = result.fetchall()
            result.close()
            assert data_from_database == [('test', 42)]
        else:
            assert len(wiretap.error_records) == 1, \
                'This execution mode was expected to produce a single error record with error code QUERY_EXECUTOR_001'
            assert wiretap.error_records[0].header['errorCode'] == 'QUERY_EXECUTOR_001', \
                 'This execution mode was expected to produce a single error record with error code QUERY_EXECUTOR_001'
            assert len(wiretap.output_records) == 1, \
                'This execution mode was expected to produce a single output (event) record of type "failed-query"'
            assert wiretap.output_records[0].header.values['sdc.event.type'] == 'failed-query', \
                 'This execution mode was expected to produce a single output (event) record of type "failed-query"'
    finally:
        if with_table:
            engine.execute(f"drop table STF_DB.STF_SCHEMA.{table_name}")
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.9.0'})
@pytest.mark.parametrize('with_table', [True, False])
@pytest.mark.parametrize('with_stage', [True, False])
@pytest.mark.parametrize('with_valid_query', [True, False])
def test_snowflake_executor_with_file_uploader(sdc_builder, sdc_executor, snowflake, with_table, with_stage, with_valid_query):
    if snowflake.sdc_stage_configurations['com_streamsets_pipeline_stage_destination_snowflake_SnowflakeDTarget'][
        'config.stageLocation'] is not 'INTERNAL':
        pytest.skip('Snowflake File Uploader is only intended to run against Snowflake Internal Stages')
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    if with_stage:
        query_stage = stage_name
    else:
        query_stage = "DUMMY_STAGE"

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    data = ("1,Iron Man\n"
            "2,Captain America\n"
            "3,Spidey\n")

    work_dir = _prepare_work_dir(sdc_executor, data)

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')

    snowflake_file_uploader.set_attributes(stage=stage_name)

    snowflake_executor = pipeline_builder.add_stage('Snowflake', type='executor')

    if with_valid_query:
        query_str = f"copy into STF_DB.STF_SCHEMA.{table_name} from @STF_DB.STF_SCHEMA.{query_stage}"
    else:
        query_str = f"this query is not valid STF_DB.STF_SCHEMA.{table_name} from @STF_DB.STF_SCHEMA.{query_stage}"

    snowflake_executor.set_attributes(sql_queries=[query_str])

    wiretap = pipeline_builder.add_wiretap()

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished
    snowflake_file_uploader >= snowflake_executor
    snowflake_executor >= wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    if with_table:
        engine.execute(f"create or replace table STF_DB.STF_SCHEMA.{table_name} (id integer, name string)")
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()

        if with_table and with_valid_query and with_stage:
            result = engine.execute(f"Select cast($1 as integer), $2 from STF_DB.STF_SCHEMA.{table_name}")
            data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
            result.close()
            assert len(wiretap.output_records) == 1
            assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]
        else:
            assert len(wiretap.error_records) >= 1, \
                'This execution mode was expected to produce one error record for the executor and others in the pipeline finisher'
            assert wiretap.error_records[0].header['errorCode'] == 'QUERY_EXECUTOR_001', \
                'This execution mode was expected to produce error records, the first one with error code QUERY_EXECUTOR_001'
            assert len(wiretap.output_records) == 1, \
                'This execution mode was expected to produce a single output (event) record of type "failed-query"'
            assert wiretap.output_records[0].header.values['sdc.event.type'] == 'failed-query', \
                'This execution mode was expected to produce a single output (event) record of type "failed-query"'
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        if with_table:
            engine.execute(f"drop table STF_DB.STF_SCHEMA.{table_name}")
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.9.0'})
def test_snowflake_executor_multiple_queries(sdc_builder, sdc_executor, snowflake):
    if snowflake.sdc_stage_configurations['com_streamsets_pipeline_stage_destination_snowflake_SnowflakeDTarget'][
        'config.stageLocation'] is not 'INTERNAL':
        pytest.skip('Snowflake File Uploader is only intended to run against Snowflake Internal Stages')
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    data = ("1,Iron Man\n"
            "2,Captain America\n"
            "3,Spidey\n")

    work_dir = _prepare_work_dir(sdc_executor, data)

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')

    snowflake_file_uploader.set_attributes(stage=stage_name)

    snowflake_executor = pipeline_builder.add_stage('Snowflake', type='executor')

    query_str1 = f"create or replace table STF_DB.STF_SCHEMA.{table_name} (id integer, name string)"
    query_str2 = f"copy into STF_DB.STF_SCHEMA.{table_name} from @STF_DB.STF_SCHEMA.{stage_name}"

    snowflake_executor.set_attributes(sql_queries=[query_str1, query_str2])

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished
    snowflake_file_uploader >= snowflake_executor

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select cast($1 as integer), $2 from STF_DB.STF_SCHEMA.{table_name}")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        engine.execute(f"drop table STF_DB.STF_SCHEMA.{table_name}")
        engine.dispose()

def _prepare_work_dir(sdc_executor, data, data2=None, file_extension='csv'):
    """Create work directory, insert test data, return the work directory."""
    work_dir = os.path.join(tempfile.gettempdir(), get_random_string())
    sdc_executor.execute_shell(f'mkdir -p {work_dir}')
    sdc_executor.write_file(os.path.join(work_dir, f'input.{file_extension}'), data)
    if data2:
        sdc_executor.write_file(os.path.join(work_dir, f'input2.{file_extension}'), data2)
    return work_dir
