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

import json
import logging
import os
import re
import string
import tempfile

import pytest
from streamsets.sdk.exceptions import StartError, StartingError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import snowflake, sdc_enterprise_lib_min_version, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Roger Federer'},
    {'id': 2, 'name': 'Rafael Nadal'},
    {'id': 3, 'name': 'Dominic Thiem'}
]
ROWS_IN_DATABASE_EXTRA = [
    {'id': 4, 'name': 'Juan Del Potro'},
    {'id': 5, 'name': 'Guillermo Vilas'},
    {'id': 6, 'name': 'Jose Luis Clerc'}
]

# AWS S3 bucket in case of AWS or Azure blob storage container in case of Azure.
STORAGE_BUCKET_CONTAINER = 'snowflake'

DATA = ("1,Roger Federer\n"
        "2,Rafael Nadal\n"
        "3,Dominic Thiem\n")


@snowflake
@pytest.mark.parametrize('use_temporary_directory_path', [True, False])
@sdc_enterprise_lib_min_version({'snowflake': '1.8.0'})
def test_basic(sdc_builder, sdc_executor, snowflake, use_temporary_directory_path):
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    work_dir = _prepare_work_dir(sdc_executor, DATA)
    temporary_directory_path = ''

    if use_temporary_directory_path:
        temporary_directory_path = f'{tempfile.gettempdir()}/{get_random_string()}'

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')

    snowflake_file_uploader.set_attributes(stage=stage_name, temporary_directory_path=temporary_directory_path)

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select cast(t.$1 as integer), t.$2 from @{stage_name} t")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.8.0'})
def test_basic_with_el(sdc_builder, sdc_executor, snowflake):
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    work_dir = _prepare_work_dir(sdc_executor, DATA, file_name=stage_name)

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')

    snowflake_file_uploader.set_attributes(
        stage="${file:removeExtension(file:fileName(record:value('/fileInfo/file')))}",
        auto_create_stage=True)

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select cast(t.$1 as integer), t.$2 from @\"{stage_name}\" t")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.8.0'})
def test_multiple_files_semicolon(sdc_builder, sdc_executor, snowflake):
    stage = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    data = ("1;Roger Federer\n" 
            "2;Rafael Nadal\n" 
            "3;Dominic Thiem\n")
    data2 = ("4;Juan Del Potro\n" 
             "5;Guillermo Vilas\n" 
             "6;Jose Luis Clerc\n")

    work_dir = _prepare_work_dir(sdc_executor, data, data2)

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')

    snowflake_file_uploader.set_attributes(stage=stage,
                                           auto_create_stage=True,
                                           column_separator=';')

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select cast(t.$1 as integer), t.$2 from @{stage} t")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE + ROWS_IN_DATABASE_EXTRA]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.8.0'})
def test_produce_events(sdc_builder, sdc_executor, snowflake):
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    regex = "^/tmp/streamsets-snowflake-uploader-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/$"

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    work_dir = _prepare_work_dir(sdc_executor, DATA)

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')

    snowflake_file_uploader.set_attributes(stage=stage_name)

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    wiretap = pipeline_builder.add_wiretap()

    origin >> snowflake_file_uploader >= wiretap.destination
    origin >= pipeline_finished

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select cast(t.$1 as integer), t.$2 from @{stage_name} t")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]
        assert len(wiretap.output_records) == 1

        if Version(sdc_executor.version) >= Version('5.5.0'):
            assert re.match(regex, str(wiretap.output_records[0].field['filepath']))
        assert wiretap.output_records[0].field['filename'] == 'input.csv'
        assert wiretap.output_records[0].field['length'] == 47

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.8.0'})
def test_stop_and_resume(sdc_builder, sdc_executor, snowflake):
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    work_dir = _prepare_work_dir(sdc_executor, DATA)

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')
    snowflake_file_uploader.set_attributes(stage=stage_name)

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select cast(t.$1 as integer), t.$2 from @{stage_name} t")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]

        data2 = "4,Juan Del Potro\n" \
                "5,Guillermo Vilas\n" \
                "6,Jose Luis Clerc\n"
        sdc_executor.write_file(os.path.join(work_dir, 'input2.csv'), data2)
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select cast(t.$1 as integer), t.$2 from @{stage_name} t")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE + ROWS_IN_DATABASE_EXTRA]

    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.8.0'})
def test_format_json(sdc_builder, sdc_executor, snowflake):
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Storage container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    data = json.dumps(ROWS_IN_DATABASE)

    work_dir = _prepare_work_dir(sdc_executor=sdc_executor, data=data, file_extension='json')

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.json'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')

    snowflake_file_uploader.set_attributes(data_format='JSON',
                                           stage=stage_name,
                                           auto_create_stage=True,
                                           strip_outer_array=True,
                                           values_representing_null=[''])

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select * from @{stage_name} t")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        database_information = []

        for row in data_from_database:
            for col, value in row.items():
                database_information.append(value)
        assert [json.loads(x) for x in database_information] == ROWS_IN_DATABASE
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        engine.dispose()


@snowflake
@sdc_enterprise_lib_min_version({'snowflake': '1.12.0'})
def test_purge_temporary_files(sdc_builder, sdc_executor, snowflake):
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    data = ("1,Roger Federer\n"
            "2,Rafael Nadal\n"
            "3,Dominic Thiem\n")

    work_dir = _prepare_work_dir(sdc_executor, data)

    origin = pipeline_builder.add_stage('Directory', type='origin').set_attributes(file_name_pattern='*.csv',
                                                                                   data_format='WHOLE_FILE',
                                                                                   files_directory=work_dir)

    snowflake_directory = f'{tempfile.gettempdir()}/{get_random_string()}'

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader').set_attributes(stage=stage_name,
                                                                                                   temporary_directory_path=snowflake_directory)

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select cast(t.$1 as integer), t.$2 from @{stage_name} t")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]

        files_in_tmp = int(sdc_executor.execute_shell(f'ls {snowflake_directory}/*/*.csv | wc -l').stdout)
        assert 0 == files_in_tmp
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        engine.dispose()


@snowflake
@sdc_min_version('5.10.0')
@pytest.mark.parametrize('wrong_object', ["database", "schema"])
def test_wrong_database_schema(sdc_builder, sdc_executor, snowflake, wrong_object):
    """Verify that the Snowflake File Uploader uses the configured databases and schema if available"""
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    work_dir = _prepare_work_dir(sdc_executor, DATA)
    temporary_directory_path = ''

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')

    snowflake_file_uploader.set_attributes(stage=stage_name, temporary_directory_path=temporary_directory_path)

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    if wrong_object == 'database':
        snowflake_file_uploader.stage_database = "WRONG_OBJ"
    elif wrong_object == 'schema':
        snowflake_file_uploader.stage_schema = "WRONG_OBJ"

    sdc_executor.add_pipeline(pipeline)
    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        pytest.fail("Pipeline should have failed validation with SNOWFLAKE_16, wrong schema or database")
    except (StartError, StartingError) as e:
        assert 'SNOWFLAKE_16' in e.message
        assert 'use {0} "{1}"'.format(wrong_object, "WRONG_OBJ") in e.message
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        engine.dispose()


@snowflake
@sdc_min_version('5.10.0')
@pytest.mark.parametrize('private_key_location', ['KEYPAIR', 'KEYPAIR_CONTENT'])
def test_key_pair_authentication(sdc_builder, sdc_executor, snowflake, private_key_location):
    """
        Similar to test_basic, but using Key Pair authentication.
    """
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'

    # The following is path inside a bucket in case of AWS S3 or
    # path inside container in case of Azure Blob Stoarge container.
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path)

    # Build the pipeline with created Snowflake entities.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    work_dir = _prepare_work_dir(sdc_executor, DATA)
    temporary_directory_path = f'{tempfile.gettempdir()}/{get_random_string()}'

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    snowflake_file_uploader = pipeline_builder.add_stage('Snowflake File Uploader')

    snowflake_file_uploader.set_attributes(stage=stage_name,
                                           temporary_directory_path=temporary_directory_path,
                                           authentication_method=private_key_location,
                                           private_key_path=snowflake.private_key_file_path,
                                           private_key_content=snowflake.private_key_file_contents,
                                           private_key_password=snowflake.private_key_passphrase)

    pipeline_finished = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> snowflake_file_uploader
    origin >= pipeline_finished

    pipeline = pipeline_builder.build().configure_for_environment(snowflake)
    sdc_executor.add_pipeline(pipeline)

    engine = snowflake.engine
    try:
        sdc_executor.start_pipeline(pipeline=pipeline).wait_for_finished()
        result = engine.execute(f"Select cast(t.$1 as integer), t.$2 from @{stage_name} t")
        data_from_database = sorted(result.fetchall(), key=lambda row: row[0])  # order by id
        result.close()
        assert data_from_database == [(row['id'], row['name']) for row in ROWS_IN_DATABASE]
    finally:
        logger.debug('Staged files will be deleted from %s ...', storage_path)
        snowflake.drop_entities(stage_name=stage_name)
        engine.dispose()


def _prepare_work_dir(sdc_executor, data, data2=None, file_extension='csv', file_name='input'):
    """Create work directory, insert test data, return the work directory."""
    work_dir = os.path.join(tempfile.gettempdir(), get_random_string())
    sdc_executor.execute_shell(f'mkdir -p {work_dir}')
    sdc_executor.write_file(os.path.join(work_dir, f'{file_name}.{file_extension}'), data)
    if data2:
        sdc_executor.write_file(os.path.join(work_dir, f'input2.{file_extension}'), data2)
    return work_dir
