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

import json
import logging
import os
import tempfile
from random import choice
from string import ascii_lowercase

import pytest
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string
from ..utils.utils_azure import AZURE_OBJECT_NAMES

logger = logging.getLogger(__name__)

pytestmark = [azure('datalake'), sdc_min_version('5.10.0')]

STAGE_NAME = 'com_streamsets_pipeline_stage_destination_client_blob_BlobStorageDTarget'

@pytest.fixture(autouse=True)
def storage_type_check(azure):
    if azure.storage_type == 'Storage':
        pytest.skip('Blob Storage tests require storage type to be of Gen2.')


def test_data_types(sdc_builder, sdc_executor):
    pytest.skip("Blob Storage stores objects without doing data type distinctions.")


def test_data_formats(sdc_builder, sdc_executor):
    pytest.skip("Blob Storage uses DataGeneratorService to generate DataFormats, we do not need to test every format here")


@sdc_min_version('6.2.0')
@pytest.mark.parametrize('use_empty_files', [True, False])
@pytest.mark.parametrize('include_checksum_in_events', [True, False])
def test_whole_file_data_format(sdc_builder, sdc_executor, azure, use_empty_files, include_checksum_in_events):
    """Test simple scenario of moving files from source to target using WHOLE_FILE_FORMAT."""
    blob_dir = get_random_string()
    blob_name_1 = f'{blob_dir}/input.txt'
    blob_name_2 = f'{blob_dir}/input2.txt'

    # Build pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    if use_empty_files:
        data_1 = ''
        checksum_data_1 = 'd41d8cd98f00b204e9800998ecf8427e'
        data_2 = ''
        checksum_data_2 = 'd41d8cd98f00b204e9800998ecf8427e'
    else:
        data_1 = 'Custom text 1'
        checksum_data_1 = 'e3d826698973ec6160bfcc7fc40f24b7'
        data_2 = 'Custom text 2'
        checksum_data_2 = '12d2a26b3580f3b6545a3a3439642283'
    work_dir = _prepare_work_dir(sdc_executor, data_1, data_2, file_extension='txt')

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = 'input*.txt'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    azure_destination = pipeline_builder.add_stage('Azure Blob Storage', type='destination')
    azure_destination.set_attributes(
        blob_type='BLOCK',
        blob_prefix=f'{blob_dir}/',
        blob_suffix='',
        data_format='WHOLE_FILE',
        file_name_expression="${record:value('/fileInfo/filename')}",
        include_checksum_in_events=include_checksum_in_events
    )

    wiretap = pipeline_builder.add_wiretap()

    origin >> azure_destination >= wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)
    dl_fs = azure.datalake.file_system
    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2, timeout_sec=60)

        # Validate event generation
        output_records = wiretap.output_records
        assert len(output_records) == 2
        assert output_records[0].get_field_data('/targetFileInfo/container') == azure.adls_gen2_filesystem_id
        assert output_records[0].get_field_data('/targetFileInfo/objectKey') == blob_name_1
        assert output_records[0].get_field_data('/targetFileInfo/accountFQDN') == azure.blob_storage_fqdn
        assert output_records[1].get_field_data('/targetFileInfo/container') == azure.adls_gen2_filesystem_id
        assert output_records[1].get_field_data('/targetFileInfo/objectKey') == blob_name_2
        assert output_records[1].get_field_data('/targetFileInfo/accountFQDN') == azure.blob_storage_fqdn
        if include_checksum_in_events:
            assert output_records[0].get_field_data('/checksumAlgorithm') == "MD5"
            assert output_records[0].get_field_data('/checksum') == checksum_data_1
            assert output_records[1].get_field_data('/checksumAlgorithm') == "MD5"
            assert output_records[1].get_field_data('/checksum') == checksum_data_2

        response = dl_fs.cat(blob_name_1).response
        assert response.status_code == 200, f'Failed to read blob {blob_name_1}. Status code: {response.status_code}'
        actual_data = response.content.decode('utf-8')
        assert actual_data == data_1

        response = dl_fs.cat(blob_name_2).response
        assert response.status_code == 200, f'Failed to read blob {blob_name_2}. Status code: {response.status_code}'
        actual_data = response.content.decode('utf-8')
        assert actual_data == data_2

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        dl_fs.rmdir(blob_dir, recursive=True)
        sdc_executor.execute_shell(f'rm -rf {work_dir}')


@sdc_min_version('6.2.0')
@pytest.mark.parametrize('file_exists_action', ["TO_ERROR", "OVERWRITE"])
@pytest.mark.parametrize('override_allowed_in_the_pipeline', [True, False])
def test_whole_file_data_format_file_exist_action(
        sdc_builder, sdc_executor, azure, file_exists_action, override_allowed_in_the_pipeline):
    """Test simple scenario of moving files from source to target using WHOLE_FILE_FORMAT."""
    blob_dir = get_random_string()
    blob_name = f'{blob_dir}/input.txt'

    # Build pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    original_data = 'Custom text 1'
    work_dir = _prepare_work_dir(sdc_executor, original_data, file_extension='txt')

    dl_fs = azure.datalake.file_system

    dl_fs.touch(blob_name)

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = 'input*.txt'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    azure_destination = pipeline_builder.add_stage('Azure Blob Storage', type='destination')
    azure_destination.set_attributes(
        blob_type='BLOCK',
        blob_prefix=f'{blob_dir}/',
        blob_suffix='',
        overwrite=override_allowed_in_the_pipeline,
        data_format='WHOLE_FILE',
        file_name_expression="${record:value('/fileInfo/filename')}",
        file_exists=file_exists_action
    )

    wiretap = pipeline_builder.add_wiretap()

    origin >> azure_destination >= wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=60)

        if file_exists_action == 'OVERWRITE':
            # Validate event generation
            output_records = wiretap.output_records
            assert len(output_records) == 1
            assert len(wiretap.error_records) == 0
            assert output_records[0].get_field_data('/targetFileInfo/container') == azure.adls_gen2_filesystem_id
            assert output_records[0].get_field_data('/targetFileInfo/objectKey') == blob_name
            assert output_records[0].get_field_data('/targetFileInfo/accountFQDN') == azure.blob_storage_fqdn

            response = dl_fs.cat(blob_name).response
            assert response.status_code == 200, f'Failed to read blob {blob_name}. Status code: {response.status_code}'
            actual_data = response.content.decode('utf-8')
            assert actual_data == original_data
        else:
            assert len(wiretap.output_records) == 0
            error_records = wiretap.error_records
            assert len(error_records) == 1
            assert error_records[0].header['errorCode'] == 'AZURE_CLIENT_18'

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        dl_fs.rmdir(blob_dir, recursive=True)
        sdc_executor.execute_shell(f'rm -rf {work_dir}')


@sdc_min_version('6.2.0')
def test_whole_file_data_format_with_large_files(sdc_builder, sdc_executor, azure):
    """Test simple scenario of moving files from source to target using WHOLE_FILE_FORMAT."""
    blob_dir = get_random_string()
    blob_name = f'{blob_dir}/input.txt'

    # Build pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    original_data = "".join(choice(ascii_lowercase) for i in range(100000))
    work_dir = _prepare_work_dir(sdc_executor, original_data, file_extension='txt')

    dl_fs = azure.datalake.file_system

    origin = pipeline_builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = 'input*.txt'
    origin.data_format = 'WHOLE_FILE'
    origin.files_directory = work_dir

    azure_destination = pipeline_builder.add_stage('Azure Blob Storage', type='destination')
    azure_destination.set_attributes(
        blob_type='BLOCK',
        blob_prefix=f'{blob_dir}/',
        blob_suffix='',
        data_format='WHOLE_FILE',
        file_name_expression="${record:value('/fileInfo/filename')}",
        include_checksum_in_events=True
    )

    wiretap = pipeline_builder.add_wiretap()

    origin >> azure_destination >= wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=60)

        # Validate event generation
        output_records = wiretap.output_records
        assert len(output_records) == 1
        assert output_records[0].get_field_data('/targetFileInfo/container') == azure.adls_gen2_filesystem_id
        assert output_records[0].get_field_data('/targetFileInfo/objectKey') == blob_name
        assert output_records[0].get_field_data('/targetFileInfo/accountFQDN') == azure.blob_storage_fqdn
        assert output_records[0].get_field_data('/checksumAlgorithm') == "MD5"
        assert output_records[0].get_field_data('/checksum') is not None

        response = dl_fs.cat(blob_name).response
        assert response.status_code == 200, f'Failed to read blob {blob_name}. Status code: {response.status_code}'
        actual_data = response.content.decode('utf-8')
        assert actual_data == original_data

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        dl_fs.rmdir(blob_dir, recursive=True)
        sdc_executor.execute_shell(f'rm -rf {work_dir}')


def test_multiple_batches(sdc_builder, sdc_executor, azure):
    """
    Test that we can process multiple batches

    The pipeline looks like:
        dev >> azure_blob_storage_destination
    """
    _test_dev_data_generator(sdc_builder, sdc_executor, azure, 100, 1, 10)


def test_multithreading(sdc_builder, sdc_executor, azure):
    """
    Test that we can process multiple threads

    The pipeline looks like:
        dev >> azure_blob_storage_destination
    """
    _test_dev_data_generator(sdc_builder, sdc_executor, azure, 100, 10, 10)


def _test_dev_data_generator(sdc_builder, sdc_executor, azure, num_records, num_threads, batch_size):
    """
    Test that we can process multiple threads and batches

    The pipeline looks like:
        dev >> azure_blob_storage_destination
    """
    files_per_thread = (num_records // num_threads) // batch_size
    blob_dir = get_random_string()
    blob_prefix = os.path.join(blob_dir, 'test')
    blob_suffix = ".json"
    blob_formatter = f'{blob_prefix}-%06d-%03d{blob_suffix}'
    builder = sdc_builder.get_pipeline_builder()
    dl_fs = azure.datalake.file_system

    dev_data_origin = builder.add_stage('Dev Data Generator')
    dev_data_origin.set_attributes(
        records_to_be_generated=num_records,
        batch_size=batch_size,
        number_of_threads=num_threads,
        fields_to_generate=[{'field': 'text', 'type': 'STRING'}]
    )

    azure_blob_storage_destination = builder.add_stage(name=STAGE_NAME)
    azure_blob_storage_destination.set_attributes(data_format='JSON',
                                                  blob_prefix=blob_prefix,
                                                  blob_suffix=blob_suffix)

    dev_data_origin >> azure_blob_storage_destination

    try:
        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        for thread_id in range(num_threads):
            for file_id in range(files_per_thread):
                blob_name = blob_formatter % (file_id, thread_id)
                logger.info('Reading blob %s', blob_name)
                response = dl_fs.cat(blob_name).response
                assert response.status_code == 200, f'Failed to read blob {blob_name}. Status code: {response.status_code}'
                actual_data = response.content.decode('utf-8').split('\n')
                actual_data = [json.loads(record) for record in actual_data]
                assert len(actual_data) == batch_size, f'Expected {batch_size} records but got {len(actual_data)}'

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', blob_dir)
        dl_fs.rmdir(blob_dir, recursive=True)


@pytest.mark.parametrize('test_name, object_name', AZURE_OBJECT_NAMES, ids=[i[0] for i in AZURE_OBJECT_NAMES])
def test_object_names(sdc_builder, sdc_executor, azure, test_name, object_name):
    """Test file names expressions for Blob Storage destination stage.

    Pipeline:
        dev >> azure_blob_storage_destination
    """
    num_records = 10
    blob_dir = get_random_string()
    blob_prefix = os.path.join(blob_dir, object_name)
    blob_suffix = ".json"
    blob_name = f'{blob_prefix}-000000-000{blob_suffix}'
    builder = sdc_builder.get_pipeline_builder()
    dl_fs = azure.datalake.file_system

    # create raw data
    data = []
    for id in range(num_records):
        data.append({"id": id, "text": get_random_string()})

    # dev raw data
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join(json.dumps(row) for row in data)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    azure_blob_storage_destination = builder.add_stage(name=STAGE_NAME)
    azure_blob_storage_destination.set_attributes(data_format='JSON',
                                                  blob_prefix=blob_prefix,
                                                  blob_suffix=blob_suffix)

    dev_raw_data_source >> azure_blob_storage_destination

    try:
        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        response = dl_fs.cat(blob_name).response
        assert response.status_code == 200, f'Failed to read blob {blob_name}. Status code: {response.status_code}'
        actual_data = response.content.decode('utf-8').split('\n')
        actual_data = [json.loads(record) for record in actual_data]
        assert len(actual_data) == num_records, f'Expected {num_records} records but got {len(actual_data)}'

        for actual, expected in zip(actual_data, data):
            assert actual == expected, f'Actual record {actual} does not match expected record {expected}'

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', blob_dir)
        dl_fs.rmdir(blob_dir, recursive=True)


def test_dataflow_events(sdc_builder, sdc_executor, azure):
    """Test generated events for Blob Storage destination stage.

    Pipeline:
        dev >> azure_blob_storage_destination >> wiretap
    """
    num_records = 10
    blob_dir = get_random_string()
    blob_prefix = os.path.join(blob_dir, "test")
    blob_suffix = ".json"
    blob_name = f'{blob_prefix}-000000-000{blob_suffix}'
    builder = sdc_builder.get_pipeline_builder()
    dl_fs = azure.datalake.file_system

    # create raw data
    data = []
    for id in range(num_records):
        data.append({"id": id, "text": get_random_string()})

    # dev raw data
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join(json.dumps(row) for row in data)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    azure_blob_storage_destination = builder.add_stage(name=STAGE_NAME)
    azure_blob_storage_destination.set_attributes(data_format='JSON',
                                                  blob_prefix=blob_prefix,
                                                  blob_suffix=blob_suffix)

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> azure_blob_storage_destination >= wiretap.destination

    pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        event = wiretap.output_records[0]
        assert event.get_field_data('/recordCount') == num_records, 'Wrong number of records'
        assert event.get_field_data('/blobName') == blob_name, 'Wrong blob name'
        assert event.get_field_data('/container') == getattr(azure_blob_storage_destination, 'storage_container_/_file_system'), \
            'Wrong storage container'

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', blob_dir)
        dl_fs.rmdir(blob_dir, recursive=True)


def _prepare_work_dir(sdc_executor, data, data2=None, file_extension='csv'):
    """Create work directory, insert test data, return the work directory."""
    work_dir = os.path.join(tempfile.gettempdir(), get_random_string())
    sdc_executor.execute_shell(f'mkdir -p {work_dir}')
    sdc_executor.write_file(os.path.join(work_dir, f'input.{file_extension}'), data)
    if data2 is not None:
        sdc_executor.write_file(os.path.join(work_dir, f'input2.{file_extension}'), data2)
    return work_dir
