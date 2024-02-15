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
import re

import pytest
from streamsets.sdk.exceptions import RunError
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [azure('datalake'), sdc_min_version('5.10.0')]

STAGE_NAME = 'com_streamsets_pipeline_stage_destination_client_blob_BlobStorageDTarget'


@pytest.fixture(autouse=True)
def storage_type_check(azure):
    if azure.storage_type == 'Storage':
        pytest.skip('Blob Storage tests require storage type to be of Gen2.')


@pytest.mark.parametrize('blob_type', ['BLOCK', 'APPEND'])
def test_blob_storage_destination(sdc_builder, sdc_executor, azure, blob_type):
    """ Writes records to Azure Blob Storage and then reads them back using the STF client. We assert read data is correct.
    The pipeline looks like:

    dev >> azure_blob_storage_destination
    """

    num_records = 10
    blob_dir = get_random_string()
    blob_prefix = os.path.join(blob_dir, 'test')
    blob_suffix = ".json"
    blob_name = f'{blob_prefix}-000000-000{blob_suffix}' if blob_type == 'BLOCK' \
        else f'{blob_prefix}-000{blob_suffix}'
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
                                                  blob_type=blob_type,
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


def test_append(sdc_builder, sdc_executor, azure):
    """ Append several batches to a unique blob using Azure Blob Storage and then reads them back using the STF client.
    We assert read data is correct.
    The pipeline looks like:

    dev >> azure_blob_storage_destination
    """

    num_records = 10
    min_num_batches = 10
    blob_dir = get_random_string()
    blob_prefix = os.path.join(blob_dir, 'test')
    blob_suffix = ".json"
    blob_name = f'{blob_prefix}-000{blob_suffix}'
    pattern = r'}{'
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
                                       stop_after_first_batch=False)

    azure_blob_storage_destination = builder.add_stage(name=STAGE_NAME)
    azure_blob_storage_destination.set_attributes(data_format='JSON',
                                                  blob_type='APPEND',
                                                  blob_prefix=blob_prefix,
                                                  blob_suffix=blob_suffix)

    dev_raw_data_source >> azure_blob_storage_destination

    pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(min_num_batches)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        batch_count = history.latest.metrics.counter('pipeline.batchCount.counter').count
        assert min_num_batches <= batch_count, f'Expected at least {min_num_batches} batches but got {batch_count}'

        response = dl_fs.cat(blob_name).response
        assert response.status_code == 200, f'Failed to read blob {blob_name}. Status code: {response.status_code}'
        actual_data = response.content.decode('utf-8')
        actual_data = re.sub(pattern, '}\n{', actual_data).split("\n")
        actual_data = [json.loads(record) for record in actual_data]
        assert len(actual_data) == batch_count * num_records, \
            f'Expected {batch_count * num_records} records but got {len(actual_data)}'
        for actual, expected in zip(actual_data, batch_count * data):
            assert actual == expected, f'Actual record {actual} does not match expected record {expected}'

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', blob_dir)
        dl_fs.rmdir(blob_dir, recursive=True)


@pytest.mark.parametrize('overwrite', [True, False])
def test_overwrite(sdc_builder, sdc_executor, azure, overwrite):
    """ Writes records to Azure Blob Storage and then reads them back using the STF client. We assert read data is correct.
    The pipeline looks like:

    dev >> azure_blob_storage_destination
    """

    num_records = 10
    blob_dir = get_random_string()
    blob_prefix = os.path.join(blob_dir, 'test')
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
                                                  overwrite=overwrite,
                                                  blob_prefix=blob_prefix,
                                                  blob_suffix=blob_suffix)

    dev_raw_data_source >> azure_blob_storage_destination

    try:
        dl_fs.touch(blob_name)
        dl_fs.write(blob_name, get_random_string(length=1024))

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)

        if not overwrite:
            with pytest.raises(RunError) as error:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert "ADLS_13" in error.value.message, f'Expected a ADLS_13 error, got "{error.value.message}" instead'

        else:
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


@azure('datalake_page')
def test_page(sdc_builder, sdc_executor, azure):
    """ Append several batches to a page blob using Azure Blob Storage and then reads them back using the STF client.
    We assert read data is correct.
    The pipeline looks like:

    dev >> azure_blob_storage_destination
    """

    num_records = 10
    min_num_batches = 10
    blob_dir = get_random_string()
    blob_prefix = os.path.join(blob_dir, 'test')
    blob_suffix = ".json"
    blob_name = f'{blob_prefix}-000{blob_suffix}'
    blob_size = 100 * 1024
    page_size = 1024
    pattern = r'}{'
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
                                       stop_after_first_batch=False)

    azure_blob_storage_destination = builder.add_stage(name=STAGE_NAME)
    azure_blob_storage_destination.set_attributes(data_format='JSON',
                                                  blob_type='PAGE',
                                                  blob_size=blob_size,
                                                  page_size=page_size,
                                                  blob_prefix=blob_prefix,
                                                  blob_suffix=blob_suffix)

    dev_raw_data_source >> azure_blob_storage_destination

    pipeline = builder.build().configure_for_environment(azure)

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(min_num_batches)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        batch_count = history.latest.metrics.counter('pipeline.batchCount.counter').count
        assert min_num_batches <= batch_count, f'Expected at least {min_num_batches} batches but got {batch_count}'

        response = dl_fs.cat(blob_name).response
        assert response.status_code == 200, f'Failed to read blob {blob_name}. Status code: {response.status_code}'
        actual_data = response.content.decode('utf-8')
        assert len(actual_data) == blob_size, f'Expected {blob_size} bytes but got {len(actual_data)}'
        for page_id in range(batch_count):
            start_pos = page_id * page_size
            end_pos = start_pos + page_size
            actual_page = actual_data[start_pos:end_pos].rstrip('\x00')
            actual_page = re.sub(pattern, '}\n{', actual_page).split("\n")
            actual_page = [json.loads(record) for record in actual_page]
            assert len(actual_page) == num_records, f'Expected {num_records} records but got {len(actual_page)}'
            for actual, expected in zip(actual_page, data):
                assert actual == expected, f'Actual record {actual} does not match expected record {expected}'

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', blob_dir)
        dl_fs.rmdir(blob_dir, recursive=True)
