# Copyright 2017 StreamSets Inc.
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

import logging
import os
import string
import time

import pytest
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

SDC_MAX_VERSION = '6.0.0'

from .utils.utils_azure import create_blob_container

logger = logging.getLogger(__name__)

HDP_LIBRARY_NAME = 'streamsets-datacollector-hdp_3_1-lib'


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib(HDP_LIBRARY_NAME)

    return hook


@azure('wasb')
@sdc_min_version('3.2.0.0')
def test_hadoop_fs_standalone_origin_simple(sdc_builder, sdc_executor, azure):
    """Test for Hadoop FS standalone origin using Azure Storage Blob. The pipeline looks like:

    Hadoop FS Standalone pipeline:
        hadoop_fs_standalone >> wiretap
                             >= pipeline_finished_executor
    """
    if Version(sdc_executor.version) >= Version('6.0.0'):
        pytest.skip(f'Test Hadoop FS Standalone Origin Simple test only run against SDC < {SDC_MAX_VERSION}')

    no_of_records = 20
    no_of_threads = 10
    files_dir_name = get_random_string(string.ascii_letters, 10)
    files_dir_path = os.path.join(os.sep, files_dir_name)

    # Build Hadoop FS Standalone origin pipeline
    builder = sdc_builder.get_pipeline_builder()
    hadoop_fs_standalone = builder.add_stage('Hadoop FS Standalone', type='origin', library=HDP_LIBRARY_NAME)
    hadoop_fs_standalone.set_attributes(batch_wait_time_in_secs=5,
                                        data_format='TEXT',
                                        files_directory=files_dir_path,
                                        file_name_pattern='*',
                                        file_name_pattern_mode='GLOB',
                                        hadoop_fs_uri=azure.storage.wasb_url,
                                        hadoop_fs_configuration=[{'key': azure.storage.wasb_account,
                                                                  'value': azure.storage.account_key}],
                                        number_of_threads=no_of_threads,
                                        read_order='LEXICOGRAPHICAL')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    wiretap = builder.add_wiretap()

    hadoop_fs_standalone >> wiretap.destination
    hadoop_fs_standalone >= pipeline_finished_executor
    hadoop_fs_pipeline = builder.build(title='Azure WASB for Hadoop FS origin').configure_for_environment(azure)

    sdc_executor.add_pipeline(hadoop_fs_pipeline)

    container_name = azure.storage.wasb_container
    create_blob_container(azure, container_name)

    try:
        logger.info('Creating blob data under %s container with path as %s', container_name, files_dir_path)
        data = [f'{{message: hello {i}}}' for i in range(no_of_records)]
        blob_paths = [f'{files_dir_name}/{get_random_string()}' for _ in range(no_of_records)]

        blob_clients = []
        for idx, blob_path in enumerate(blob_paths):
            logger.debug('Creating blob data at %s', blob_path)
            blob_client = azure.storage.get_blob_client(container=container_name, blob=blob_path)
            blob_client.upload_blob(data[idx])
            blob_clients.append(blob_client)

        # HDFS does not process files created less than 5 seconds ago by default, so we must wait a bit
        time.sleep(10)

        logger.debug('Starting Hadoop FS Standalone pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(hadoop_fs_pipeline).wait_for_finished()

        assert len(wiretap.output_records) == no_of_records
        assert sorted(data) == sorted([str(record.field['text']) for record in wiretap.output_records])
    finally:
        logger.info('Deleting blob data under %s container with path as %s', container_name, files_dir_path)
        for blob_path, blob_client in zip(blob_paths, blob_clients):
            logger.debug('Deleting blob data at %s', blob_path)
            blob_client.delete_blob()
