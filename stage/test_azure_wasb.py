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

"""A module to test various SDC stages of Azure."""

import json
import logging
import os
import string

import pytest

from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

HDP_LIBRARY_NAME = 'streamsets-datacollector-hdp_2_6-lib'
SDC_RPC_PORT = 20000 # Specify a port for SDC RPC stages to use.


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib(HDP_LIBRARY_NAME)

    return hook


@azure('wasb')
@sdc_min_version('3.2.0.0')
def test_hadoop_fs_standalone_origin_simple(sdc_builder, sdc_executor, azure):
    """Test for Hadoop FS standalone origin using Azure Storage Blob. Since the origin is multithreaded, we use
    SDC RPC destination and origin wiring to snapshot capture the single batch and assert. The pipeline looks like:

    Hadoop FS Standalone pipeline:
        hadoop_fs_standalone >> sdc_rpc_destination
                             >= pipeline_finished_executor
    Snapshot pipeline:
        sdc_rpc_origin >> trash
    """
    no_of_records = 20
    no_of_threads = 10
    files_dir_name = get_random_string(string.ascii_letters, 10)
    files_dir_path = os.path.join(os.sep, files_dir_name)

    # Build Hadoop FS Standalone origin pipeline
    builder = sdc_builder.get_pipeline_builder()
    hadoop_fs_standalone = builder.add_stage('Hadoop FS Standalone', type='origin', library=HDP_LIBRARY_NAME)
    hadoop_fs_standalone.set_attributes(batch_wait_time_in_secs=5, data_format='JSON',
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

    sdc_rpc_destination = builder.add_stage('SDC RPC', type='destination')
    sdc_rpc_destination.sdc_rpc_connection.append('{}:{}'.format(sdc_executor.server_host, SDC_RPC_PORT))
    sdc_rpc_destination.sdc_rpc_id = get_random_string(string.ascii_letters, 10)

    hadoop_fs_standalone >= pipeline_finished_executor
    hadoop_fs_standalone >> sdc_rpc_destination
    hadoop_fs_pipeline = builder.build(title='Azure WASB for Hadoop FS origin').configure_for_environment(azure)

    # Build the Snapshot pipeline
    builder = sdc_builder.get_pipeline_builder()

    sdc_rpc_origin = builder.add_stage('SDC RPC', type='origin')
    sdc_rpc_origin.sdc_rpc_listening_port = SDC_RPC_PORT
    sdc_rpc_origin.sdc_rpc_id = sdc_rpc_destination.sdc_rpc_id

    trash = builder.add_stage('Trash')

    sdc_rpc_origin >> trash
    snapshot_pipeline = builder.build(title='Snapshot pipeline')

    sdc_executor.add_pipeline(hadoop_fs_pipeline, snapshot_pipeline)

    try:
        blob_service = azure.storage.account.create_block_blob_service()
        container_name = azure.storage.wasb_container

        logger.info('Creating blob data under %s container with path as %s', container_name, files_dir_path)
        data = [f'{{"message": "hello {i}"}}' for i in range(no_of_records)]
        blob_paths = ['{}/{}'.format(files_dir_name, get_random_string(string.ascii_letters, 10))
                      for _ in range(no_of_records)]
        for idx, blob_path in enumerate(blob_paths):
            logger.debug('Creating blob data at %s', blob_path)
            blob_service.create_blob_from_text(container_name, blob_path, data[idx])

        logger.debug('Starting snapshot pipeline and capturing snapshot ...')
        snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline, start_pipeline=True,
                                                                  wait=False, batches=no_of_records, batch_size=1) # batches won't work till SDC-8765 is fixed
        logger.debug('Starting Hadoop FS Standalone pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(hadoop_fs_pipeline).wait_for_finished()

        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=120).snapshot
        records_from_snapshot = [json.dumps(record.value2) for record in snapshot[sdc_rpc_origin.instance_name].output]

        sdc_executor.stop_pipeline(snapshot_pipeline, force=True)

        history = sdc_executor.get_pipeline_history(snapshot_pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == no_of_records
        assert all([record in data for record in records_from_snapshot]) #TODO checks only subsets of data till SDC-8765 is fixed
    finally:
        logger.info('Deleting blob data under %s container with path as %s', container_name, files_dir_path)
        for blob_path in blob_paths:
            logger.debug('Deleting blob data at %s', blob_path)
            blob_service.delete_blob(container_name, blob_path)
