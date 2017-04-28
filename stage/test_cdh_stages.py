# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import logging
import os
import string

import pytest

from testframework import environment, sdc
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000

def test_hadoop_fs_origin_simple(sdc_builder, sdc_executor, cluster):
    """Write a simple file into a Hadoop FS folder with a randomly-generated name and confirm that the Hadoop FS origin
    successfully reads it. Because cluster mode pipelines don't support snapshots, we do this verification using a
    second standalone pipeline whose origin is an SDC RPC written to by the Hadoop FS pipeline. Specifically, this would
    look like:

    Hadoop FS pipeline:
        hadoop_fs_origin >> sdc_rpc_destination

    Snapshot pipeline:
        sdc_rpc_origin >> trash
    """
    hadoop_fs_folder = os.path.join(os.sep, get_random_string(string.ascii_letters, 10))

    # Build the Hadoop FS pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    hadoop_fs_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_hdfs_cluster_ClusterHdfsDSource')
    hadoop_fs_origin.data_format = 'TEXT'
    hadoop_fs_origin.input_paths.append(hadoop_fs_folder)

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.rpc_connections.append('{}:{}'.format(sdc_executor.server_host,
                                                              SDC_RPC_PORT))
    sdc_rpc_destination.rpc_id = get_random_string(string.ascii_letters, 10)

    hadoop_fs_origin >> sdc_rpc_destination
    hadoop_fs_pipeline = builder.build(title='Hadoop FS pipeline').configure_for_environment(cluster)
    hadoop_fs_pipeline.configuration['executionMode'] = 'CLUSTER_BATCH'

    # Build the Snapshot pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    sdc_rpc_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_sdcipc_SdcIpcDSource')
    sdc_rpc_origin.rpc_port = SDC_RPC_PORT
    sdc_rpc_origin.rpc_id = sdc_rpc_destination.rpc_id
    # Since YARN jobs take a while to get going, set RPC origin batch wait time to 5 min. to avoid
    # getting an empty batch in the snapshot.
    sdc_rpc_origin.batch_wait_time_in_secs = 300

    trash = builder.add_stage(label='Trash')

    sdc_rpc_origin >> trash
    snapshot_pipeline = builder.build(title='Snapshot pipeline')

    # Add both pipelines we just created to SDC and start writing files to Hadoop FS with the HDFS client.
    sdc_executor.add_pipeline(hadoop_fs_pipeline, snapshot_pipeline)

    try:
        lines_in_file = ['hello', 'hi', 'how are you?']

        logger.debug('Writing file %s/file.txt to Hadoop FS ...', hadoop_fs_folder)
        cluster.hdfs.client.makedirs(hadoop_fs_folder)
        cluster.hdfs.client.write(os.path.join(hadoop_fs_folder, 'file.txt'), data='\n'.join(lines_in_file))

        # So here's where we do the clever stuff. We use SDC's capture snapshot endpoint to start and begin
        # capturing a snapshot from the snapshot pipeline. We do this, however, without using the synchronous
        # wait_for_finished function. That way, we can switch over and start the Hadoop FS pipeline. Once that one
        # completes, we can go back and do an assert on the snapshot pipeline's snapshot.
        logger.debug('Starting snapshot pipeline and capturing snapshot ...')
        snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline,
                                                                    start_pipeline=True)

        logger.debug('Starting Hadoop FS pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(hadoop_fs_pipeline).wait_for_finished()

        snapshot = snapshot_pipeline_command.wait_for_finished().snapshot
        sdc_executor.stop_pipeline(snapshot_pipeline)
        lines_from_snapshot = [record.value['value']['text']['value']
                               for record in snapshot[snapshot_pipeline[0].instance_name].output]

        assert lines_from_snapshot == lines_in_file
    finally:
        cluster.hdfs.client.delete(hadoop_fs_folder, recursive=True)
