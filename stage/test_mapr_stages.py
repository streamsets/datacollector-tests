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

import json
import logging
import os
import string

import pytest

from testframework.markers import *
from testframework import environment, sdc
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000

@cluster('mapr')
def test_mapr_db_destination(sdc_builder, sdc_executor, cluster):
    """Write a handful of records to the MapR-DB destination and confirm their presence with an HBase client.

    dev_raw_data_source >> mapr_db
    """
    # Generate some data.
    bike_brands = [dict(name='Cannondale'),
                   dict(name='Specialized'),
                   dict(name='Bianchi'),
                   dict(name='BMC')]
    raw_data = ''.join(json.dumps(brand) for brand in bike_brands)

    table_name = '/user/sdc/{}'.format(get_random_string(string.ascii_letters, 10))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data)

    mapr_db = pipeline_builder.add_stage('MapR DB', type='destination')
    mapr_db.set_attributes(table_name=table_name,
                           row_key='/name',
                           fields=[dict(columnValue='/name',
                                        columnStorageType='TEXT',
                                        columnName='cf1:cq1')])

    dev_raw_data_source >> mapr_db
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    try:
        logger.info('Creating MapR-DB table %s ...', table_name)
        cluster.execute_command('table', 'create', path=table_name, defaultreadperm='p', defaultwriteperm='p')
        cluster.execute_command('table', 'cf', 'create', path=table_name, cfname='cf1')

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(len(bike_brands))
        sdc_executor.stop_pipeline(pipeline)

        rows = [(key, value) for key, value in cluster.mapr_db.client.table(name=table_name).scan()]
        # Bike brands are stored in a list of dicts ('name' => brand). Manipulate this to match what we
        # expect our MapR-DB rows to look like (including putting them in lexicographic order).
        assert sorted((bike_brand['name'].encode(), {b'cf1:cq1': bike_brand['name'].encode()})
                      for bike_brand in bike_brands) == rows
    finally:
        logger.info('Deleting MapR-DB table %s ...', table_name)
        cluster.execute_command('table', 'delete', path=table_name)


@cluster('mapr')
def test_mapr_fs_origin(sdc_builder, sdc_executor, cluster):
    """Write a simple file into a MapR FS folder with a randomly-generated name and confirm that the MapR FS origin
    successfully reads it. Because cluster mode pipelines don't support snapshots, we do this verification using a
    second standalone pipeline whose origin is an SDC RPC written to by the MapR FS pipeline. Specifically, this would
    look like:

    MapR FS pipeline:
        mapr_fs_origin >> sdc_rpc_destination

    Snapshot pipeline:
        sdc_rpc_origin >> trash
    """
    mapr_fs_folder = os.path.join(os.sep, get_random_string(string.ascii_letters, 10))

    # Build the MapR FS pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    mapr_fs_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_maprfs_ClusterMapRFSDSource')
    mapr_fs_origin.data_format = 'TEXT'
    mapr_fs_origin.input_paths.append(mapr_fs_folder)

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.rpc_connections.append('{}:{}'.format(sdc_executor.server_host,
                                                              SDC_RPC_PORT))
    sdc_rpc_destination.rpc_id = get_random_string(string.ascii_letters, 10)

    mapr_fs_origin >> sdc_rpc_destination
    mapr_fs_pipeline = builder.build(title='MapR FS pipeline').configure_for_environment(cluster)
    mapr_fs_pipeline.configuration['executionMode'] = 'CLUSTER_BATCH'

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

    # Add both pipelines we just created to SDC and start writing files to MapR FS with the HDFS client.
    sdc_executor.add_pipeline(mapr_fs_pipeline, snapshot_pipeline)

    try:
        lines_in_file = ['hello', 'hi', 'how are you?']

        logger.debug('Writing file %s/file.txt to MapR FS ...', mapr_fs_folder)
        cluster.mapr_fs.client.makedirs(mapr_fs_folder)
        cluster.mapr_fs.client.write(os.path.join(mapr_fs_folder, 'file.txt'), data='\n'.join(lines_in_file))

        # So here's where we do the clever stuff. We use SDC's capture snapshot endpoint to start and begin
        # capturing a snapshot from the snapshot pipeline. We do this, however, without using the synchronous
        # wait_for_finished function. That way, we can switch over and start the MapR FS pipeline. Once that one
        # completes, we can go back and do an assert on the snapshot pipeline's snapshot.
        logger.debug('Starting snapshot pipeline and capturing snapshot ...')
        snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline,
                                                                  start_pipeline=True)

        logger.debug('Starting MapR FS pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(mapr_fs_pipeline).wait_for_finished()

        snapshot = snapshot_pipeline_command.wait_for_finished().snapshot
        sdc_executor.stop_pipeline(snapshot_pipeline)
        lines_from_snapshot = [record.value['value']['text']['value']
                               for record in snapshot[snapshot_pipeline[0].instance_name].output]

        assert lines_from_snapshot == lines_in_file
    finally:
        cluster.mapr_fs.client.delete(mapr_fs_folder, recursive=True)

@cluster('mapr')
def test_mapr_standalone_streams(sdc_builder, sdc_executor, cluster):
    """This test will start MapR Streams producer and consumer pipelines which check for integrity of data
    from a MapR Streams producer to MapR Streams consumer. Both the pipelines run as standalone. Specifically, this
    would look like:

    MapR Streams producer pipeline:
        dev_raw_data_source >> mapr_streams_producer

    MapR Streams consumer pipeline:
        mapr_streams_consumer >> trash
    """
    # MapR Stream name has to be pre-created in MapR cluster. Clusterdock MapR image has this already.
    stream_name = '/sample-stream'
    stream_topic_name = stream_name + ':' + get_random_string(string.ascii_letters, 10)

    # Build the MapR Stream producer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = 'Hello World!'

    mapr_streams_producer = builder.add_stage('MapR Streams Producer')
    mapr_streams_producer.topic_name = stream_topic_name
    mapr_streams_producer.data_format = 'TEXT'

    dev_raw_data_source >> mapr_streams_producer
    producer_pipeline = builder.build('MapR Streams producer pipeline').configure_for_environment(cluster)
    producer_pipeline.rate_limit = 1

    # Build the MapR Stream consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    mapr_streams_consumer = builder.add_stage('MapR Streams Consumer')
    mapr_streams_consumer.topic_name = stream_topic_name
    mapr_streams_consumer.data_format = 'TEXT'

    trash = builder.add_stage(label='Trash')

    mapr_streams_consumer >> trash
    consumer_pipeline = builder.build('MapR Streams consumer pipeline').configure_for_environment(cluster)
    consumer_pipeline.rate_limit = 1

    sdc_executor.add_pipeline(producer_pipeline, consumer_pipeline)

    # Run pipelines and assert the data flow. To do that, the sequence of steps is as follows:
    # 1. Start MapR Stream producer and make sure to wait till some batches generate
    # 2. Start MapR Stream consumer via capture snapshot feature to make sure data flow can be captured
    # 3. Capture snapshot on the MapR Stream consumer
    # 4. Compare and assert snapshot result to the data injected at the producer
    try:
        sdc_executor.start_pipeline(producer_pipeline).wait_for_pipeline_batch_count(5)
        snapshot_pipeline_command = sdc_executor.capture_snapshot(consumer_pipeline,start_pipeline=True)
        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=120).snapshot
        snapshot_data = snapshot[consumer_pipeline[0].instance_name].output[0].value['value']['text']['value']
        assert dev_raw_data_source.raw_data == snapshot_data
    finally:
        sdc_executor.stop_pipeline(consumer_pipeline)
        sdc_executor.stop_pipeline(producer_pipeline)

