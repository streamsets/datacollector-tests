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

from testframework.markers import *
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000
SNAPSHOT_TIMEOUT_SEC = 120

@cluster('cdh')
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

@cluster('cdh')
def test_kafka_destination(sdc_builder, sdc_executor, cluster):
    """Send simple text messages into Kafka Destination from Dev Raw Data Source and
       confirm that Kafka successfully reads them using KafkaConsumer from cluster.
       Specifically, this would look like:

       Kafka Destination Origin pipeline:
           dev_raw_data_source >> kafka_destination

    """

    kafka_topic_name = get_random_string(string.ascii_letters, 10)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = 'Hello World!'

    kafka_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.kafka_topic_name = kafka_topic_name
    kafka_destination.data_format = 'TEXT'

    dev_raw_data_source >> kafka_destination
    kafka_destination_pipeline = builder.build(title='Kafka Destination pipeline').configure_for_environment(cluster)
    kafka_destination_pipeline.configuration['rateLimit'] = 1

    sdc_executor.add_pipeline(kafka_destination_pipeline)

    # Specify timeout so that iteration of consumer is stopped after that time and
    # specify auto_offset_reset to get messages from beginning.
    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([kafka_topic_name])

    # Send messages using pipeline to Kafka Destination.
    logger.debug('Starting Kafka Destination pipeline and waiting for it to produce 10 records ...')
    sdc_executor.start_pipeline(kafka_destination_pipeline).wait_for_pipeline_batch_count(10)

    logger.debug('Stopping Kafka Destination pipeline and getting the count of records produced in total ...')
    sdc_executor.stop_pipeline(kafka_destination_pipeline).wait_for_stopped()

    history = sdc_executor.pipeline_history(kafka_destination_pipeline)
    msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
    logger.debug('No. of messages sent in the pipeline = %s', msgs_sent_count)

    msgs_received = [message.value.decode().strip() for message in consumer]
    logger.debug('No. of messages received in Kafka Consumer = %d', (len(msgs_received)))

    logger.debug('Verifying messages with Kafka consumer client ...')
    assert msgs_sent_count == len(msgs_received)
    assert msgs_received == [dev_raw_data_source.raw_data] * msgs_sent_count

@cluster('cdh')
@pytest.mark.parametrize("execution_mode", ['cluster', 'standalone'])
def test_kafka_origin(sdc_builder, sdc_executor, cluster, execution_mode):
    """Write simple text messages into Kafka and confirm that Kafka successfully reads them.
    Because cluster mode pipelines don't support snapshots, we do this verification using a
    second standalone pipeline whose origin is an SDC RPC written to by the Kafka Consumer pipeline.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline:
        kafka_consumer >> sdc_rpc_destination

    Snapshot pipeline:
        sdc_rpc_origin >> trash
    """

    is_cluster_mode = True if execution_mode == 'cluster' else False
    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    kafka_consumer = builder.add_stage(name='com_streamsets_pipeline_stage_origin_kafka_KafkaDSource',
                                       library=cluster.kafka.cluster_stage_lib if is_cluster_mode
                                       else cluster.kafka.standalone_stage_lib)
    kafka_consumer.data_format = 'TEXT'
    kafka_consumer.kafka_topic_name = get_random_string(string.ascii_letters, 10)
    if is_cluster_mode:
        kafka_consumer.max_rate_per_partition = 10
    kafka_consumer.consumer_configs = [{"key": "auto.offset.reset", "value": "earliest"}]

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.rpc_connections.append('{}:{}'.format(sdc_executor.server_host,
                                                              SDC_RPC_PORT))
    sdc_rpc_destination.rpc_id = get_random_string(string.ascii_letters, 10)

    kafka_consumer >> sdc_rpc_destination
    kafka_consumer_pipeline = builder.build(title='{0} pipeline'.format(execution_mode)).configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['executionMode'] = 'CLUSTER_YARN_STREAMING' if is_cluster_mode else 'STANDALONE'

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
    snapshot_pipeline = builder.build(title='{0} Snapshot pipeline'.format(execution_mode))

    # Add both pipelines we just created to SDC.
    sdc_executor.add_pipeline(kafka_consumer_pipeline, snapshot_pipeline)

    try:
        logger.debug('Starting snapshot pipeline and capturing snapshot ...')
        snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline,
                                                                  start_pipeline=True)

        logger.debug('Starting Kafka Consumer pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(kafka_consumer_pipeline).wait_for_status('RUNNING')

        producer = cluster.kafka.producer()
        for _ in range(20):
            producer.send(kafka_consumer.kafka_topic_name, b'Hello World from SDC & DPM!')
        producer.flush()

        # Verify the messages are received correctly.
        logger.debug('Finish the snapshot and verify')
        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=SNAPSHOT_TIMEOUT_SEC).snapshot
        lines_from_snapshot = [record.value['value']['text']['value']
                               for record in snapshot[snapshot_pipeline[0].instance_name].output]

        assert lines_from_snapshot == ['Hello World from SDC & DPM!'] * 20

    finally:
        sdc_executor.stop_pipeline(snapshot_pipeline)
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)
