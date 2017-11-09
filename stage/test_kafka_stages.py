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


import logging
import string

from testframework.markers import cluster
from testframework.markers import sdc_min_version
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000
SNAPSHOT_TIMEOUT_SEC = 120


@cluster('cdh')
def test_kafka_destination(sdc_builder, sdc_executor, cluster):
    """Send simple text messages into Kafka Destination from Dev Raw Data Source and
       confirm that Kafka successfully reads them using KafkaConsumer from cluster.
       Specifically, this would look like:

       Kafka Destination Origin pipeline:
           dev_raw_data_source >> kafka_destination

    """

    kafka_topic_name = get_random_string(string.ascii_letters, 10)
    logger.debug('kafka_topic_name %s', kafka_topic_name)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = 'Hello World!'

    kafka_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.topic = kafka_topic_name
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
    sdc_executor.stop_pipeline(kafka_destination_pipeline)

    history = sdc_executor.pipeline_history(kafka_destination_pipeline)
    msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
    logger.debug('No. of messages sent in the pipeline = %s', msgs_sent_count)

    msgs_received = [message.value.decode().strip() for message in consumer]
    logger.debug('No. of messages received in Kafka Consumer = %d', (len(msgs_received)))

    logger.debug('Verifying messages with Kafka consumer client ...')
    assert msgs_sent_count == len(msgs_received)
    assert msgs_received == [dev_raw_data_source.raw_data] * msgs_sent_count


def get_kafka_consumer_stage(pipeline_builder, cluster, cluster_mode):
    """Create and return a Kafka origin stage depending on execution mode for the pipeline."""

    pipeline_builder.add_error_stage('Discard')

    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer',
                                                type='origin',
                                                library=(cluster.kafka.cluster_stage_lib
                                                         if cluster_mode
                                                         else cluster.kafka.standalone_stage_lib))
    topic_name = get_random_string(string.ascii_letters, 10)
    kafka_consumer.set_attributes(data_format='TEXT',
                                  batch_wait_time_in_millisecs=20000,
                                  kafka_topic_name=topic_name,
                                  consumer_configs=[{'key': 'auto.offset.reset', 'value': 'earliest'}])

    if cluster_mode:
        kafka_consumer.set_attributes(max_batch_size=10,
                                      max_rate_per_partition=10)

    return kafka_consumer


def verify_kafka_origin_results(kafka_topic_name, kafka_consumer_pipeline, snapshot_pipeline, cluster, sdc_executor):
    """Send messages to Kafka and take a snapshot to verify results.
    Note that kafka_consumer_pipeline = snapshot_pipeline in case of standalone mode.
    """

    cluster_mode = kafka_consumer_pipeline != snapshot_pipeline
    snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline, start_pipeline=True,
                                                              wait=False)

    if cluster_mode:
        sdc_executor.start_pipeline(kafka_consumer_pipeline)

    producer = cluster.kafka.producer()
    for _ in range(10):
        producer.send(kafka_topic_name, b'Hello World from SDC & DPM!')
    producer.flush()

    logger.debug('Finish the snapshot and verify')
    snapshot_command = snapshot_pipeline_command.wait_for_finished(timeout_sec=SNAPSHOT_TIMEOUT_SEC)
    snapshot = snapshot_command.snapshot
    lines_from_snapshot = [record.value['value']['text']['value']
                           for record in snapshot[snapshot_pipeline[0].instance_name].output]

    if sdc_executor.get_pipeline_status(kafka_consumer_pipeline) == 'RUNNING':
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)
    if cluster_mode and sdc_executor.get_pipeline_status(snapshot_pipeline) == 'RUNNING':
        sdc_executor.stop_pipeline(snapshot_pipeline)

    assert lines_from_snapshot == ['Hello World from SDC & DPM!'] * 10


@cluster('cdh')
def test_kafka_origin_standalone(sdc_builder, sdc_executor, cluster):
    """Write simple text messages into Kafka and confirm that Kafka successfully reads them.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster, cluster_mode=False)

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Standalone pipeline').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        verify_kafka_origin_results(kafka_consumer.kafka_topic_name,
                                    kafka_consumer_pipeline,
                                    kafka_consumer_pipeline,
                                    cluster,
                                    sdc_executor)
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh')
@sdc_min_version('3.0.0.0')
def test_kafka_multi_origin_standalone(sdc_builder, sdc_executor, cluster):
    """Write simple text messages into Kafka and confirm that MultiTopic origin can read them.
    Specifically, this would look like:

    Kafka Multi Topic Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = builder.add_stage('Kafka Multitopic Consumer')
    topic_name = get_random_string(string.ascii_letters, 10)
    kafka_consumer.set_attributes(data_format='TEXT',
                                  batch_wait_time_in_ms=20000,
                                  topic_list=[topic_name],
                                  kafka_configuration=[{'key': 'auto.offset.reset', 'value': 'earliest'}])

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Multitopic').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        verify_kafka_origin_results(topic_name,
                                    kafka_consumer_pipeline,
                                    kafka_consumer_pipeline,
                                    cluster,
                                    sdc_executor)
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh')
def test_kafka_origin_cluster(sdc_builder, sdc_executor, cluster):
    """Write simple text messages into Kafka and confirm that Kafka successfully reads them.
    Because cluster mode pipelines don't support snapshots, we do this verification using a
    second standalone pipeline whose origin is an SDC RPC written to by the Kafka Consumer pipeline.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline with cluster mode:
        kafka_consumer >> sdc_rpc_destination

    Snapshot pipeline:
        sdc_rpc_origin >> trash
    """

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster, cluster_mode=True)

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.rpc_connections.append('{}:{}'.format(sdc_executor.server_host,
                                                              SDC_RPC_PORT))
    sdc_rpc_destination.rpc_id = get_random_string(string.ascii_letters, 10)
    kafka_consumer >> sdc_rpc_destination
    kafka_consumer_pipeline = builder.build(title='Cluster pipeline').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['executionMode'] = 'CLUSTER_YARN_STREAMING'

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
    snapshot_pipeline = builder.build(title='Cluster Snapshot pipeline')

    sdc_executor.add_pipeline(kafka_consumer_pipeline, snapshot_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        verify_kafka_origin_results(kafka_consumer.kafka_topic_name,
                                    kafka_consumer_pipeline,
                                    snapshot_pipeline,
                                    cluster,
                                    sdc_executor)
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)
