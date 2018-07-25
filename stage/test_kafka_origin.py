# Copyright 2018 StreamSets Inc.
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
import string

import pytest

from streamsets.sdk.utils import Version
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000
SNAPSHOT_TIMEOUT_SEC = 120

MIN_SDC_VERSION_WITH_SPARK_2_LIB = Version('3.3.0')


def get_kafka_consumer_stage(sdc_version, pipeline_builder, cluster, cluster_mode):
    """Create and return a Kafka origin stage depending on execution mode for the pipeline."""

    pipeline_builder.add_error_stage('Discard')

    if cluster_mode:
        if Version(sdc_version) < MIN_SDC_VERSION_WITH_SPARK_2_LIB:
            kafka_cluster_stage_lib = cluster.kafka.cluster_stage_lib_spark1
        else:
            kafka_cluster_stage_lib = cluster.kafka.cluster_stage_lib_spark2
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer',
                                                type='origin',
                                                library=(kafka_cluster_stage_lib
                                                         if cluster_mode
                                                         else cluster.kafka.standalone_stage_lib))
    topic = get_random_string(string.ascii_letters, 10)
    kafka_consumer.set_attributes(data_format='TEXT',
                                  batch_wait_time_in_ms=20000,
                                  topic=topic,
                                  kafka_configuration=[{'key': 'auto.offset.reset', 'value': 'earliest'}])

    if cluster_mode:
        kafka_consumer.set_attributes(max_batch_size_in_records=10,
                                      rate_limit_per_partition_in_kafka_messages=10)

    return kafka_consumer


def produce_kafka_messages(topic, cluster, message):
    """Send messages to Kafka."""
    producer = cluster.kafka.producer()
    for _ in range(10):
        producer.send(topic, message)
    producer.flush()


def verify_kafka_origin_results(kafka_consumer_pipeline, snapshot_pipeline, sdc_executor, message):
    """Take a snapshot from sdc kafka origin pipeline to verify results.
    Note that kafka_consumer_pipeline = snapshot_pipeline in case of standalone mode.
    """
    cluster_mode = kafka_consumer_pipeline != snapshot_pipeline
    snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline, start_pipeline=True,
                                                              wait=False)

    if cluster_mode:
        sdc_executor.start_pipeline(kafka_consumer_pipeline)

    logger.debug('Finish the snapshot and verify')
    snapshot_command = snapshot_pipeline_command.wait_for_finished(timeout_sec=SNAPSHOT_TIMEOUT_SEC)
    snapshot = snapshot_command.snapshot
    lines_from_snapshot = [record.value['value']['text']['value']
                           for record in snapshot[snapshot_pipeline[0].instance_name].output]

    if sdc_executor.get_pipeline_status(kafka_consumer_pipeline) == 'RUNNING':
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)
    if cluster_mode and sdc_executor.get_pipeline_status(snapshot_pipeline) == 'RUNNING':
        sdc_executor.stop_pipeline(snapshot_pipeline)

    assert lines_from_snapshot == [message] * 10


@cluster('cdh', 'kafka')
def test_kafka_origin_standalone(sdc_builder, sdc_executor, cluster):
    """Write simple text messages into Kafka and confirm that Kafka successfully reads them.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(sdc_builder.version, builder, cluster, cluster_mode=False)

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Standalone pipeline').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic,
                               cluster,
                               b'Hello World from SDC & DPM!')
        verify_kafka_origin_results(kafka_consumer_pipeline,
                                    kafka_consumer_pipeline,
                                    sdc_executor,
                                    'Hello World from SDC & DPM!')
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)
        sdc_executor.remove_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
@sdc_min_version('3.0.0.0')
def test_kafka_multi_origin_standalone(sdc_builder, sdc_executor, cluster):
    """Write simple text messages into Kafka and confirm that MultiTopic origin can read them.
    Specifically, this would look like:

    Kafka Multi Topic Consumer Origin pipeline with standalone mode:
        kafka_multitopic_consumer >> trash
    """

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()

    topic_name = get_random_string(string.ascii_letters, 10)
    kafka_multitopic_consumer = builder.add_stage('Kafka Multitopic Consumer')
    kafka_multitopic_consumer.set_attributes(data_format='TEXT',
                                             batch_wait_time_in_ms=2000,
                                             topic_list=[topic_name],
                                             configuration_properties=[{'key': 'auto.offset.reset',
                                                                        'value': 'earliest'}])

    trash = builder.add_stage(label='Trash')
    kafka_multitopic_consumer >> trash
    kafka_multitopic_consumer_pipeline = builder.build(title='Kafka Multitopic').configure_for_environment(cluster)
    kafka_multitopic_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_multitopic_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(topic_name,
                               cluster,
                               b'Hello World from SDC & DPM!')
        verify_kafka_origin_results(kafka_multitopic_consumer_pipeline,
                                    kafka_multitopic_consumer_pipeline,
                                    sdc_executor,
                                    'Hello World from SDC & DPM!')
    finally:
        sdc_executor.stop_pipeline(kafka_multitopic_consumer_pipeline)
        sdc_executor.remove_pipeline(kafka_multitopic_consumer_pipeline)


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
    if (Version(sdc_builder.version) < MIN_SDC_VERSION_WITH_SPARK_2_LIB and
            ('kafka' in cluster.kerberized_services or cluster.kafka.is_ssl_enabled)):
        pytest.skip('Kafka cluster mode test only '
                    f'runs against cluster with the non-secured Kafka for SDC version {sdc_builder.version}.')

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(sdc_builder.version, builder, cluster, cluster_mode=True)

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.sdc_rpc_connection.append('{}:{}'.format(sdc_executor.server_host,
                                                                 SDC_RPC_PORT))
    sdc_rpc_destination.sdc_rpc_id = get_random_string(string.ascii_letters, 10)
    kafka_consumer >> sdc_rpc_destination
    kafka_consumer_pipeline = builder.build(title='Cluster pipeline').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['executionMode'] = 'CLUSTER_YARN_STREAMING'

    # Build the Snapshot pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')
    sdc_rpc_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_sdcipc_SdcIpcDSource')
    sdc_rpc_origin.sdc_rpc_listening_port = SDC_RPC_PORT
    sdc_rpc_origin.sdc_rpc_id = sdc_rpc_destination.sdc_rpc_id
    # Since YARN jobs take a while to get going, set RPC origin batch wait time to 5 min. to avoid
    # getting an empty batch in the snapshot.
    sdc_rpc_origin.batch_wait_time_in_secs = 300
    trash = builder.add_stage(label='Trash')
    sdc_rpc_origin >> trash
    snapshot_pipeline = builder.build(title='Cluster Snapshot pipeline')

    sdc_executor.add_pipeline(kafka_consumer_pipeline, snapshot_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic,
                               cluster,
                               b'Hello World from SDC & DPM!')
        verify_kafka_origin_results(kafka_consumer_pipeline,
                                    snapshot_pipeline,
                                    sdc_executor,
                                    'Hello World from SDC & DPM!')
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)
        sdc_executor.stop_pipeline(snapshot_pipeline)
        sdc_executor.remove_pipeline(kafka_consumer_pipeline)
        sdc_executor.remove_pipeline(snapshot_pipeline)
