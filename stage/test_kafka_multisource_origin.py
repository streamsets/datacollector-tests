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
import time

import pytest
from streamsets.testframework.environments import cloudera
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SNAPSHOT_TIMEOUT_SEC = 120
SDC_RPC_LISTENING_PORT = 21512


@pytest.fixture(autouse=True)
def kafka_check(cluster):
    if isinstance(cluster, ClouderaManagerCluster) and not hasattr(cluster, 'kafka'):
        pytest.skip('Kafka tests require Kafka to be installed on the cluster')


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')

    return hook


@cluster('cdh', 'kafka')
@sdc_min_version('3.15.0')
def test_kafka_origin_including_timestamps(sdc_builder, sdc_executor, cluster):
    """Check that timestamp and timestamp type are included in record header. Verifies that for previous versions of
    kafka (< 0.10), a validation issue is thrown.

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """
    stage_libs = cluster.sdc_stage_libs

    message = 'Hello World from SDC & DPM!'
    expected = '{\'text\': Hello World from SDC & DPM!}'

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(builder, cluster)
    kafka_multitopic_consumer.set_attributes(include_timestamps=True)

    trash = builder.add_stage(label='Trash')
    kafka_multitopic_consumer >> trash
    kafka_consumer_pipeline = builder.build().configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_multitopic_consumer.topic_list[0], cluster, message.encode(), 'TEXT')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'TEXT_TIMESTAMP')
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
@sdc_min_version('3.6.0')
def test_kafka_origin_timestamp_offset_strategy(sdc_builder, sdc_executor, cluster):
    """Check that accessing a topic for first time using TIMESTAMP offset strategy retrieves messages
    which timestamp >= Auto Offset Reset Timestamp configuration value.

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """
    stage_libs = cluster.sdc_stage_libs

    if ('streamsets-datacollector-apache-kafka_0_9-lib' in stage_libs or
            'streamsets-datacollector-apache-kafka_0_8-lib' in stage_libs or
            'streamsets-datacollector-cdh_kafka_2_1-lib' in stage_libs or
            'streamsets-datacollector-apache-kafka_0_10-lib' in stage_libs):
        pytest.skip('Test only designed to run on Kafka version >= 0.10.1')

    messages = [f'message{i}' for i in range(1, 5)]
    expected = [f'{{\'text\': message{i}}}' for i in range(3, 5)]

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(builder, cluster)

    timestamp = produce_kafka_messages_in_different_timestamp(kafka_multitopic_consumer.topic_list[0], cluster, messages, 'TEXT', 2)

    kafka_multitopic_consumer.set_attributes(auto_offset_reset='TIMESTAMP',
                                  auto_offset_reset_timestamp_in_ms=int(timestamp),
                                  consumer_group=get_random_string(string.ascii_letters, 10))

    trash = builder.add_stage(label='Trash')
    kafka_multitopic_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Standalone pipeline').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.

        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'TEXT')
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


# SDC-10501: Option to enable/disable Kafka auto commit Offsets
@cluster('cdh', 'kafka')
@sdc_min_version('3.6.0')
def test_kafka_origin_not_saving_offset(sdc_builder, sdc_executor, cluster):
    """Ensure that we read all the data, even when a pipeline fails - thus no records are "auto committed". The test
       runs the same pipeline twice - once with failure and second time with success and ensures that the second run
       see all the records.

       The pipeline reads from Kafka and uses delay processor to model longer processing time (so that Kafka's auto
       commit takes place) and then jython processor to generate pipeline failure (1/0).
    """
    topic = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()

    origin = get_kafka_multitopic_consumer_stage(builder, cluster)
    origin.topic_list = [topic]
    origin.auto_offset_reset = 'EARLIEST'
    origin.consumer_group = get_random_string(string.ascii_letters, 10)
    origin.batch_wait_time_in_ms = 100

    delay = builder.add_stage('Delay')
    delay.delay_between_batches = 5*1000

    script = builder.add_stage('Jython Evaluator', type='processor')
    script.script = '1/${DIVISOR}'

    trash = builder.add_stage(label='Trash')

    origin >> delay >> script >> trash

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    pipeline.configuration['executionMode'] = 'STANDALONE'
    pipeline.add_parameters(DIVISOR='0')

    sdc_executor.add_pipeline(pipeline)

    # Produce one message
    producer = cluster.kafka.producer()
    producer.send(topic, 'Super Secret Message'.encode())
    producer.flush()

    try:
        # Start our pipeline - it should fail
        sdc_executor.start_pipeline(pipeline, runtime_parameters={'DIVISOR': 0}).wait_for_status('RUN_ERROR', ignore_errors=True)

        # Adding second message so that the topic have at least one new message, so that getting snapshot on older
        # versions wont't time out but returns immediately.
        producer = cluster.kafka.producer()
        producer.send(topic, 'Not So Super Secret Message'.encode())
        producer.flush()

        # Now run the pipeline second time and it should succeed
        snapshot = sdc_executor.capture_snapshot(pipeline, runtime_parameters={'DIVISOR': 1}, start_pipeline=True).snapshot

        # Now this should still read both records
        records = snapshot[origin].output
        assert len(records) == 2
        assert records[0].field['text'] == 'Super Secret Message'
        assert records[1].field['text'] == 'Not So Super Secret Message'

    finally:
        sdc_executor.stop_pipeline(pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_save_offset(sdc_builder, sdc_executor, cluster):
    """ Above SDC-10501 introduced a bug which does not commit offset when the number of records
    is less than the max batch size. This process 5 records for the 1st run, stop pipeline, and
    run again to process 3 records for the 2nd run. 2nd run should process 3 records as the offset
    should be saved after the 1st run.

    Kafka Multitopic Origin >> Trash (Run twice)
    """
    topic = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()

    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(builder, cluster)
    kafka_multitopic_consumer.topic_list = [topic]
    kafka_multitopic_consumer.auto_offset_reset = 'EARLIEST'
    kafka_multitopic_consumer.consumer_group = get_random_string(string.ascii_letters, 10)
    kafka_multitopic_consumer.batch_wait_time_in_ms = 100

    trash = builder.add_stage(label='Trash')

    kafka_multitopic_consumer >> trash

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(pipeline)

    # Produce 5 messages
    messages = [f'message{i}' for i in range(0, 5)]
    produce_kafka_messages_list(kafka_multitopic_consumer.topic_list[0], cluster, messages, 'TEXT')

    try:
        # Start the pipeline, read one batch and stop.
        snapshot1 = sdc_executor.capture_snapshot(pipeline, batches=1, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        # Check if the pipeline processed 5 records
        records = snapshot1[kafka_multitopic_consumer].output
        assert len(records) == 5

        # Produce another 3 messages
        messages2 = [f'message{i}' for i in range(5, 8)]
        produce_kafka_messages_list(kafka_multitopic_consumer.topic_list[0], cluster, messages2, 'TEXT')

        # Run the pipeline second time
        snapshot2 = sdc_executor.capture_snapshot(pipeline, batches=1, start_pipeline=True).snapshot
        #  2nd run should processed only 3 records
        records2 = snapshot2[kafka_multitopic_consumer].output
        assert len(records2) == 3

    finally:
        sdc_executor.stop_pipeline(pipeline)


# SDC-10897: Kafka setting for Batch Wait Time and Max Batch Size not working in conjunction
@cluster('cdh', 'kafka')
@sdc_min_version('3.6.0')
def test_kafka_origin_batch_max_size(sdc_builder, sdc_executor, cluster):
    """Check that retrieving messages from Kafka using Kafka Multitopic Consumer respects both the Batch Max Wait Time
    and the Max Batch Size. Batches are sent when the first of the two conditions is met. This test is checking that
    the Max Batch Size condition is first met.

    Kafka Multitopic Consumer Origin pipeline with standalone mode:
        kafka_multitopic_consumer >> trash
    """

    messages = [f'message{i}' for i in range(1, 21)]
    expected = [f'message{i}' for i in range(1, 21)]

    num_batches = 2
    kafka_consumer_group = get_random_string(string.ascii_letters, 10)

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(builder, cluster)

    produce_kafka_messages_list(kafka_multitopic_consumer.topic_list[0], cluster, messages, 'TEXT')

    kafka_multitopic_consumer.set_attributes(auto_offset_reset='EARLIEST',
                                             consumer_group=kafka_consumer_group,
                                             max_batch_size_in_records=10,
                                             batch_wait_time_in_ms=30000)

    trash = builder.add_stage(label='Trash')
    kafka_multitopic_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Multitopic pipeline Maximum batch size threshold')\
        .configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # First test checking Max Batch Size is reached
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        # Start Pipeline.
        snapshot = sdc_executor.capture_snapshot(kafka_consumer_pipeline,
                                                 start_pipeline=True,
                                                 batches=num_batches,
                                                 batch_size=10).snapshot
        records_fields = []

        for snapshot_batch in snapshot.snapshot_batches:
            for value in snapshot_batch[kafka_consumer_pipeline[0].instance_name].output_lanes.values():
                for record in value:
                    records_fields.append(str(record.field['text']))

        assert expected == records_fields

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline, force=True)


# SDC-10897: Kafka setting for Batch Wait Time and Max Batch Size not working in conjunction
@cluster('cdh', 'kafka')
@sdc_min_version('3.9.1')
def test_kafka_origin_batch_max_wait_time(sdc_builder, sdc_executor, cluster):
    """Check that retrieving messages from Kafka using Kafka Multitopic Consumer respects both the Batch Max Wait Time
    and the Max Batch Size. Batches are sent when the first of the two conditions is met. This test is checking that
    the Batch Max Wait Time condition is first met.

    Kafka Multitopic Consumer Origin pipeline with standalone connected to an rpc destination which is read in another
    pipeline:
        kafka_multitopic_consumer >> sdc_rpc_destination
        sdc_rpc_origin >> trash
    """

    messages = [f'message{i}' for i in range(1, 21)]
    expected = [f'message{i}' for i in range(1, 21)]

    kafka_consumer_group = get_random_string(string.ascii_letters, 10)

    sdc_rpc_id = get_random_string(string.ascii_letters, 10)

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(builder, cluster)

    produce_kafka_messages_list(kafka_multitopic_consumer.topic_list[0], cluster, messages, 'TEXT')

    kafka_multitopic_consumer.set_attributes(auto_offset_reset='EARLIEST',
                                             consumer_group=kafka_consumer_group,
                                             max_batch_size_in_records=100,
                                             batch_wait_time_in_ms=10)

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.sdc_rpc_connection.append('{}:{}'.format(sdc_executor.server_host, SDC_RPC_LISTENING_PORT))
    sdc_rpc_destination.sdc_rpc_id = sdc_rpc_id

    kafka_multitopic_consumer >> sdc_rpc_destination

    kafka_consumer_pipeline = builder.build(title='Kafka Multitopic pipeline Maximum batch wait time threshold')\
        .configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    # Build the rpc origin pipeline.
    builder = sdc_builder.get_pipeline_builder()
    sdc_rpc_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_sdcipc_SdcIpcDSource')
    sdc_rpc_origin.sdc_rpc_listening_port = SDC_RPC_LISTENING_PORT
    sdc_rpc_origin.sdc_rpc_id = sdc_rpc_id

    trash = builder.add_stage(label='Trash')

    sdc_rpc_origin >> trash

    rpc_origin_pipeline = builder.build('SDC RPC origin pipeline for kafka Max Batch Wait Time')

    sdc_executor.add_pipeline(rpc_origin_pipeline, kafka_consumer_pipeline)

    try:
        snapshot_command = sdc_executor.capture_snapshot(rpc_origin_pipeline,
                                                         start_pipeline=True,
                                                         batch_size=20,
                                                         wait=False)

        sdc_executor.get_pipeline_status(rpc_origin_pipeline).wait_for_status('RUNNING')

        start_kafka_pipeline_command = sdc_executor.start_pipeline(kafka_consumer_pipeline)

        start = time.time()
        start_kafka_pipeline_command.wait_for_pipeline_output_records_count(20)
        end = time.time()
        total_time = (end - start)
        assert total_time < 5.0

        snapshot = snapshot_command.wait_for_finished().snapshot

        records_fields = []

        for snapshot_batch in snapshot.snapshot_batches:
            for value in snapshot_batch[rpc_origin_pipeline[0].instance_name].output_lanes.values():
                for record in value:
                    records_fields.append(str(record.field['text']))

        assert expected == records_fields

    finally:
        status = sdc_executor.get_pipeline_status(kafka_consumer_pipeline).response.json().get('status')
        if status != 'STOPPED':
            sdc_executor.stop_pipeline(kafka_consumer_pipeline)
        status = sdc_executor.get_pipeline_status(rpc_origin_pipeline).response.json().get('status')
        if status != 'STOPPED':
            sdc_executor.stop_pipeline(rpc_origin_pipeline)


# SDC-13819: Kafka Multi-Topic Consumer refuses to ingest malformed records, rather than sending them to pipeline error handling
@cluster('cdh', 'kafka')
def test_kafka_origin_only_errors(sdc_builder, sdc_executor, cluster):
    """
    Ensure that the origin can read batches with only error records. We accomplish that by configuring JSON as a file
    format and pushing messages that are clearly not JSON.
    """
    builder = sdc_builder.get_pipeline_builder()
    origin = get_kafka_multitopic_consumer_stage(builder, cluster)
    # We explicitly read files as JSON
    origin.data_format = 'JSON'

    trash = builder.add_stage(label='Trash')
    origin >> trash

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(pipeline)
    try:
        # Produce three text messages (e.g. no JSON)
        produce_kafka_messages_list(origin.topic_list[0], cluster, ["A", "B", "C"], 'TEXT')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # No normal records should be read
        assert len(snapshot[origin].output) == 0
        # But we should see the 3 errors
        assert len(snapshot[origin].error_records) == 3
    finally:
        sdc_executor.stop_pipeline(pipeline)


def get_kafka_multitopic_consumer_stage(pipeline_builder, cluster):
    """Create and return a Kafka origin stage depending on execution mode for the pipeline."""
    cluster_version = cluster.version[3:]
    stages_library = cluster.kafka.standalone_stage_lib
    if isinstance(cluster, ClouderaManagerCluster):
        cdh_version_tuple = tuple(int(i) for i in cluster_version.split('.'))
        if cdh_version_tuple >= cloudera.EARLIEST_CDH_VERSION_WITH_KAFKA:
            stages_library = cluster.sdc_stage_libs[0]

    # Default on error action.
    pipeline_builder.add_error_stage('Discard')

    kafka_multitopic_consumer = pipeline_builder.add_stage('Kafka Multitopic Consumer',
                                                type='origin',
                                                library=stages_library)
    # Default stage configuration.
    kafka_multitopic_consumer.set_attributes(data_format='TEXT',
                                  batch_wait_time_in_ms=20000,
                                  topic_list=[get_random_string(string.ascii_letters, 10)])

    return kafka_multitopic_consumer


def produce_kafka_messages(topic, cluster, message, data_format):
    """Send basic messages to Kafka"""
    # Get Kafka producer
    producer = cluster.kafka.producer()

    basic_data_formats = ['XML', 'CSV', 'SYSLOG', 'NETFLOW', 'COLLECTD', 'BINARY', 'LOG', 'TEXT', 'JSON']

    # Write records into Kafka depending on the data_format.
    if data_format in basic_data_formats:
        producer.send(topic, message)

    producer.flush()


def produce_kafka_messages_list(topic, cluster, message_list, data_format):
    """Send basic messages from a list to Kafka"""
    # Get Kafka producer
    producer = cluster.kafka.producer()

    basic_data_formats = ['XML', 'CSV', 'SYSLOG', 'NETFLOW', 'COLLECTD', 'BINARY', 'LOG', 'TEXT', 'JSON']

    # Write records into Kafka depending on the data_format.
    if data_format in basic_data_formats:
        for message in message_list:
            producer.send(topic, message.encode())

    producer.flush()


def produce_kafka_messages_in_different_timestamp(topic, cluster, messages, data_format, num_messages_to_send_first):
    """send num_messages_to_send_first messages, sleep 30 seconds, then send the rest of the messages and return the
    timestamp value after the 30 seconds sleep (<= timestamp of first message in second batch and >= last message in
    first batch)
    """
    timestamp = -1
    if num_messages_to_send_first < len(messages):
        # Send first batch of messages.
        for i in range(0, num_messages_to_send_first):
            message = messages[i]
            produce_kafka_messages(topic, cluster, message.encode(), data_format)

        # Sleep for 30 seconds.
        time.sleep(30)
        timestamp = int(time.time() * 1000)

        # Send second batch of messages.
        for j in range(num_messages_to_send_first, len(messages)):
            message = messages[j]
            produce_kafka_messages(topic, cluster, message.encode(), data_format)

    return timestamp


def verify_kafka_origin_results(kafka_multitopic_consumer_pipeline, sdc_executor, message, data_format):
    """Start, stop pipeline and verify results using snapshot"""

    # Start Pipeline.
    snapshot_pipeline_command = sdc_executor.capture_snapshot(kafka_multitopic_consumer_pipeline,
                                                              start_pipeline=True,
                                                              wait=False)

    logger.debug('Finish the snapshot and verify')
    snapshot_command = snapshot_pipeline_command.wait_for_finished(timeout_sec=SNAPSHOT_TIMEOUT_SEC)
    snapshot = snapshot_command.snapshot

    basic_data_formats = ['XML', 'CSV', 'SYSLOG', 'COLLECTD', 'TEXT', 'JSON', 'AVRO', 'AVRO_WITHOUT_SCHEMA']

    # Verify snapshot data.
    if data_format in basic_data_formats:
        record_field = [record.field for record in snapshot[kafka_multitopic_consumer_pipeline[0].instance_name].output]
        assert message == [str(record_field[0]), str(record_field[1])]

    elif data_format == 'TEXT_TIMESTAMP':
        record_field = [record.field for record in snapshot[kafka_multitopic_consumer_pipeline[0].instance_name].output]
        record_header = [record.header for record in snapshot[kafka_multitopic_consumer_pipeline[0].instance_name].output]
        for element in record_header:
            logger.debug('ELEMENT: %s', element['values'])
            assert 'timestamp' in element['values']
            assert 'timestampType' in element['values']
        assert message == str(record_field[0])