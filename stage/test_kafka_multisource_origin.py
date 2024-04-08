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

import json
import logging
import string
import time

import pytest
from streamsets.testframework.environments import cloudera
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)

KAFKA_LIBRARIES_THAT_DONT_SUPPORT_HEADERS = ['streamsets-datacollector-apache-kafka_0_9-lib',
                                             'streamsets-datacollector-apache-kafka_0_8-lib',
                                             'streamsets-datacollector-cdh_kafka_2_1-lib',
                                             'streamsets-datacollector-apache-kafka_0_10-lib']


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
@sdc_min_version('3.16.0')
def test_kafka_origin_including_timestamps(sdc_builder, sdc_executor, cluster):
    """Check that timestamp and timestamp type are included in record header. Verifies that for previous versions of
    kafka (< 0.10), a validation issue is thrown.
    """
    INPUT_DATA = 'Hello World from SDC & DPM!'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(pipeline_builder, cluster)
    kafka_multitopic_consumer.set_attributes(include_timestamps=True)
    wiretap = pipeline_builder.add_wiretap()
    kafka_multitopic_consumer >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    producer = cluster.kafka.producer()
    producer.send(kafka_multitopic_consumer.topic_list[0], INPUT_DATA.encode())
    producer.flush()

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        assert [INPUT_DATA] == [f'{record.field["text"]}' for record in wiretap.output_records]
        assert all('timestamp' in record.header.values for record in wiretap.output_records)
        assert all('timestampType' in record.header.values for record in wiretap.output_records)
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@cluster('cdh', 'kafka')
@sdc_min_version('3.7.0')
def test_kafka_origin_timestamp_offset_strategy(sdc_builder, sdc_executor, cluster):
    """Check that accessing a topic for first time using TIMESTAMP offset strategy retrieves messages
    which timestamp >= Auto Offset Reset Timestamp configuration value.
    """
    if any(stage_lib in cluster.sdc_stage_libs for stage_lib in KAFKA_LIBRARIES_THAT_DONT_SUPPORT_HEADERS):
        pytest.skip('Test only designed to run on Kafka version >= 0.10.1')

    INPUT_DATA = [f'message{i}' for i in range(5)]
    EXPECTED_OUTPUT = [{'text': f'message{i}'} for i in range(3, 5)]

    # Build the Kafka consumer pipeline with Standalone mode.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(pipeline_builder, cluster)

    # Send first 3 messages, save the timestamp to use, then send the last 2.
    for i in range(3):
        producer = cluster.kafka.producer()
        producer.send(kafka_multitopic_consumer.topic_list[0], INPUT_DATA[i].encode())
        producer.flush()
    time.sleep(30)
    timestamp = int(time.time() * 1000)
    for i in range(3, 5):
        producer = cluster.kafka.producer()
        producer.send(kafka_multitopic_consumer.topic_list[0], INPUT_DATA[i].encode())
        producer.flush()

    kafka_multitopic_consumer.set_attributes(auto_offset_reset='TIMESTAMP',
                                             auto_offset_reset_timestamp_in_ms=timestamp)

    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_multitopic_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert [record.field for record in wiretap.output_records] == EXPECTED_OUTPUT
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


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
    builder = sdc_builder.get_pipeline_builder()

    origin = get_kafka_multitopic_consumer_stage(builder, cluster)
    origin.batch_wait_time_in_ms = 100

    if Version(sdc_builder.version) < Version('3.7.0'):
        origin.configuration_properties = [{'key': 'auto.offset.reset', 'value': 'earliest'}]
    else:
        origin.auto_offset_reset = 'EARLIEST'

    delay = builder.add_stage('Delay')
    delay.delay_between_batches = 5 * 1000

    script = builder.add_stage('Jython Evaluator', type='processor')
    script.script = """1/${DIVISOR}
for record in sdc.records:
  try:
      sdc.output.write(record)
  except Exception as e:
      sdc.error.write(record, str(e))
"""

    wiretap = builder.add_wiretap()

    origin >> delay >> script >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    pipeline.configuration['executionMode'] = 'STANDALONE'
    pipeline.add_parameters(DIVISOR='0')

    sdc_executor.add_pipeline(pipeline)

    # Produce one message
    producer = cluster.kafka.producer()
    producer.send(origin.topic_list[0], 'Super Secret Message'.encode())
    producer.flush()

    try:
        # Start our pipeline - it should fail
        sdc_executor.start_pipeline(pipeline, runtime_parameters={'DIVISOR': 0}).wait_for_status('RUN_ERROR',
                                                                                                 ignore_errors=True)

        # Adding second message so that the topic have at least one new message, so that getting an older
        # versions won't time out but returns immediately.
        producer = cluster.kafka.producer()
        producer.send(origin.topic_list[0], 'Not So Super Secret Message'.encode())
        producer.flush()

        assert len(wiretap.output_records) == 0

        # Now run the pipeline second time and it should succeed
        sdc_executor.start_pipeline(pipeline, runtime_parameters={'DIVISOR': 1})
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2, timeout_sec=120)

        # Now this should still read both records
        records = wiretap.output_records
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
    builder = sdc_builder.get_pipeline_builder()

    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(builder, cluster)

    if Version(sdc_builder.version) < Version('3.7.0'):
        kafka_multitopic_consumer.configuration_properties = [{'key': 'auto.offset.reset', 'value': 'earliest'}]
    else:
        kafka_multitopic_consumer.auto_offset_reset = 'EARLIEST'

    wiretap = builder.add_wiretap()

    kafka_multitopic_consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(pipeline)

    # Produce 5 messages
    messages = [f'message{i}' for i in range(0, 5)]
    produce_kafka_messages_list(kafka_multitopic_consumer.topic_list[0], cluster, messages, 'TEXT')

    try:
        # Start the pipeline, read one batch and stop.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 5, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        # Check if the pipeline processed 5 records
        records = [f'{record.field["text"]}' for record in wiretap.output_records]
        assert len(records) == 5
        assert sorted(messages) == sorted(records)

        # Produce another 3 messages
        messages2 = [f'message{i}' for i in range(5, 8)]
        produce_kafka_messages_list(kafka_multitopic_consumer.topic_list[0], cluster, messages2, 'TEXT')

        # Resetting wiretap to clean up data from previous runs
        wiretap.reset()

        # Run the pipeline second time
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        #  2nd run should processed only 3 records
        records2 = [f'{record.field["text"]}' for record in wiretap.output_records]
        assert len(records2) == 3
        assert sorted(messages2) == sorted(records2)

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
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

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(builder, cluster)

    produce_kafka_messages_list(kafka_multitopic_consumer.topic_list[0], cluster, messages, 'TEXT')

    if Version(sdc_builder.version) < Version('3.7.0'):
        kafka_multitopic_consumer.configuration_properties = [{'key': 'auto.offset.reset', 'value': 'earliest'}]
    else:
        kafka_multitopic_consumer.auto_offset_reset = 'EARLIEST'

    kafka_multitopic_consumer.set_attributes(batch_wait_time_in_ms=30000)

    wiretap = builder.add_wiretap()
    kafka_multitopic_consumer >> wiretap.destination
    kafka_consumer_pipeline = builder.build(title='Kafka Multitopic pipeline Maximum batch size threshold') \
        .configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    # First test checking Max Batch Size is reached
    # Publish messages to Kafka and verify using wiretap if the same messages are received.
    # Start Pipeline.
    try:
        sdc_executor.start_pipeline(kafka_consumer_pipeline)
        sdc_executor.wait_for_pipeline_metric(kafka_consumer_pipeline,
                                              'input_record_count',
                                              num_batches * 10,
                                              timeout_sec=300)
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)

        assert sorted(expected) == sorted([str(record.field['text']) for record in wiretap.output_records])
    finally:
        if sdc_executor.get_pipeline_status(kafka_consumer_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(kafka_consumer_pipeline)


# SDC-10897: Kafka setting for Batch Wait Time and Max Batch Size not working in conjunction
@cluster('cdh', 'kafka')
@sdc_min_version('3.9.1')
def test_kafka_origin_batch_max_wait_time(sdc_builder, sdc_executor, cluster):
    """Check that retrieving messages from Kafka using Kafka Multitopic Consumer respects both the Batch Max Wait Time
    and the Max Batch Size. Batches are sent when the first of the two conditions is met. This test is checking that
    the Batch Max Wait Time condition is first met.
        kafka_multitopic_consumer >> wiretap
    """

    messages = [f'message{i}' for i in range(10, 30)]

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(builder, cluster)

    produce_kafka_messages_list(kafka_multitopic_consumer.topic_list[0], cluster, messages, 'TEXT')

    kafka_multitopic_consumer.set_attributes(auto_offset_reset='EARLIEST',
                                             max_batch_size_in_records=1000,
                                             batch_wait_time_in_ms=60_000)

    wiretap = builder.add_wiretap()

    kafka_multitopic_consumer >> wiretap.destination

    kafka_consumer_pipeline = builder.build().configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:

        start_kafka_pipeline_command = sdc_executor.start_pipeline(kafka_consumer_pipeline)

        start = time.time()
        start_kafka_pipeline_command.wait_for_pipeline_output_records_count(len(messages))
        end = time.time()
        total_time = (end - start)
        assert 65.0 > total_time > 55.0  # Execution time is around 60s which is what we expect it to wait

        assert len(messages) == len(wiretap.output_records)
        assert messages == sorted([str(record.field['text']) for record in wiretap.output_records])
    finally:
        status = sdc_executor.get_pipeline_status(kafka_consumer_pipeline).response.json().get('status')
        if status != 'STOPPED':
            sdc_executor.stop_pipeline(kafka_consumer_pipeline)


# SDC-13819: Kafka Multi-Topic Consumer refuses to ingest malformed records, rather than sending them to pipeline
# error handling
@cluster('cdh', 'kafka')
@sdc_min_version('3.15.0')
def test_kafka_origin_only_errors(sdc_builder, sdc_executor, cluster):
    """
    Ensure that the origin can read batches with only error records. We accomplish that by configuring JSON as a file
    format and pushing messages that are clearly not JSON.
    """
    builder = sdc_builder.get_pipeline_builder()
    origin = get_kafka_multitopic_consumer_stage(builder, cluster)
    # We explicitly read files as JSON
    origin.data_format = 'JSON'

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(pipeline)
    # Produce three text messages (e.g. no JSON)
    produce_kafka_messages_list(origin.topic_list[0], cluster, ["A", "B", "C"], 'TEXT')
    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
        sdc_executor.stop_pipeline(pipeline)

        # No normal records should be read
        assert len(wiretap.output_records) == 0
        # But we should see the 3 errors
        assert len(wiretap.error_records) == 3
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


# SDC-14063: Kafka multitopic consumer commits offset in a preview mode
@cluster('cdh', 'kafka')
def test_kafka_preview_not_committing_offset(sdc_builder, sdc_executor, cluster):
    """Ensure that preview won't commit offset when reading from Kafka."""
    builder = sdc_builder.get_pipeline_builder()
    origin = get_kafka_multitopic_consumer_stage(builder, cluster)

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(pipeline)
    try:
        # Produce three text messages and confirm that preview can see them
        produce_kafka_messages_list(origin.topic_list[0], cluster, ["A", "B", "C"], 'TEXT')
        preview = sdc_executor.run_pipeline_preview(pipeline, timeout=30_000).wait_for_finished().preview
        assert [record.field['text'] for record in preview[origin].output] == ['A', 'B', 'C']

        # Produce additional 3 messages and ensure that normal pipeline run can see all 6 records
        # (e.g. no commit after preview)
        produce_kafka_messages_list(origin.topic_list[0], cluster, ["D", "E", "F"], 'TEXT')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        assert [record.field['text'] for record in wiretap.output_records] == ['A', 'B', 'C', 'D', 'E', 'F']
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@cluster('cdh', 'kafka')
@sdc_min_version('3.16.1')
def test_kafka_shift_offset(sdc_builder, sdc_executor, cluster):
    """Ensure that no data is lost when committed offset is shifted from batch size"""
    builder = sdc_builder.get_pipeline_builder()
    origin = get_kafka_multitopic_consumer_stage(builder, cluster)

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Produce 200 messages, not enough for one batch
        produce_kafka_messages_list(origin.topic_list[0], cluster, [f'{i}' for i in range(200)], 'TEXT')

        sdc_executor.start_pipeline(pipeline)

        # Produce 1000 messages, size of a batch. With the previous 200 there will be a shift in the offset
        produce_kafka_messages_list(origin.topic_list[0], cluster, [f'{i}' for i in range(200, 1200)], 'TEXT')

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1200, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        actual = sorted([f'{record.field["text"]}' for record in wiretap.output_records])
        assert sorted([f'{i}' for i in range(1200)]) == actual
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@cluster('cdh', 'kafka')
@sdc_min_version('3.16.2')
def test_kafka_topic_with_hyphen(sdc_builder, sdc_executor, cluster):
    INPUT_DATA = 'Hello World from SDC & DPM!'
    TOPIC_NAME = f'{get_random_string(string.ascii_lowercase, 5)}-{get_random_string(string.ascii_lowercase, 5)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [TOPIC_NAME])
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_multitopic_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)
    produce_kafka_messages(kafka_multitopic_consumer.topic_list[0], cluster, INPUT_DATA.encode(), 'TEXT')

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert [INPUT_DATA] == [f'{record.field["text"]}' for record in wiretap.output_records]
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


# SDC-16127: KafkaMultiConsumer: Does not handle messages with null payload
@cluster('cdh', 'kafka')
def test_kafka_multiconsumer_null_payload(sdc_builder, sdc_executor, cluster):
    """Check that retrieving a message with null payload from Kafka using Kafka Multitopic Consumer does not return anything.
    The message should be written to Kafka without using SDC.

    Kafka Multitopic Consumer Origin pipeline with standalone mode:
        kafka_multitopic_consumer >> wiretap.destination
    """
    message = json.dumps({'abc': '123'})
    expected_error_message = 'KAFKA_74 - Tombstone message: payload is null'

    contains_header = cluster.sdc_stage_libs not in KAFKA_LIBRARIES_THAT_DONT_SUPPORT_HEADERS

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(pipeline_builder, cluster)
    kafka_multitopic_consumer.set_attributes(data_format='JSON')
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_multitopic_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    # Add messages, one of them with null payload
    producer = cluster.kafka.producer()
    producer.send(kafka_multitopic_consumer.topic_list[0], message.encode())
    producer.send(kafka_multitopic_consumer.topic_list[0], value=None, key='abc'.encode())
    producer.flush()

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Check the output records, there should be only 1 message, since the null should be discarded
        output_records = [record.field for record in wiretap.output_records]
        assert len(output_records) == 1
        # Check that the null record did generate an error record
        output_errors = wiretap.error_records
        assert len(output_errors) == 1

        error_record = output_errors[0]
        assert error_record.header['errorMessage'] == expected_error_message
        assert 'abc' in str(error_record)  # 'abc' is the key set in the message, making it as messageKey

        if contains_header:
            assert error_record.header.values['topic'] == kafka_multitopic_consumer.topic_list[0]
            assert error_record.header.values['offset'] == '1'
            assert error_record.header.values['partition'] == '0'
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.11.0')
@cluster('cdh', 'kafka')
def test_kafka_multiconsumer_null_message_key(sdc_builder, sdc_executor, cluster):
    """Check that retrieving a message with null message key and null payload does not throw and unexpected
    NullPointerException (NPE). It creates an error record instead.

    Kafka Multitopic Consumer Origin pipeline with standalone mode:
        kafka_multitopic_consumer >> wiretap.destination
    """
    message = json.dumps({'abc': '123'})
    expected_error_message = 'KAFKA_79 - Tombstone message: payload and message key are null'

    contains_header = cluster.sdc_stage_libs not in KAFKA_LIBRARIES_THAT_DONT_SUPPORT_HEADERS

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(pipeline_builder, cluster)
    kafka_multitopic_consumer.set_attributes(data_format='JSON')
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_multitopic_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    brokers = ','.join(cluster.kafka.brokers)
    topic = kafka_multitopic_consumer.topic_list[0]
    logger.debug(f"topic: {topic}")

    # Add messages
    #
    # 1. The first one with null message key and null payload
    # It has to be done from the shell with a kafkacat command (can't be done with kafka-python)
    # 1.1. Force to install 'kafkacat' if not installed
    kafkacat_install_command = 'apt-get install kafkacat -y'
    install_response = execute_shell_command(sdc_executor, kafkacat_install_command)
    # Checking there were no issues with the installation
    assert 'kafkacat' not in install_response.stderr.rstrip(), f"Issues when running '{kafkacat_install_command}'"
    # 1.2. Produce the new message
    kafkacat_produce_command = f'echo ":" | kafkacat -b {brokers} -t {topic} -P -Z -K:'
    produce_response = execute_shell_command(sdc_executor, kafkacat_produce_command)
    # Check the message with null payload and null message key was sent successfully
    assert produce_response.stdout.rstrip() == '' and produce_response.stderr.rstrip() == '', \
        f"Issues when running '{kafkacat_produce_command}'"

    # 2. The second one has a proper message key and payload
    producer = cluster.kafka.producer()
    producer.send(topic, message.encode())
    producer.flush()

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Check the output records, there should be only 1 message, since the null should be discarded
        output_records = wiretap.output_records
        assert len(output_records) == 1
        # Check that the null record did generate an error record
        output_errors = wiretap.error_records
        assert len(output_errors) == 1

        error_record = output_errors[0]
        assert error_record.header['errorMessage'] == expected_error_message

        if contains_header:
            assert error_record.header.values['topic'] == topic
            assert error_record.header.values['offset'] == '0'
            assert error_record.header.values['partition'] == '0'
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_json_array_error(sdc_builder, sdc_executor, cluster):
    """SDC-15723: Improve behavior on JSON type missconfiguration. When user configures JSON
     parser to parse "Array" but the data aren't array"""

    message = '(not_a_valid_json)'
    expected_error_message = 'KAFKA_37 - Cannot parse record from message : Cannot parse JSON from record'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(pipeline_builder, cluster)
    kafka_multitopic_consumer.set_attributes(data_format='JSON',
                                             json_content='ARRAY_OBJECTS')
    wiretap = pipeline_builder.add_wiretap()
    kafka_multitopic_consumer >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    producer.send(kafka_multitopic_consumer.topic_list[0], message.encode())
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)
        assert 1 == len(wiretap.error_records)
        error_message = wiretap.error_records[0].header['errorMessage'].split("'")
        received_error = error_message[0] + error_message[2]
        assert expected_error_message == received_error
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@cluster('cdh', 'kafka')
@sdc_min_version('5.0.0')
def test_kafka_topic_with_pattern(sdc_builder, sdc_executor, cluster):
    topic_data_1 = 'Info from Topic 1'
    topic_data_2 = 'Info from Topic 2'
    topic_prefix = f'{get_random_string(string.ascii_lowercase, 5)}'
    topic_1 = f'{topic_prefix}_1'
    topic_2 = f'{topic_prefix}_2'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic_1, topic_2])
    kafka_multitopic_consumer.set_attributes(topic_subscription_type="PATTERN", topic_pattern=f'{topic_prefix}.*')
    wiretap = pipeline_builder.add_wiretap()
    kafka_multitopic_consumer >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)
    produce_kafka_messages(topic_1, cluster, topic_data_1.encode(), 'TEXT')
    produce_kafka_messages(topic_2, cluster, topic_data_2.encode(), 'TEXT')

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        output_records = [f'{record.field["text"]}' for record in wiretap.output_records]
        assert len(output_records) == 2
        assert topic_data_1 and topic_data_2 in output_records
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@cluster('cdh', 'kafka')
@sdc_min_version('5.0.0')
def test_kafka_topic_with_pattern_new_topic_during_pipeline_run(sdc_builder, sdc_executor, cluster):
    topic_data_1 = 'Info from Topic 1'
    topic_data_2 = 'Info from Topic 2'
    topic_data_3 = 'Info from Topic 3'
    topic_prefix = f'{get_random_string(string.ascii_lowercase, 5)}'
    topic_1 = f'{topic_prefix}_1'
    topic_2 = f'{topic_prefix}_2'
    topic_3 = f'{topic_prefix}_3'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = get_kafka_multitopic_consumer_stage(pipeline_builder, cluster,
                                                                    [topic_1, topic_2, topic_3])
    kafka_multitopic_consumer.set_attributes(topic_subscription_type="PATTERN", topic_pattern=f'{topic_prefix}.*',
                                             metadata_refresh_time_in_ms=10_000)
    wiretap = pipeline_builder.add_wiretap()
    kafka_multitopic_consumer >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)
    produce_kafka_messages(topic_1, cluster, topic_data_1.encode(), 'TEXT')
    produce_kafka_messages(topic_2, cluster, topic_data_2.encode(), 'TEXT')

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2, timeout_sec=120)
        produce_kafka_messages(topic_3, cluster, topic_data_3.encode(), 'TEXT')
        time.sleep(20)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        output_records = [f'{record.field["text"]}' for record in wiretap.output_records]
        assert len(output_records) == 3
        assert all(topic_data in output_records for topic_data in [topic_data_1, topic_data_2, topic_data_3])
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


def get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, topic_list=None):
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

    if topic_list is None:
        topic_list = [get_random_string(string.ascii_letters, 10)]
    # Default stage configuration.
    kafka_multitopic_consumer.set_attributes(data_format='TEXT',
                                             batch_wait_time_in_ms=20000,
                                             consumer_group=get_random_string(string.ascii_letters, 10),
                                             topic_list=topic_list)

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


def execute_shell_command(sdc_executor, command):
    response = sdc_executor.execute_shell(command)
    logger.debug(f"Output after executing '{command}' shell command: %s", response)
    return response
