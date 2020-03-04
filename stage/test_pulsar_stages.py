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
import datetime

from pulsar import MessageId
from stage.utils.utils_xml import get_xml_output_field
from streamsets.testframework.markers import pulsar, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SNAPSHOT_TIMEOUT_SEC = 120


def get_pulsar_consumer_stage(pipeline_builder, topic, initial_offset):
    """Create and return a Pulsar Consumer origin stage depending on execution mode for the pipeline."""
    pulsar_consumer = pipeline_builder.add_stage('Pulsar Consumer',
                                                 type='origin')
    pulsar_consumer.set_attributes(data_format='TEXT',
                                   batch_wait_time_in_ms=20000,
                                   topic=topic,
                                   consumer_name='consumer',
                                   initial_offset=initial_offset)
    return pulsar_consumer


def get_pulsar_producer_stage(pipeline_builder, topic):
    """Create and return a Pulsar Producer origin stage depending on execution mode for the pipeline."""

    pulsar_producer = pipeline_builder.add_stage('Pulsar Producer',
                                                 type='destination')
    pulsar_producer.set_attributes(data_format='TEXT',
                                   text_field_path='/text',
                                   topic=topic)
    return pulsar_producer


def verify_results(pulsar_consumer_pipeline, sdc_executor, number_of_messages, message, data_format):
    """Take a snapshot from sdc Pulsar origin pipeline to verify results."""

    snapshot_pipeline_command = sdc_executor.capture_snapshot(pulsar_consumer_pipeline, start_pipeline=True, wait=False)
    verify_results_snapshot(pulsar_consumer_pipeline, snapshot_pipeline_command, number_of_messages, message,
                            data_format)


def verify_results_snapshot(pulsar_consumer_pipeline, snapshot_pipeline_command, number_of_messages,
                            message, data_format):
    """Take a snapshot from sdc Pulsar origin pipeline to verify results."""

    logger.debug('Finish the snapshot and verify')
    logger.debug('Time: %s', datetime.datetime.now())
    snapshot_command = snapshot_pipeline_command.wait_for_finished(timeout_sec=SNAPSHOT_TIMEOUT_SEC)
    snapshot = snapshot_command.snapshot
    record_field = [record.field for record in snapshot[pulsar_consumer_pipeline[0].instance_name].output]

    assert record_field is not None
    number_of_messages_received = 0
    for record in record_field:
        if data_format == 'TEXT':
            assert record['text'] == message
        elif data_format == 'JSON':
            assert record['Name'] == message['Name']
            assert record['Job'] == message['Job']
        elif data_format == 'XML':
            output_record = get_xml_output_field(pulsar_consumer_pipeline[0], record, 'text')
            assert output_record['value'] == message
        elif data_format == 'DELIMITED':
            logger.debug(record)
            logger.debug(message)
            assert list(record.values())[0] == message.split(',')[0]
            assert list(record.values())[1] == message.split(',')[1]
        number_of_messages_received += 1

    assert number_of_messages_received == number_of_messages


def get_dev_raw_data_source(pipeline_builder, raw_data, data_format='JSON', stop_after_first_batch=False):
    """ Adds a 'Dev Raw Data Source' stage to pipeline_builder and sets raw_data, data_format and
    stop_after_first_batch properties of that stage. Returns the added stage"""

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = data_format
    dev_raw_data_source.raw_data = raw_data
    dev_raw_data_source.stop_after_first_batch = stop_after_first_batch

    return dev_raw_data_source


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_consumer(sdc_builder, sdc_executor, pulsar):
    """Test for Pulsar consumer origin stage. We do so by publishing data to a test topic using Pulsar client and
    having a pipeline which reads that data using Pulsar consumer origin stage. Data is then asserted for what is
    published at Pulsar client and what we read in the pipeline snapshot. The pipeline looks like:

    Pulsar Consumer pipeline:
        pulsar_consumer >> trash
    """
    sub_name = get_random_string(string.ascii_letters, 10)
    consumer_name = get_random_string(string.ascii_letters, 10)
    topic_name = get_random_string(string.ascii_letters, 10)
    max_records = 100
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage('Pulsar Consumer').set_attributes(subscription_name=sub_name,
                                                                          consumer_name=consumer_name,
                                                                          topic=topic_name,
                                                                          data_format='TEXT',
                                                                          max_batch_size_in_records=max_records)
    trash = builder.add_stage('Trash')

    pulsar_consumer >> trash

    consumer_origin_pipeline = builder.build(title='Pulsar Consumer pipeline').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    client = pulsar.client
    admin = pulsar.admin
    try:
        snapshot_command = sdc_executor.capture_snapshot(consumer_origin_pipeline, start_pipeline=True, wait=False)

        producer = client.create_producer(topic_name)
        for _ in range(max_records):
            producer.send(input_text.encode())

        snapshot = snapshot_command.wait_for_finished().snapshot
        sdc_executor.stop_pipeline(consumer_origin_pipeline)
        output_records = [record.field['text'] for record in snapshot[pulsar_consumer.instance_name].output]
        assert output_records == [input_text] * 10 # 10 hardcoded for snapshot batch size
    finally:
        producer.close() # all producer/consumers need to be closed before topic can be deleted without force
        client.close()
        admin.delete_topic(producer.topic())


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_producer(sdc_builder, sdc_executor, pulsar):
    """Test for Pulsar producer target stage. We do so by publishing data to a test topic using Pulsar producer
    stage and then read the data from that topic using Pulsar client. We assert the data from the client to what has
    been injected by the producer pipeline. The pipeline looks like:

    Pulsar Producer pipeline:
        dev_raw_data_source >> pulsar_producer
    """
    topic_name = get_random_string(string.ascii_letters, 10)
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=input_text)
    pulsar_producer = builder.add_stage('Pulsar Producer')
    pulsar_producer.set_attributes(topic=topic_name, data_format='TEXT')

    dev_raw_data_source >> pulsar_producer
    producer_dest_pipeline = builder.build(title='Pulsar Producer pipeline').configure_for_environment(pulsar)

    # add pipeline and capture pipeline messages to assert
    sdc_executor.add_pipeline(producer_dest_pipeline)
    sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_batch_count(10)
    sdc_executor.stop_pipeline(producer_dest_pipeline)

    history = sdc_executor.get_pipeline_history(producer_dest_pipeline)
    msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
    logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

    client = pulsar.client
    admin = pulsar.admin
    try:
        reader = client.create_reader(topic_name, MessageId.earliest)
        msgs_received = []
        while reader.has_message_available():
            msgs_received.append(reader.read_next().data().decode().strip()) # strip to remove newlines
    finally:
        reader.close() # reader needs to be closed before topic can be deleted without force
        client.close()
        admin.delete_topic(reader.topic())

    logger.debug('Number of messages received from Pulsar = %d', len(msgs_received))
    assert msgs_received == [input_text] * msgs_sent_count


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_text(sdc_builder, sdc_executor, pulsar):
    """Write simple text messages into pulsar and confirm that pulsar successfully reads them.
        Specifically, this would look like:

        Pulsar Producer Destination pipeline with standalone mode:
            dev_raw_data_soruce >> pulsar_producer

        Pulsar Consumer Origin pipeline with standalone mode:
            pulsar_consumer >> trash
        """
    message = 'pulsar message martin xavi'
    data = {'text': message}
    json_data = json.dumps(data)

    topic = get_random_string(string.ascii_letters, 10)

    # Build the Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder, json_data)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, topic)
    pulsar_producer.set_attributes(compresion_type='NONE')

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone Text').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic, 'LATEST')
    pulsar_consumer.set_attributes(subscription_type='FAILOVER',
                                   consumer_queue_size=1000)

    trash = pulsar_consumer_pipeline_builder.add_stage(label='Trash')
    pulsar_consumer >> trash
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    try:
        number_of_messages = 10
        # Publish messages to Pulsar and verify using snapshot if the same messages are received.

        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_pipeline_batch_count(1)

        verify_results(pulsar_consumer_pipeline,
                       sdc_executor,
                       number_of_messages,
                       message, 'TEXT')
    finally:
        sdc_executor.stop_pipeline(pulsar_producer_pipeline, wait=True, force=True)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline, wait=True, force=True)


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_json(sdc_builder, sdc_executor, pulsar):
    """Write simple json messages into pulsar and confirm that pulsar successfully reads them.
        Specifically, this would look like:

        Pulsar Producer Destination pipeline with standalone mode:
            dev_raw_data_soruce >> pulsar_producer

        Pulsar Consumer Origin pipeline with standalone mode:
            pulsar_consumer >> trash

        Producer includes topic name in message field
        Producer partition type = Round Robin


        """

    topic = get_random_string(string.ascii_letters, 10)
    message = {'Topic': topic, 'Name': 'Xavi', 'Job': 'Developer'}
    json_data = json.dumps(message)

    # Build the Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder, json_data)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, '${record:value(\'/Topic\')}')
    pulsar_producer.set_attributes(data_format='JSON',
                                   partition_type='ROUND_ROBIN')

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone JSON').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic,
                                                'EARLIEST')
    pulsar_consumer.set_attributes(subscription_type='SHARED',
                                   consumer_queue_size=10000,
                                   data_format='JSON')

    trash = pulsar_consumer_pipeline_builder.add_stage(label='Trash')
    pulsar_consumer >> trash
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone JSON').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    try:
        number_of_messages = 10
        # Publish messages to Pulsar and verify using snapshot if the same messages are received.

        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_pipeline_batch_count(1)

        verify_results(pulsar_consumer_pipeline,
                       sdc_executor,
                       number_of_messages,
                       message, 'JSON')
    finally:
        sdc_executor.stop_pipeline(pulsar_producer_pipeline, wait=True, force=True)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline, wait=True, force=True)


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_xml(sdc_builder, sdc_executor, pulsar):
    """Write simple XML messages into pulsar and confirm that pulsar successfully reads them.
        Specifically, this would look like:

        Pulsar Producer Destination pipeline with standalone mode:
            dev_raw_data_soruce text message >> pulsar_producer XML

        Pulsar Consumer Origin pipeline with standalone mode:
            pulsar_consumer XML >> trash
        """
    message = "Text message that will be converted in XML"

    topic = get_random_string(string.ascii_letters, 10)

    # Build the Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder, message, data_format='TEXT')

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, topic)
    pulsar_producer.set_attributes(data_format='XML',
                                   partition_type='SINGLE',
                                   hashing_scheme='JAVA_STRING_HASH',
                                   message_key='12345',
                                   compresion_type='ZLIB')

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone XML').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic, 'EARLIEST')
    pulsar_consumer.set_attributes(data_format='XML',
                                   consumer_queue_size=1)

    trash = pulsar_consumer_pipeline_builder.add_stage(label='Trash')
    pulsar_consumer >> trash
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone XML').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    try:
        number_of_messages = 10
        # Publish messages to Pulsar and verify using snapshot if the same messages are received.
        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_pipeline_batch_count(1)

        verify_results(pulsar_consumer_pipeline,
                       sdc_executor,
                       number_of_messages,
                       message, 'XML')
    finally:
        sdc_executor.stop_pipeline(pulsar_producer_pipeline, wait=True, force=True)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline, wait=True, force=True)


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_topics_list(sdc_builder, sdc_executor, pulsar):
    """Write simple json messages into pulsar and confirm that pulsar successfully reads them.
        Specifically, this would look like:

        Pulsar Producer Destination pipeline with standalone mode:
            dev_raw_data_soruce >> pulsar_producer_1

        Pulsar Producer Destination pipeline with standalone mode:
            dev_raw_data_soruce >> pulsar_producer_2


        Pulsar Consumer Origin pipeline with standalone mode:
            pulsar_consumer_1_2 >> trash
        """

    message = {'Name': 'Xavi', 'Job': 'Developer'}
    json_data = json.dumps(message)

    topic1 = get_random_string(string.ascii_letters, 10)
    topic2 = get_random_string(string.ascii_letters, 10)

    # Build the First Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder1 = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder1.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder1, json_data,
                                                  stop_after_first_batch=True)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder1, topic1)
    pulsar_producer.set_attributes(data_format='JSON',
                                   hashing_scheme='MUMUR3_32HASH',
                                   message_key='12345',
                                   enable_batching=False,
                                   async_send=False)

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline1 = pulsar_producer_pipeline_builder1.build(
        title='Pulsar Producer Standalone List Topics 1').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline1)

    # Build the Second Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder2 = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder2.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder2, json_data,
                                                  stop_after_first_batch=True)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder2, topic2)
    pulsar_producer.set_attributes(data_format='JSON',
                                   enable_batching=False,
                                   async_send=False)

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline2 = pulsar_producer_pipeline_builder2.build(
        title='Pulsar Producer Standalone List Topics 2').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline2)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic1, 'EARLIEST')
    topics_list = [topic1, topic2]

    pulsar_consumer.set_attributes(topics_selector='TOPICS_LIST',
                                   topics_list=topics_list,
                                   subscription_type='EXCLUSIVE',
                                   ead_compacted=False,
                                   data_format='JSON')

    trash = pulsar_consumer_pipeline_builder.add_stage(label='Trash')
    pulsar_consumer >> trash
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone List Topics').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    try:
        number_of_messages = 2
        # Publish messages to Pulsar starting two pipelines
        # and verify using snapshot if the two same messages are received.

        snapshot_pipeline_command = sdc_executor.capture_snapshot(pulsar_consumer_pipeline, start_pipeline=True,
                                                                  wait=False)

        sdc_executor.start_pipeline(pulsar_producer_pipeline1, wait=True)
        sdc_executor.get_pipeline_status(pulsar_producer_pipeline1).wait_for_status('FINISHED')

        sdc_executor.start_pipeline(pulsar_producer_pipeline2, wait=True)
        sdc_executor.get_pipeline_status(pulsar_producer_pipeline2).wait_for_status('FINISHED')

        verify_results_snapshot(pulsar_consumer_pipeline,
                                snapshot_pipeline_command,
                                number_of_messages,
                                message, 'JSON')
    finally:

        sdc_executor.stop_pipeline(pulsar_consumer_pipeline, wait=True, force=True)


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_topics_pattern(sdc_builder, sdc_executor, pulsar):
    """Write simple json messages into pulsar and confirm that pulsar successfully reads them.
        Specifically, this would look like:

        Pulsar Producer Destination pipeline with standalone mode:
            dev_raw_data_soruce >> pulsar_producer_1

        Pulsar Producer Destination pipeline with standalone mode:
            dev_raw_data_soruce >> pulsar_producer_2


        Pulsar Consumer Origin pipeline with standalone mode:
            pulsar_consumer_1_2 >> trash

        The name of topics 1 and topics 2 begins with SDC

        A pulsar_consumer_3 is included. Its name does not begin with SDC
        """

    message = 'Xavi,Developer, Very Long Message In Order To Spend More Time Sending It'

    topic1 = 'SDC' + get_random_string(string.ascii_letters, 10)
    topic2 = 'XXX' + get_random_string(string.ascii_letters, 10)
    topic3 = 'SDC' + get_random_string(string.ascii_letters, 10)

    # Build the First Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder1 = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder1.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder1, message, data_format='DELIMITED',
                                                  stop_after_first_batch=True)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder1, topic1)
    pulsar_producer.set_attributes(data_format='DELIMITED',
                                   enable_batching=False,
                                   async_send=False)

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline1 = pulsar_producer_pipeline_builder1.build(
        title='Pulsar Producer Standalone Pattern 1').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline1)

    # Build the Second Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder2 = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder2.add_error_stage('Discard')

    dev_raw_data_source2 = get_dev_raw_data_source(pulsar_producer_pipeline_builder2, message, data_format='DELIMITED',
                                                   stop_after_first_batch=True)

    pulsar_producer2 = get_pulsar_producer_stage(pulsar_producer_pipeline_builder2, topic2)
    pulsar_producer2.set_attributes(data_format='DELIMITED',
                                    enable_batching=False,
                                    async_send=False)

    dev_raw_data_source2 >> pulsar_producer2
    pulsar_producer_pipeline2 = pulsar_producer_pipeline_builder2.build(
        title='Pulsar Producer Standalone Pattern 2').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline2)

    # Build the Third Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder3 = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder3.add_error_stage('Discard')

    dev_raw_data_source3 = get_dev_raw_data_source(pulsar_producer_pipeline_builder3, message, data_format='DELIMITED',
                                                   stop_after_first_batch=True)

    pulsar_producer3 = get_pulsar_producer_stage(pulsar_producer_pipeline_builder3, topic3)
    pulsar_producer3.set_attributes(data_format='DELIMITED',
                                    enable_batching=False,
                                    async_send=False)

    dev_raw_data_source3 >> pulsar_producer3
    pulsar_producer_pipeline3 = pulsar_producer_pipeline_builder3.build(
        title='Pulsar Producer Standalone Pattern 3').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline3)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic1, 'EARLIEST')
    pulsar_consumer.set_attributes(topics_selector='TOPICS_PATTERN',
                                   topics_pattern='persistent://public/default/(' + topic1 + '|' + topic3 + ')',
                                   subscription_type='EXCLUSIVE',
                                   read_compacted=False,
                                   data_format='DELIMITED')

    trash = pulsar_consumer_pipeline_builder.add_stage(label='Trash')
    pulsar_consumer >> trash
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone Pattern').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    try:
        number_of_messages = 2
        # Publish messages to Pulsar starting three pipelines
        # and verify using snapshot if the two same messages
        # corresponding to topics SDC* are received.

        snapshot_pipeline_command = sdc_executor.capture_snapshot(pulsar_consumer_pipeline, start_pipeline=True,
                                                                  wait=False)

        sdc_executor.start_pipeline(pulsar_producer_pipeline1, wait=True)
        sdc_executor.get_pipeline_status(pulsar_producer_pipeline1).wait_for_status('FINISHED')
        sdc_executor.start_pipeline(pulsar_producer_pipeline2, wait=True)
        sdc_executor.get_pipeline_status(pulsar_producer_pipeline2).wait_for_status('FINISHED')
        sdc_executor.start_pipeline(pulsar_producer_pipeline3, wait=True)
        sdc_executor.get_pipeline_status(pulsar_producer_pipeline3).wait_for_status('FINISHED')

        verify_results_snapshot(pulsar_consumer_pipeline,
                                snapshot_pipeline_command,
                                number_of_messages,
                                message, 'DELIMITED')
    finally:
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline, wait=True, force=True)


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_json_tls_encrypt(sdc_builder, sdc_executor, pulsar):
    """Write simple json messages into pulsar and confirm that pulsar successfully reads them.
        Specifically, this would look like:

        Pulsar Producer Destination pipeline with standalone mode:
            dev_raw_data_soruce >> pulsar_producer

        Pulsar Consumer Origin pipeline with standalone mode:
            pulsar_consumer >> trash

        Producer includes topic name in message field
        Producer partition type = Round Robin


        """

    topic = get_random_string(string.ascii_letters, 10)
    message = {'Topic': topic, 'Name': 'Xavi', 'Job': 'Developer'}
    json_data = json.dumps(message)

    # Build the Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder, json_data)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, '${record:value(\'/Topic\')}')
    pulsar_producer.set_attributes(data_format='JSON',
                                   partition_type='ROUND_ROBIN',
                                   compresion_type='LZ4',
                                   enable_tls=True)

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone JSON').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic,
                                                'EARLIEST')
    pulsar_consumer.set_attributes(subscription_type='SHARED',
                                   consumer_queue_size=10000,
                                   data_format='JSON',
                                   enable_tls=True)

    trash = pulsar_consumer_pipeline_builder.add_stage(label='Trash')
    pulsar_consumer >> trash
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone JSON').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    try:
        number_of_messages = 10
        # Publish messages to Pulsar and verify using snapshot if the same messages are received.

        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_pipeline_batch_count(1)

        verify_results(pulsar_consumer_pipeline,
                       sdc_executor,
                       number_of_messages,
                       message, 'JSON')
    finally:
        sdc_executor.stop_pipeline(pulsar_producer_pipeline, wait=True, force=True)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline, wait=True, force=True)


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_json_tls_mutual_auth(sdc_builder, sdc_executor, pulsar):
    """Write simple json messages into pulsar and confirm that pulsar successfully reads them.
        Specifically, this would look like:

        Pulsar Producer Destination pipeline with standalone mode:
            dev_raw_data_soruce >> pulsar_producer

        Pulsar Consumer Origin pipeline with standalone mode:
            pulsar_consumer >> trash

        Producer includes topic name in message field
        Producer partition type = Round Robin


        """

    topic = get_random_string(string.ascii_letters, 10)
    message = {'Topic': topic, 'Name': 'Xavi', 'Job': 'Developer'}
    json_data = json.dumps(message)

    # Build the Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder, json_data)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, '${record:value(\'/Topic\')}')
    pulsar_producer.set_attributes(data_format='JSON',
                                   partition_type='ROUND_ROBIN',
                                   compresion_type='LZ4',
                                   nable_tls=True,
                                   enable_mutual_authentication=True)

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone JSON').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic,
                                                'EARLIEST')
    pulsar_consumer.set_attributes(subscription_type='SHARED',
                                   consumer_queue_size=10000,
                                   data_format='JSON',
                                   enable_tls=True,
                                   enable_mutual_authentication=True)

    trash = pulsar_consumer_pipeline_builder.add_stage(label='Trash')
    pulsar_consumer >> trash
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone JSON').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    try:
        number_of_messages = 10
        # Publish messages to Pulsar and verify using snapshot if the same messages are received.

        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_pipeline_batch_count(1)

        verify_results(pulsar_consumer_pipeline,
                       sdc_executor,
                       number_of_messages,
                       message, 'JSON')
    finally:
        sdc_executor.stop_pipeline(pulsar_producer_pipeline, wait=True, force=True)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline, wait=True, force=True)
