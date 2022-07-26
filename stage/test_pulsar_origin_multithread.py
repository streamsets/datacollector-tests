# Copyright 2021 StreamSets Inc.
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
import pytest

from pulsar import schema
import xml.etree.ElementTree as ET
from streamsets.testframework.markers import pulsar, sdc_min_version
from streamsets.testframework.utils import get_random_string

from .utils.utils_pulsar import disable_auto_update_schema, create_topic_with_schema, set_schema_validation_enforced, \
    enable_auto_update_schema

logger = logging.getLogger(__name__)

PULSAR_PUSH_ORIGIN_STAGE_NAME = "com_streamsets_pipeline_stage_origin_pulsarmultithread_PulsarDSourceMultiThread"


def get_pulsar_producer_stage(pipeline_builder, topic):
    """Create and return a Pulsar Producer origin stage depending on execution mode for the pipeline."""

    pulsar_producer = pipeline_builder.add_stage('Pulsar Producer',
                                                 type='destination')
    pulsar_producer.set_attributes(data_format='TEXT',
                                   text_field_path='/text',
                                   topic=topic)
    return pulsar_producer


def get_dev_raw_data_source(pipeline_builder, raw_data, data_format='JSON', stop_after_first_batch=False):
    """ Adds a 'Dev Raw Data Source' stage to pipeline_builder and sets raw_data, data_format and
    stop_after_first_batch properties of that stage. Returns the added stage"""

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = data_format
    dev_raw_data_source.raw_data = raw_data
    dev_raw_data_source.stop_after_first_batch = stop_after_first_batch

    return dev_raw_data_source



@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('subscription_type', ['SHARED', 'EXCLUSIVE'])
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_consumer(sdc_builder, sdc_executor, pulsar, subscription_type, number_of_threads):
    """Test for Pulsar consumer origin stage. We do so by publishing data to a test topic using Pulsar client and
    having a pipeline which reads that data using Pulsar consumer origin stage. Data is then asserted for what is
    published at Pulsar client and what we read from wiretap. The pipeline looks like:

    Pulsar Consumer pipeline:
        pulsar_consumer >> wiretap
    """
    sub_name = get_random_string(string.ascii_letters, 10)
    consumer_name = get_random_string(string.ascii_letters, 10)
    topic_name = get_random_string(string.ascii_letters, 10)
    max_records = 100
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(subscription_name=sub_name,
                                   consumer_name=consumer_name,
                                   topic=topic_name,
                                   data_format='TEXT',
                                   subscription_type=subscription_type,
                                   max_batch_size_in_records=max_records,
                                   number_of_threads=number_of_threads)
    wiretap = builder.add_wiretap()

    pulsar_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='Pulsar Consumer pipeline').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    producer = pulsar.client.create_producer(topic_name)
    try:
        sdc_executor.start_pipeline(consumer_origin_pipeline)

        records_sent = []
        for i in range(max_records):
            record_to_send = f"{i}-{input_text}"
            records_sent.append(record_to_send)
            producer.send(record_to_send.encode())

        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', max_records)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        output_records = [record.field['text'].value for record in wiretap.output_records]
        assert sorted(output_records) == sorted(records_sent)
    finally:
        producer.close()  # all producer/consumers need to be closed before topic can be deleted without force
        pulsar.client.close()
        pulsar.admin.delete_topic(producer.topic())


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_consumer_with_parameters(sdc_builder, sdc_executor, pulsar, number_of_threads):
    """Make sure that pulsar's topic name is properly resolved with pipeline parameters."""
    sub_name = get_random_string(string.ascii_letters, 10)
    consumer_name = get_random_string(string.ascii_letters, 10)
    topic_name = get_random_string(string.ascii_letters, 10)
    max_records = 100
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(subscription_name=sub_name,
                                   consumer_name=consumer_name,
                                   topic="${TOPIC_NAME}",
                                   data_format='TEXT',
                                   max_batch_size_in_records=max_records,
                                   number_of_threads=number_of_threads)
    wiretap = builder.add_wiretap()

    pulsar_consumer >> wiretap.destination

    pipeline = builder.build(title='Pulsar Consumer pipeline').configure_for_environment(pulsar)
    pipeline.add_parameters(TOPIC_NAME=topic_name)
    sdc_executor.add_pipeline(pipeline)

    client = pulsar.client
    admin = pulsar.admin
    try:
        sdc_executor.start_pipeline(pipeline)

        producer = client.create_producer(topic_name)
        for _ in range(max_records):
            producer.send(input_text.encode())

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', max_records)
        sdc_executor.stop_pipeline(pipeline)

        output_records = [record.field['text'] for record in wiretap.output_records]
        assert output_records == [input_text] * max_records
    finally:
        producer.close()  # all producer/consumers need to be closed before topic can be deleted without force
        client.close()
        admin.delete_topic(producer.topic())


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_origin_standalone_text(sdc_builder, sdc_executor, pulsar, number_of_threads):
    """
    Write simple text messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_source >> pulsar_producer

    Pulsar Consumer Origin pipeline with standalone mode:
        pulsar_consumer >> wiretap
    """
    message = 'pulsar message martin xavi'
    data = {'text': message}
    json_data = json.dumps(data)

    topic = get_random_string(string.ascii_letters, 10)

    # Build the Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder,
                                                  json_data,
                                                  stop_after_first_batch=True)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, topic)
    pulsar_producer.set_attributes(compression_type='NONE')

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone Text').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = pulsar_consumer_pipeline_builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(data_format='TEXT',
                                   batch_wait_time_in_ms=20000,
                                   topic=topic,
                                   consumer_name='consumer',
                                   initial_offset='LATEST',
                                   number_of_threads=number_of_threads)
    pulsar_consumer.set_attributes(subscription_type='FAILOVER', consumer_queue_size=1000)

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    try:
        sdc_executor.start_pipeline(pulsar_consumer_pipeline)
        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

        assert len(wiretap.output_records) == 1
        assert [record.field['text'] for record in wiretap.output_records][0] == message
    finally:
        pulsar.admin.delete_topic(f"persistent://public/default/{topic}")


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_origin_standalone_json(sdc_builder, sdc_executor, pulsar, number_of_threads):
    """
    Write simple json messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_source >> pulsar_producer

    Pulsar Consumer Origin pipeline with standalone mode:
        pulsar_consumer >> wiretap

    Producer includes topic name in message field
    Producer partition type = Round Robin
    """

    topic = get_random_string(string.ascii_letters, 10)
    message = {'Topic': topic, 'Name': 'Xavi', 'Job': 'Developer'}
    json_data = json.dumps(message)

    # Build the Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder,
                                                  json_data,
                                                  stop_after_first_batch=True)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, '${record:value(\'/Topic\')}')
    pulsar_producer.set_attributes(data_format='JSON',
                                   partition_type='ROUND_ROBIN',
                                   compression_type='LZ4')

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone JSON').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = pulsar_consumer_pipeline_builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(data_format='TEXT',
                                   batch_wait_time_in_ms=20000,
                                   topic=topic,
                                   consumer_name='consumer',
                                   initial_offset='EARLIEST',
                                   number_of_threads=number_of_threads)
    pulsar_consumer.set_attributes(subscription_type='SHARED',
                                   consumer_queue_size=10000,
                                   data_format='JSON')

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    try:
        sdc_executor.start_pipeline(pulsar_consumer_pipeline)
        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

        assert len(wiretap.output_records) == 1
        assert [record.field for record in wiretap.output_records][0] == message
    finally:
        pulsar.admin.delete_topic(f"persistent://public/default/{topic}")


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_origin_standalone_xml(sdc_builder, sdc_executor, pulsar, number_of_threads):
    """
    Write simple XML messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_source text message >> pulsar_producer XML

    Pulsar Consumer Origin pipeline with standalone mode:
        pulsar_consumer XML >> wiretap
    """
    message = "Text message that will be converted in XML"

    topic = get_random_string(string.ascii_letters, 10)

    # Build the Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder,
                                                  message,
                                                  data_format='TEXT',
                                                  stop_after_first_batch=True)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, topic)
    pulsar_producer.set_attributes(data_format='XML',
                                   partition_type='SINGLE',
                                   hashing_scheme='JAVA_STRING_HASH',
                                   message_key='12345',
                                   compression_type='ZLIB')

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone XML').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = pulsar_consumer_pipeline_builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(data_format='TEXT',
                                   batch_wait_time_in_ms=20000,
                                   topic=topic,
                                   consumer_name='consumer',
                                   initial_offset='EARLIEST',
                                   number_of_threads=number_of_threads)
    pulsar_consumer.set_attributes(data_format='XML',
                                   consumer_queue_size=1)

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    try:
        sdc_executor.start_pipeline(pulsar_consumer_pipeline)
        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

        assert len(wiretap.output_records) == 1
        assert [record.field['text']['value'] for record in wiretap.output_records][0] == message
    finally:
        pulsar.admin.delete_topic(f"persistent://public/default/{topic}")


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_origin_standalone_topics_list(sdc_builder, sdc_executor, pulsar, number_of_threads):
    """
    Write simple json messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_source >> pulsar_producer_1

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_source >> pulsar_producer_2


    Pulsar Consumer Origin pipeline with standalone mode:
        pulsar_consumer_1_2 >> wiretap
    """

    message = {'Name': 'Xavi', 'Job': 'Developer'}
    json_data = json.dumps(message)

    topic1 = get_random_string(string.ascii_letters, 10)
    topic2 = get_random_string(string.ascii_letters, 10)

    # Build the First Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder1 = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder1.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder1,
                                                  json_data,
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

    pulsar_consumer = pulsar_consumer_pipeline_builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    topics_list = [topic1, topic2]

    pulsar_consumer.set_attributes(batch_wait_time_in_ms=20000,
                                   consumer_name='consumer',
                                   initial_offset='EARLIEST',
                                   topics_selector='TOPICS_LIST',
                                   topics_list=topics_list,
                                   subscription_type='EXCLUSIVE',
                                   read_compacted=False,
                                   data_format='JSON',
                                   number_of_threads=number_of_threads)

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    try:
        sdc_executor.start_pipeline(pulsar_consumer_pipeline)
        sdc_executor.start_pipeline(pulsar_producer_pipeline1).wait_for_finished()
        sdc_executor.start_pipeline(pulsar_producer_pipeline2).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

        assert len(wiretap.output_records) == 2
        records = [record.field for record in wiretap.output_records]
        for record in records:
            assert record == message
    finally:
        pulsar.admin.delete_topic(f"persistent://public/default/{topic1}")
        pulsar.admin.delete_topic(f"persistent://public/default/{topic2}")


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_origin_standalone_topics_pattern(sdc_builder, sdc_executor, pulsar, number_of_threads):
    """
    Write simple json messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_source >> pulsar_producer_1

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_source >> pulsar_producer_2

    Pulsar Consumer Origin pipeline with standalone mode:
        pulsar_consumer_1_2 >> wiretap

    The name of topics 1 and topics 2 begins with SDC
    A pulsar_consumer_3 is included. Its name does not begin with SDC
    """

    message = 'Xavi, Developer, Very Long Message In Order To Spend More Time Sending It'

    topic1 = 'SDC' + get_random_string(string.ascii_letters, 10)
    topic2 = 'XXX' + get_random_string(string.ascii_letters, 10)
    topic3 = 'SDC' + get_random_string(string.ascii_letters, 10)

    # Build the First Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder1 = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder1.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder1,
                                                  message,
                                                  data_format='DELIMITED',
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

    dev_raw_data_source2 = get_dev_raw_data_source(pulsar_producer_pipeline_builder2,
                                                   message,
                                                   data_format='DELIMITED',
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

    dev_raw_data_source3 = get_dev_raw_data_source(pulsar_producer_pipeline_builder3,
                                                   message,
                                                   data_format='DELIMITED',
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

    pulsar_consumer = pulsar_consumer_pipeline_builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(data_format='TEXT',
                                   batch_wait_time_in_ms=20000,
                                   topic=topic1,
                                   consumer_name='consumer',
                                   initial_offset='EARLIEST',
                                   number_of_threads=number_of_threads)
    pulsar_consumer.set_attributes(topics_selector='TOPICS_PATTERN',
                                   topics_pattern=f'persistent://public/default/({topic1}|{topic2}|{topic3})',
                                   subscription_type='EXCLUSIVE',
                                   read_compacted=False,
                                   max_batch_size_in_records=10,
                                   data_format='DELIMITED')

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    try:
        sdc_executor.start_pipeline(pulsar_consumer_pipeline).wait_for_status('RUNNING')

        sdc_executor.start_pipeline(pulsar_producer_pipeline1).wait_for_finished()
        sdc_executor.start_pipeline(pulsar_producer_pipeline2).wait_for_finished()
        sdc_executor.start_pipeline(pulsar_producer_pipeline3).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1, timeout_sec=100)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

        assert len(wiretap.output_records) == 3

        records = [record.field for record in wiretap.output_records]
        for record in records:
            assert ','.join(str(element) for element in record.values()) == message
    finally:
        pulsar.admin.delete_topic(f"persistent://public/default/{topic1}")
        pulsar.admin.delete_topic(f"persistent://public/default/{topic2}")
        pulsar.admin.delete_topic(f"persistent://public/default/{topic3}")


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_origin_standalone_json_tls_encrypt(sdc_builder, sdc_executor, pulsar, number_of_threads):
    """
    Write simple json messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_source >> pulsar_producer

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

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder,
                                                  json_data,
                                                  stop_after_first_batch=True)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, '${record:value(\'/Topic\')}')
    pulsar_producer.set_attributes(data_format='JSON',
                                   partition_type='ROUND_ROBIN',
                                   compression_type='LZ4',
                                   enable_tls=True)

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone JSON').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = pulsar_consumer_pipeline_builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(data_format='TEXT',
                                   batch_wait_time_in_ms=20000,
                                   topic=topic,
                                   consumer_name='consumer',
                                   initial_offset='EARLIEST',
                                   number_of_threads=number_of_threads)
    pulsar_consumer.set_attributes(subscription_type='SHARED',
                                   consumer_queue_size=10000,
                                   data_format='JSON',
                                   enable_tls=True)

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    try:
        sdc_executor.start_pipeline(pulsar_consumer_pipeline)
        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

        assert len(wiretap.output_records) == 1
        assert [record.field for record in wiretap.output_records][0] == message
    finally:
        pulsar.admin.delete_topic(f"persistent://public/default/{topic}")


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_origin_standalone_json_tls_mutual_auth(sdc_builder, sdc_executor, pulsar, number_of_threads):
    """
    Write simple json messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_source >> pulsar_producer

    Pulsar Consumer Origin pipeline with standalone mode:
        pulsar_consumer >> wiretap

    Producer includes topic name in message field
    Producer partition type = Round Robin
    """

    topic = get_random_string(string.ascii_letters, 10)
    message = {'Topic': topic, 'Name': 'Xavi', 'Job': 'Developer'}
    json_data = json.dumps(message)

    # Build the Pulsar producer pipeline with Standalone mode.
    pulsar_producer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_producer_pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = get_dev_raw_data_source(pulsar_producer_pipeline_builder,
                                                  json_data,
                                                  stop_after_first_batch=True)

    pulsar_producer = get_pulsar_producer_stage(pulsar_producer_pipeline_builder, '${record:value(\'/Topic\')}')
    pulsar_producer.set_attributes(data_format='JSON',
                                   partition_type='ROUND_ROBIN',
                                   compression_type='LZ4',
                                   enable_tls=True,
                                   enable_mutual_authentication=True)

    dev_raw_data_source >> pulsar_producer
    pulsar_producer_pipeline = pulsar_producer_pipeline_builder.build(
        title='Pulsar Producer Standalone JSON').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pulsar_producer_pipeline)

    # Build the Pulsar consumer pipeline with Standalone mode.
    pulsar_consumer_pipeline_builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer_pipeline_builder.add_error_stage('Discard')

    pulsar_consumer = pulsar_consumer_pipeline_builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(data_format='TEXT',
                                   batch_wait_time_in_ms=20000,
                                   topic=topic,
                                   consumer_name='consumer',
                                   initial_offset='EARLIEST',
                                   number_of_threads=number_of_threads)
    pulsar_consumer.set_attributes(subscription_type='SHARED',
                                   consumer_queue_size=10000,
                                   data_format='JSON',
                                   enable_tls=True,
                                   enable_mutual_authentication=True)

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    try:
        sdc_executor.start_pipeline(pulsar_consumer_pipeline)
        sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

        assert len(wiretap.output_records) == 1
        assert [record.field for record in wiretap.output_records][0] == message
    finally:
        pulsar.admin.delete_topic(f"persistent://public/default/{topic}")


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_consumer_topic_header(sdc_builder, sdc_executor, pulsar, number_of_threads):
    """Test for Pulsar consumer origin stage.
    We verify that the output records contains a header with the topic

    Pulsar Consumer pipeline:
        pulsar_consumer >> wiretap
    """
    topic_name = get_random_string()
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(subscription_name=get_random_string(),
                                   consumer_name=get_random_string(),
                                   topic=topic_name,
                                   data_format='TEXT',
                                   number_of_threads=number_of_threads)
    wiretap = builder.add_wiretap()

    pulsar_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(pulsar)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    client = pulsar.client
    admin = pulsar.admin
    try:
        sdc_executor.start_pipeline(consumer_origin_pipeline)

        producer = client.create_producer(topic_name)
        producer.send(input_text.encode())

        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        output_records = [record.field['text'] for record in wiretap.output_records]
        assert 1 == len(output_records)
        assert output_records == [input_text]
        assert topic_name in str(wiretap.output_records[0].header.values)
    finally:
        producer.close()
        client.close()
        admin.delete_topic(producer.topic())


class ComplexSchemaClass(schema.Record):
    number = schema.Integer()
    topic = schema.String()

    def __init__(self, number, topic):
        self.number = number
        self.topic = topic

@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
@pytest.mark.parametrize("data_format, schema_info, message_data", [
    (
            "TEXT",
            {
                "type": "STRING",
                "schema": "",
                "properties": {}
            },
            "Test string"
    ),
    (
            "AVRO",
            {
                "type": "AVRO",
                "schema": '{\"type\":\"record\",\"name\":\"schema\",\"fields\":'
                          '[{\"name\":\"number\",\"type\":[\"null\", \"int\"]},'
                          '{\"name\":\"topic\",\"type\":[\"null\", \"string\"]}]}',
                "properties": {}
            },
            # This must contains values for the fields in the class ComplexSchemaClass. This is because we use this
            # class instances to push messages to Pulsar. This is how the pulsar-client does work.
            {"number": 10, "topic": "some_topic"}
    ),
    (
            "JSON",
            {
                "type": "JSON",
                "schema": '{\"type\":\"record\",\"name\":\"schema\",\"fields\":'
                          '[{\"name\":\"number\",\"type\":[\"null\", \"int\"]},'
                          '{\"name\":\"topic\",\"type\":[\"null\", \"string\"]}]}',
                "properties": {}
            },
            # This must contains values for the fields in the class ComplexSchemaClass. This is because we use this
            # class instances to push messages to Pulsar. This is how the pulsar-client does work.
            {"number": 10, "topic": "some_topic"}
    ),
    (
            "XML",
            {
                "type": "STRING",
                "schema": "",
                "properties": {}
            },
            "<record><name>Fran</name><age>32</age></record>"
    ),
])
def test_pulsar_consumer_schemas(sdc_builder, sdc_executor, pulsar, data_format, schema_info, message_data,
                                 number_of_threads):
    topic_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(subscription_name=get_random_string(),
                                   consumer_name=get_random_string(),
                                   topic=topic_name,
                                   schema="USER_SCHEMA",
                                   schema_info=json.dumps(schema_info),
                                   data_format=data_format,
                                   max_batch_size_in_records=1,
                                   initial_offset='EARLIEST',
                                   number_of_threads=number_of_threads)
    if schema_info["type"] == "AVRO":
        pulsar_consumer.set_attributes(avro_schema_location="INLINE",
                                       avro_schema=schema_info["schema"])
    wiretap = builder.add_wiretap()

    pulsar_consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pipeline)

    # Prepare Pulsar
    if schema_info["type"] == "STRING":
        pulsar_schema = schema.StringSchema()
    elif schema_info["type"] == "AVRO":
        pulsar_schema = schema.AvroSchema(ComplexSchemaClass)
    elif schema_info["type"] == "JSON":
        pulsar_schema = schema.JsonSchema(ComplexSchemaClass)
    else:
        raise RuntimeError(f"No test schema for {schema_info['type']}")

    set_schema_validation_enforced(pulsar.admin, False)
    enable_auto_update_schema(pulsar.admin)

    create_topic_with_schema(pulsar.admin, topic_name, schema_info)
    producer = pulsar.client.create_producer(topic_name, schema=pulsar_schema)

    # To be sure the pipeline does not upload the schema and so respect the schema
    # we set for the topic
    set_schema_validation_enforced(pulsar.admin)
    disable_auto_update_schema(pulsar.admin)

    try:
        if schema_info["type"] == "STRING":
            producer.send(message_data)
        else:  # Valid for complex schemas AVRO and JSON
            producer.send(ComplexSchemaClass(**message_data))

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        output_records = [record for record in wiretap.output_records]
        assert len(output_records) == 1
        assert topic_name in str(wiretap.output_records[0].header.values)

        if data_format == "TEXT":
            assert output_records[0].field["text"] == message_data
        elif data_format == "AVRO":  # Valid for complex schemas AVRO and JSON
            for k, v in message_data.items():
                assert k in output_records[0].field
                assert output_records[0].field[k] == v
        elif data_format == "JSON":
            assert output_records[0].field == message_data
        elif data_format == "XML":
            t = ET.ElementTree(ET.fromstring(message_data)).getroot()
            # Won't work for xml with arbitrary nested levels even though this is enough for this test purposes
            xml_message_data = {t.tag: {c.tag: [{"value": c.text}] for c in t.getchildren()}}
            assert output_records[0].field == xml_message_data
        else:
            raise RuntimeError(f"Final assertions are not written for the schema type {schema_info['type']}")
    finally:
        producer.close()
        pulsar.admin.delete_topic(producer.topic())
        sdc_executor.remove_pipeline(pipeline)


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize("subscription_type", ['EXCLUSIVE', 'SHARED'])
@pytest.mark.parametrize('number_of_threads', [1, 4])
@pytest.mark.parametrize("data_format, schema_info, message_data", [
    (
            "TEXT",
            {
                "type": "STRING",
                "schema": "",
                "properties": {}
            },
            "Test string"
    ),
    (
            "AVRO",
            {
                "type": "AVRO",
                "schema": '{\"type\":\"record\",\"name\":\"schema\",\"fields\":'
                          '[{\"name\":\"number\",\"type\":[\"null\", \"int\"]},'
                          '{\"name\":\"topic\",\"type\":[\"null\", \"string\"]}]}',
                "properties": {}
            },
            # This must contains values for the fields in the class ComplexSchemaClass. This is because we use this
            # class instances to push messages to Pulsar. This is how the pulsar-client does work.
            {"number": 10, "topic": "some_topic"}
    ),
    (
            "JSON",
            {
                "type": "JSON",
                "schema": '{\"type\":\"record\",\"name\":\"schema\",\"fields\":'
                          '[{\"name\":\"number\",\"type\":[\"null\", \"int\"]},'
                          '{\"name\":\"topic\",\"type\":[\"null\", \"string\"]}]}',
                "properties": {}
            },
            # This must contains values for the fields in the class ComplexSchemaClass. This is because we use this
            # class instances to push messages to Pulsar. This is how the pulsar-client does work.
            {"number": 10, "topic": "some_topic"}
    ),
    (
            "XML",
            {
                "type": "STRING",
                "schema": "",
                "properties": {}
            },
            "<record><name>Fran</name><age>32</age></record>"
    ),
])
def test_pulsar_consumer_schemas_topics_list(sdc_builder, sdc_executor, pulsar, data_format, schema_info, message_data,
                                 number_of_threads, subscription_type):
    topics_list = sorted([get_random_string() for _ in range(2)])

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(subscription_name=get_random_string(),
                                   consumer_name=get_random_string(),
                                   topics_selector='TOPICS_LIST',
                                   topics_list=topics_list,
                                   schema="USER_SCHEMA",
                                   schema_info=json.dumps(schema_info),
                                   data_format=data_format,
                                   max_batch_size_in_records=1,
                                   initial_offset='EARLIEST',
                                   number_of_threads=number_of_threads,
                                   subscription_type=subscription_type)
    if schema_info["type"] == "AVRO":
        pulsar_consumer.set_attributes(avro_schema_location="INLINE",
                                       avro_schema=schema_info["schema"])
    wiretap = builder.add_wiretap()

    pulsar_consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pipeline)

    # Prepare Pulsar
    if schema_info["type"] == "STRING":
        pulsar_schema = schema.StringSchema()
    elif schema_info["type"] == "AVRO":
        pulsar_schema = schema.AvroSchema(ComplexSchemaClass)
    elif schema_info["type"] == "JSON":
        pulsar_schema = schema.JsonSchema(ComplexSchemaClass)
    else:
        raise RuntimeError(f"No test schema for {schema_info['type']}")

    set_schema_validation_enforced(pulsar.admin, False)
    enable_auto_update_schema(pulsar.admin)

    producers = []
    for topic_name in topics_list:
        create_topic_with_schema(pulsar.admin, topic_name, schema_info)
        producers.append(pulsar.client.create_producer(topic_name, schema=pulsar_schema))

    # To be sure the pipeline does not upload the schema and so respect the schema
    # we set for the topic
    set_schema_validation_enforced(pulsar.admin)
    disable_auto_update_schema(pulsar.admin)

    try:
        for producer in producers:
            if schema_info["type"] == "STRING":
                producer.send(message_data)
            else:  # Valid for complex schemas AVRO and JSON
                producer.send(ComplexSchemaClass(**message_data))

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        output_records = sorted([record for record in wiretap.output_records],
                                key=lambda x: x.header.values['pulsar.topic'])

        assert len(output_records) == len(topics_list)
        for topic_name, output_record in zip(topics_list, output_records):
            assert topic_name in str(output_record.header.values)
            if data_format == "TEXT":
                assert output_record.field["text"] == message_data
            elif data_format == "AVRO":  # Valid for complex schemas AVRO and JSON
                for k, v in message_data.items():
                    assert k in output_record.field
                    assert output_record.field[k] == v
            elif data_format == "JSON":
                assert output_record.field == message_data
            elif data_format == "XML":
                t = ET.ElementTree(ET.fromstring(message_data)).getroot()
                # Won't work for xml with arbitrary nested levels even though this is enough for this test purposes
                xml_message_data = {t.tag: {c.tag: [{"value": c.text}] for c in t.getchildren()}}
                assert output_record.field == xml_message_data
            else:
                raise RuntimeError(f"Final assertions are not written for the schema type {schema_info['type']}")
    finally:
        for producer in producers:
            producer.close()
            pulsar.admin.delete_topic(producer.topic())
        sdc_executor.remove_pipeline(pipeline)


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize("subscription_type", ['EXCLUSIVE', 'SHARED'])
@pytest.mark.parametrize('number_of_threads', [1, 4])
@pytest.mark.parametrize("data_format, schema_info, message_data", [
    (
            "TEXT",
            {
                "type": "STRING",
                "schema": "",
                "properties": {}
            },
            "Test string"
    ),
    (
            "AVRO",
            {
                "type": "AVRO",
                "schema": '{\"type\":\"record\",\"name\":\"schema\",\"fields\":'
                          '[{\"name\":\"number\",\"type\":[\"null\", \"int\"]},'
                          '{\"name\":\"topic\",\"type\":[\"null\", \"string\"]}]}',
                "properties": {}
            },
            # This must contains values for the fields in the class ComplexSchemaClass. This is because we use this
            # class instances to push messages to Pulsar. This is how the pulsar-client does work.
            {"number": 10, "topic": "some_topic"}
    ),
    (
            "JSON",
            {
                "type": "JSON",
                "schema": '{\"type\":\"record\",\"name\":\"schema\",\"fields\":'
                          '[{\"name\":\"number\",\"type\":[\"null\", \"int\"]},'
                          '{\"name\":\"topic\",\"type\":[\"null\", \"string\"]}]}',
                "properties": {}
            },
            # This must contains values for the fields in the class ComplexSchemaClass. This is because we use this
            # class instances to push messages to Pulsar. This is how the pulsar-client does work.
            {"number": 10, "topic": "some_topic"}
    ),
    (
            "XML",
            {
                "type": "STRING",
                "schema": "",
                "properties": {}
            },
            "<record><name>Fran</name><age>32</age></record>"
    ),
])
def test_pulsar_consumer_schemas_topics_pattern(sdc_builder, sdc_executor, pulsar, data_format, schema_info, message_data,
                                             number_of_threads, subscription_type):
    topics_list = sorted([f'topic-prefix{get_random_string()}' for _ in range(2)])

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(subscription_name=get_random_string(),
                                   consumer_name=get_random_string(),
                                   topics_selector='TOPICS_PATTERN',
                                   topics_pattern=f'persistent://public/default/({"|".join(topics_list)})',
                                   schema="USER_SCHEMA",
                                   schema_info=json.dumps(schema_info),
                                   data_format=data_format,
                                   max_batch_size_in_records=1,
                                   initial_offset='EARLIEST',
                                   number_of_threads=number_of_threads,
                                   subscription_type=subscription_type)
    if schema_info["type"] == "AVRO":
        pulsar_consumer.set_attributes(avro_schema_location="INLINE",
                                       avro_schema=schema_info["schema"])
    wiretap = builder.add_wiretap()

    pulsar_consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pipeline)

    # Prepare Pulsar
    if schema_info["type"] == "STRING":
        pulsar_schema = schema.StringSchema()
    elif schema_info["type"] == "AVRO":
        pulsar_schema = schema.AvroSchema(ComplexSchemaClass)
    elif schema_info["type"] == "JSON":
        pulsar_schema = schema.JsonSchema(ComplexSchemaClass)
    else:
        raise RuntimeError(f"No test schema for {schema_info['type']}")

    set_schema_validation_enforced(pulsar.admin, False)
    enable_auto_update_schema(pulsar.admin)

    producers = []
    for topic_name in topics_list:
        create_topic_with_schema(pulsar.admin, topic_name, schema_info)
        producers.append(pulsar.client.create_producer(topic_name, schema=pulsar_schema))

    # To be sure the pipeline does not upload the schema and so respect the schema
    # we set for the topic
    set_schema_validation_enforced(pulsar.admin)
    disable_auto_update_schema(pulsar.admin)

    try:
        for producer in producers:
            if schema_info["type"] == "STRING":
                producer.send(message_data)
            else:  # Valid for complex schemas AVRO and JSON
                producer.send(ComplexSchemaClass(**message_data))

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        output_records = sorted([record for record in wiretap.output_records],
                                key=lambda x: x.header.values['pulsar.topic'])

        assert len(output_records) == len(topics_list)
        for topic_name, output_record in zip(topics_list, output_records):
            assert topic_name in str(output_record.header.values)
            if data_format == "TEXT":
                assert output_record.field["text"] == message_data
            elif data_format == "AVRO":  # Valid for complex schemas AVRO and JSON
                for k, v in message_data.items():
                    assert k in output_record.field
                    assert output_record.field[k] == v
            elif data_format == "JSON":
                assert output_record.field == message_data
            elif data_format == "XML":
                t = ET.ElementTree(ET.fromstring(message_data)).getroot()
                # Won't work for xml with arbitrary nested levels even though this is enough for this test purposes
                xml_message_data = {t.tag: {c.tag: [{"value": c.text}] for c in t.getchildren()}}
                assert output_record.field == xml_message_data
            else:
                raise RuntimeError(f"Final assertions are not written for the schema type {schema_info['type']}")
    finally:
        for producer in producers:
            producer.close()
            pulsar.admin.delete_topic(producer.topic())
        sdc_executor.remove_pipeline(pipeline)


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize("subscription_type", ['EXCLUSIVE', 'SHARED'])
@pytest.mark.parametrize("number_of_threads", [1, 4])
def test_pulsar_consumer_max_batch_time(sdc_builder, sdc_executor, pulsar, subscription_type, number_of_threads):
    sub_name = get_random_string(string.ascii_letters, 10)
    consumer_name = get_random_string(string.ascii_letters, 10)
    topic_name = get_random_string(string.ascii_letters, 10)
    number_messages = 10
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(subscription_name=sub_name,
                                   consumer_name=consumer_name,
                                   topic=topic_name,
                                   data_format='TEXT',
                                   initial_offset='EARLIEST',
                                   batch_wait_time_in_ms=2000,
                                   max_batch_size_in_records=1_000_000,
                                   subscription_type=subscription_type,
                                   number_of_threads=number_of_threads)
    wiretap = builder.add_wiretap()
    pulsar_consumer >> wiretap.destination

    pipeline = builder.build(title='Pulsar Consumer pipeline Max batch time').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pipeline)

    producer = pulsar.client.create_producer(topic_name)
    try:
        sdc_executor.start_pipeline(pipeline)

        for _ in range(number_messages):
            producer.send(input_text.encode())

        # It will never reach 1_000_000 records so that if it reaches 10 it would mean the max batch timeout has been
        # reached and the timeout mechanisms worked as expected.
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', number_messages, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        output_records = [record.field['text'] for record in wiretap.output_records]
        assert len(output_records) == number_messages
        assert output_records == [input_text] * number_messages
    finally:
        producer.close()
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)
        pulsar.admin.delete_topic(producer.topic())


@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.skip('Suspect this is breaking Pulsar so skipping temporarily')
@pytest.mark.parametrize("subscription_type", ['EXCLUSIVE', 'SHARED'])
@pytest.mark.parametrize("number_of_threads", [1, 8])
def test_pulsar_consumer_multiple_threads_assert_parallel(sdc_builder, sdc_executor, pulsar, subscription_type,
                                                          number_of_threads):
    """ For every record, a jython script will sleep the pipeline for 10 seconds. If two records are
    handled in parallel the wait time will be 10 seconds as well """
    sub_name = get_random_string(string.ascii_letters, 10)
    consumer_name = get_random_string(string.ascii_letters, 10)
    topic_name = get_random_string(string.ascii_letters, 10)
    max_records = number_of_threads
    input_text = 'Hello World!'
    busy_time = 10

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(subscription_name=sub_name,
                                   consumer_name=consumer_name,
                                   topic=topic_name,
                                   data_format='TEXT',
                                   number_of_threads=number_of_threads,
                                   subscription_type=subscription_type,
                                   max_batch_size_in_records=1)
    jython_evaluator = builder.add_stage('Jython Evaluator')
    jython_evaluator.set_attributes(script=f"""
from time import sleep 
for record in records: output.write(record);sleep({busy_time});
""")
    wiretap = builder.add_wiretap()

    pulsar_consumer >> jython_evaluator >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='Pulsar Consumer pipeline').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    producer = pulsar.client.create_producer(topic_name)
    try:
        sdc_executor.start_pipeline(consumer_origin_pipeline)

        for _ in range(max_records):
            producer.send(input_text.encode())

        import time
        start_time = time.time()
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', max_records)
        assert busy_time < time.time() - start_time < busy_time * 1.5
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        output_records = [record.field['text'] for record in wiretap.output_records]
        assert output_records == [input_text] * max_records
    finally:
        producer.close()  # all producer/consumers need to be closed before topic can be deleted without force
        pulsar.client.close()
        pulsar.admin.delete_topic(producer.topic())

@pulsar
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('number_of_threads', [1, 2, 4])
def test_pulsar_consumer_schemas_wrong_dataformat(sdc_builder, sdc_executor, pulsar, number_of_threads):
    topic_name = get_random_string()
    data_format = "JSON"  # Wrong data format
    schema_info = {
        "type": "AVRO",
        "schema": '{\"type\":\"record\",\"name\":\"schema\",\"fields\":'
                  '[{\"name\":\"number\",\"type\":[\"null\", \"int\"]},'
                  '{\"name\":\"topic\",\"type\":[\"null\", \"string\"]}]}',
        "properties": {}
    }
    message_data = {"number": 10, "topic": "some_topic"}
    n_messages = 10

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage(name=PULSAR_PUSH_ORIGIN_STAGE_NAME)
    pulsar_consumer.set_attributes(subscription_name=get_random_string(),
                                   consumer_name=get_random_string(),
                                   topic=topic_name,
                                   schema="USER_SCHEMA",
                                   schema_info=json.dumps(schema_info),
                                   data_format=data_format,
                                   max_batch_size_in_records=1,
                                   initial_offset='EARLIEST',
                                   number_of_threads=number_of_threads)
    if schema_info["type"] == "AVRO":
        pulsar_consumer.set_attributes(avro_schema_location="INLINE",
                                       avro_schema=schema_info["schema"])
    wiretap = builder.add_wiretap()

    pulsar_consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pipeline)

    # Prepare Pulsar
    pulsar_schema = schema.AvroSchema(ComplexSchemaClass)

    set_schema_validation_enforced(pulsar.admin, False)
    enable_auto_update_schema(pulsar.admin)

    create_topic_with_schema(pulsar.admin, topic_name, schema_info)
    producer = pulsar.client.create_producer(topic_name, schema=pulsar_schema)

    # To be sure the pipeline does not upload the schema and so respect the schema
    # we set for the topic
    set_schema_validation_enforced(pulsar.admin)
    disable_auto_update_schema(pulsar.admin)

    try:
        for _ in range(n_messages):
            producer.send(ComplexSchemaClass(**message_data))

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', n_messages)
        sdc_executor.stop_pipeline(pipeline)

        assert 0 == len(wiretap.output_records)
        assert n_messages == len(wiretap.error_records)
    finally:
        producer.close()
        pulsar.admin.delete_topic(producer.topic())
        sdc_executor.remove_pipeline(pipeline)
