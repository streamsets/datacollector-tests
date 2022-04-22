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

from .utils.utils_pulsar import enforce_schema_validation_for_pulsar_topic, disable_auto_update_schema, \
    create_topic_with_schema, set_schema_validation_enforced, enable_auto_update_schema, json_to_avro

logger = logging.getLogger(__name__)


def get_pulsar_consumer_stage(pipeline_builder, topic, initial_offset):
    """Create and return a Pulsar Consumer origin stage depending on execution mode for the pipeline."""
    pulsar_consumer = pipeline_builder.add_stage('Pulsar Consumer', type='origin')
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
    pulsar_consumer = builder.add_stage('Pulsar Consumer').set_attributes(subscription_name=sub_name,
                                                                          consumer_name=consumer_name,
                                                                          topic=topic_name,
                                                                          data_format='TEXT',
                                                                          max_batch_size_in_records=max_records)
    wiretap = builder.add_wiretap()

    pulsar_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='Pulsar Consumer pipeline').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    client = pulsar.client
    admin = pulsar.admin
    try:
        sdc_executor.start_pipeline(consumer_origin_pipeline)

        producer = client.create_producer(topic_name)
        for _ in range(max_records):
            producer.send(input_text.encode())

        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', max_records)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        output_records = [record.field['text'] for record in wiretap.output_records]
        assert output_records == [input_text] * max_records
    finally:
        producer.close()  # all producer/consumers need to be closed before topic can be deleted without force
        client.close()
        admin.delete_topic(producer.topic())


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_consumer_with_parameters(sdc_builder, sdc_executor, pulsar):
    """Make sure that pulsar's topic name is properly resolved with pipeline parameters."""
    sub_name = get_random_string(string.ascii_letters, 10)
    consumer_name = get_random_string(string.ascii_letters, 10)
    topic_name = get_random_string(string.ascii_letters, 10)
    max_records = 100
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage('Pulsar Consumer').set_attributes(subscription_name=sub_name,
                                                                          consumer_name=consumer_name,
                                                                          topic="${TOPIC_NAME}",
                                                                          data_format='TEXT',
                                                                          max_batch_size_in_records=max_records)
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
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_text(sdc_builder, sdc_executor, pulsar):
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

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic, 'LATEST')
    pulsar_consumer.set_attributes(subscription_type='FAILOVER', consumer_queue_size=1000)

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    sdc_executor.start_pipeline(pulsar_consumer_pipeline)
    sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

    sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

    assert len(wiretap.output_records) == 1
    assert [record.field['text'] for record in wiretap.output_records][0] == message


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_json(sdc_builder, sdc_executor, pulsar):
    """
    Write simple json messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_soruce >> pulsar_producer

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

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic,
                                                'EARLIEST')
    pulsar_consumer.set_attributes(subscription_type='SHARED',
                                   consumer_queue_size=10000,
                                   data_format='JSON')

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    sdc_executor.start_pipeline(pulsar_consumer_pipeline)
    sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

    sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

    assert len(wiretap.output_records) == 1
    assert [record.field for record in wiretap.output_records][0] == message


@pulsar
@sdc_min_version('3.14.0')
def test_pulsar_origin_standalone_xml(sdc_builder, sdc_executor, pulsar):
    """
    Write simple XML messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_soruce text message >> pulsar_producer XML

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

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic, 'EARLIEST')
    pulsar_consumer.set_attributes(data_format='XML',
                                   consumer_queue_size=1)

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    sdc_executor.start_pipeline(pulsar_consumer_pipeline)
    sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

    sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

    assert len(wiretap.output_records) == 1
    assert [record.field['text']['value'] for record in wiretap.output_records][0] == message


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_topics_list(sdc_builder, sdc_executor, pulsar):
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

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic1, 'EARLIEST')
    topics_list = [topic1, topic2]

    pulsar_consumer.set_attributes(topics_selector='TOPICS_LIST',
                                   topics_list=topics_list,
                                   subscription_type='EXCLUSIVE',
                                   read_compacted=False,
                                   data_format='JSON')

    wiretap = pulsar_consumer_pipeline_builder.add_wiretap()
    pulsar_consumer >> wiretap.destination
    pulsar_consumer_pipeline = pulsar_consumer_pipeline_builder.build(
        title='Pulsar Consumer Standalone pipeline').configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pulsar_consumer_pipeline)

    # Publish messages to Pulsar and verify using wiretap if the same messages are received.
    sdc_executor.start_pipeline(pulsar_consumer_pipeline)
    sdc_executor.start_pipeline(pulsar_producer_pipeline1).wait_for_finished()
    sdc_executor.start_pipeline(pulsar_producer_pipeline2).wait_for_finished()

    sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

    assert len(wiretap.output_records) == 2
    records = [record.field for record in wiretap.output_records]
    for record in records:
        assert record == message


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_topics_pattern(sdc_builder, sdc_executor, pulsar):
    """
    Write simple json messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_soruce >> pulsar_producer_1

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_soruce >> pulsar_producer_2

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

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic1, 'EARLIEST')
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


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_json_tls_encrypt(sdc_builder, sdc_executor, pulsar):
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

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic,
                                                'EARLIEST')
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
    sdc_executor.start_pipeline(pulsar_consumer_pipeline)
    sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

    sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

    assert len(wiretap.output_records) == 1
    assert [record.field for record in wiretap.output_records][0] == message


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_origin_standalone_json_tls_mutual_auth(sdc_builder, sdc_executor, pulsar):
    """
    Write simple json messages into pulsar and confirm that pulsar successfully reads them.
    Specifically, this would look like:

    Pulsar Producer Destination pipeline with standalone mode:
        dev_raw_data_soruce >> pulsar_producer

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

    pulsar_consumer = get_pulsar_consumer_stage(pulsar_consumer_pipeline_builder, topic,
                                                'EARLIEST')
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
    sdc_executor.start_pipeline(pulsar_consumer_pipeline)
    sdc_executor.start_pipeline(pulsar_producer_pipeline).wait_for_finished()

    sdc_executor.wait_for_pipeline_metric(pulsar_consumer_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pulsar_consumer_pipeline)

    assert len(wiretap.output_records) == 1
    assert [record.field for record in wiretap.output_records][0] == message


@pulsar
@sdc_min_version('4.4.0')
def test_pulsar_consumer_topic_header(sdc_builder, sdc_executor, pulsar):
    """Test for Pulsar consumer origin stage.
    We verify that the output records contains a header with the topic

    Pulsar Consumer pipeline:
        pulsar_consumer >> wiretap
    """
    topic_name = get_random_string()
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage('Pulsar Consumer').set_attributes(subscription_name=get_random_string(),
                                                                          consumer_name=get_random_string(),
                                                                          topic=topic_name,
                                                                          data_format='TEXT')
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
def test_pulsar_consumer_schemas(sdc_builder, sdc_executor, pulsar, data_format, schema_info, message_data):
    topic_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    pulsar_consumer = builder.add_stage('Pulsar Consumer')
    pulsar_consumer.set_attributes(subscription_name=get_random_string(),
                                   consumer_name=get_random_string(),
                                   topic=topic_name,
                                   schema="USER_SCHEMA",
                                   schema_info=json.dumps(schema_info),
                                   data_format=data_format,
                                   max_batch_size_in_records=1,
                                   initial_offset='EARLIEST')
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
            raise Exception(f"Final assertions are not written for the schema type {schema_info['type']}")
    finally:
        producer.close()
        pulsar.admin.delete_topic(producer.topic())
        sdc_executor.remove_pipeline(pipeline)
