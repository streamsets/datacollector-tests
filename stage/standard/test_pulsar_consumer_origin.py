# Copyright 2020 StreamSets Inc.
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
from streamsets.testframework.markers import pulsar
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Topics are URLs so we have to respect URL specs
TOPIC_NAMES = [
    ('lowercase', get_random_string(string.ascii_lowercase)),
    ('uppercase', get_random_string(string.ascii_uppercase)),
    ('letters', get_random_string(string.ascii_letters)),
    ('digits', get_random_string(string.digits)),
    ('hexadecimal', get_random_string(string.hexdigits).lower()),
    ('hypen', get_random_string() + '-' + get_random_string()),
    ('start_hypen', '-' + get_random_string()),
    ('end_hypen', get_random_string() + '-'),
    ('underscore', get_random_string() + '_' + get_random_string()),
    ('start_underscore', get_random_string() + '_'),
    ('end_underscore', '_' + get_random_string()),
    ('dot', get_random_string() + '.' + get_random_string()),
    ('start_dot', '.' + get_random_string()),
    ('end_dot', get_random_string() + '.')
]

CONSUMER_NAMES = TOPIC_NAMES

@pulsar
def test_data_types(sdc_builder, sdc_executor, cluster):
    pytest.skip("Pulsar isn't a typed data store")


@pulsar
@pytest.mark.parametrize('test_name, topic_name', TOPIC_NAMES, ids=[t[0] for t in TOPIC_NAMES])
def test_object_names_topic(sdc_builder, sdc_executor, pulsar, test_name, topic_name, keep_data):
    message = 'Hello World from SDC & DPM!'
    expected_output = {'text': 'Hello World from SDC & DPM!'}

    builder = sdc_builder.get_pipeline_builder()
    consumer = builder.add_stage('Pulsar Consumer', type='origin')
    consumer.data_format = 'TEXT'
    consumer.topic = topic_name
    consumer.batch_wait_time_in_ms = 20000
    consumer.consumer_name = get_random_string()
    consumer.initial_offset = 'EARLIEST'

    wiretap = builder.add_wiretap()

    consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pipeline)

    client = pulsar.client
    admin = pulsar.admin
    try:
        producer = client.create_producer(topic_name)
        producer.send(message.encode())

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field for record in wiretap.output_records] == [expected_output]
    finally:
        if not keep_data:
            producer.close()
            client.close()
            admin.delete_topic(producer.topic())


@pulsar
@pytest.mark.parametrize('test_name, consumer_group_name', CONSUMER_NAMES, ids=[t[0] for t in CONSUMER_NAMES])
def test_object_names_consumer_group(sdc_builder, sdc_executor, pulsar, test_name, consumer_group_name, keep_data):
    message = 'Hello World from SDC & DPM!'
    expected_output = {'text': 'Hello World from SDC & DPM!'}

    builder = sdc_builder.get_pipeline_builder()
    consumer = builder.add_stage('Pulsar Consumer', type='origin')
    consumer.data_format = 'TEXT'
    consumer.topic = get_random_string()
    consumer.batch_wait_time_in_ms = 20000
    consumer.consumer_name = consumer_group_name
    consumer.initial_offset = 'EARLIEST'

    wiretap = builder.add_wiretap()

    consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pipeline)

    client = pulsar.client
    admin = pulsar.admin
    try:
        producer = client.create_producer(consumer.topic)
        producer.send(message.encode())

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field for record in wiretap.output_records] == [expected_output]
    finally:
        if not keep_data:
            producer.close()
            client.close()
            admin.delete_topic(producer.topic())


@pulsar
def test_dataflow_events(sdc_builder, sdc_executor, pulsar):
    pytest.skip('Pulsar Origin does not produce events')


@pulsar
@pytest.mark.parametrize('initial_offset', ['EARLIEST', 'LATEST'])
def test_resume_offset(sdc_builder, sdc_executor, pulsar, initial_offset, keep_data):
    message_count = 10
    topic = get_random_string()
    builder = sdc_builder.get_pipeline_builder()

    consumer = builder.add_stage('Pulsar Consumer', type='origin')
    consumer.data_format = 'TEXT'
    consumer.topic = topic
    consumer.batch_wait_time_in_ms = 20000
    consumer.consumer_name = get_random_string()
    consumer.initial_offset = initial_offset

    wiretap = builder.add_wiretap()

    consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pipeline)

    client = pulsar.client
    admin = pulsar.admin
    try:
        producer = client.create_producer(topic)
        total_data = []
        for _ in range(message_count):
            actual_data = get_random_string()
            total_data.append(actual_data)
            producer.send(actual_data.encode())

        sdc_executor.start_pipeline(pipeline)

        if initial_offset == 'LATEST':
            assert len(wiretap.output_records) == 0
            total_data = []
            for _ in range(message_count):
                actual_data = get_random_string()
                total_data.append(actual_data)
                producer.send(actual_data.encode())

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', message_count, timeout_sec=60)
        sdc_executor.stop_pipeline(pipeline)

        records = [record.field['text'] for record in wiretap.output_records]
        assert len(records) == message_count
        assert all(element in total_data for element in records)

        wiretap.reset()

        total_data = []
        for _ in range(message_count):
            actual_data = get_random_string()
            total_data.append(actual_data)
            producer.send(actual_data.encode())

        sdc_executor.start_pipeline(pipeline)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', message_count, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        records = [record.field['text'] for record in wiretap.output_records]
        assert len(records) == message_count
        assert all(element in total_data for element in records)

    finally:
        if not keep_data:
            producer.close()
            client.close()
            admin.delete_topic(producer.topic())


@pulsar
def test_multiple_batch(sdc_builder, sdc_executor, pulsar, keep_data):
    message_count = 10_000
    topic = get_random_string()
    builder = sdc_builder.get_pipeline_builder()

    consumer = builder.add_stage('Pulsar Consumer', type='origin')
    consumer.data_format = 'TEXT'
    consumer.topic = topic
    consumer.batch_wait_time_in_ms = 20000
    consumer.consumer_name = get_random_string()
    consumer.initial_offset = 'EARLIEST'

    wiretap = builder.add_wiretap()

    consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pipeline)

    client = pulsar.client
    admin = pulsar.admin
    try:
        producer = client.create_producer(topic)
        total_data = []
        for _ in range(message_count):
            actual_data = get_random_string()
            total_data.append(actual_data)
            producer.send(actual_data.encode())

        sdc_executor.start_pipeline(pipeline)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', message_count, timeout_sec=60)
        sdc_executor.stop_pipeline(pipeline)

        records = [record.field['text'] for record in wiretap.output_records]
        assert len(records) == message_count
        assert all(element in total_data for element in records)
    finally:
        if not keep_data:
            producer.close()
            client.close()
            admin.delete_topic(producer.topic())