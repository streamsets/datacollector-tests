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
from pulsar import MessageId
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import pulsar, sdc_min_version
from streamsets.testframework.utils import get_random_string
import json

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


@pulsar
def test_data_types(sdc_builder, sdc_executor, pulsar):
    pytest.skip("Pulsar isn't a typed data store")


@pulsar
@pytest.mark.parametrize('test_name, topic_name', TOPIC_NAMES, ids=[t[0] for t in TOPIC_NAMES])
def test_object_names_topic(sdc_builder, sdc_executor, pulsar, test_name, topic_name, keep_data):
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'TEXT'
    source.raw_data = 'Hi!'
    source.stop_after_first_batch = True

    producer = builder.add_stage('Pulsar Producer')
    producer.topic = topic_name
    producer.data_format = 'TEXT'

    source >> producer

    pipeline = builder.build().configure_for_environment(pulsar)
    pipeline.configuration['rateLimit'] = 1

    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    messages = _dump_messages_and_clean_up(topic_name, pulsar, keep_data)
    assert messages == ["Hi!"]


@pulsar
def test_dataflow_events(sdc_builder, sdc_executor, pulsar):
    pytest.skip('Pulsar Origin does not produce events')


@pulsar
def test_multiple_batch(sdc_builder, sdc_executor, pulsar, keep_data):
    batch_size = 100
    batches = 10
    topic = get_random_string()

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.delay_between_batches = 0
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    producer = builder.add_stage('Pulsar Producer')
    producer.topic = topic
    producer.data_format = 'JSON'
    producer.async_send = False

    origin >> producer

    pipeline = builder.build().configure_for_environment(pulsar)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', batch_size * batches)
    sdc_executor.stop_pipeline(pipeline)

    history = sdc_executor.get_pipeline_history(pipeline)
    recordsCount = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

    logger.info(f"Wrote {recordsCount} records")
    messages = _dump_messages_and_clean_up(topic, pulsar, keep_data)
    sequence = [int(json.loads(m)['seq']) for m in messages]
    assert sorted(sequence) == [*range(0, recordsCount)]


@pulsar
def test_push_pull(sdc_builder, sdc_executor, cluster):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is art of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")


@stub
def test_data_format_binary(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_delimited(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_json(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_protobuf(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_text(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_sdc_record(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_xml(sdc_builder, sdc_executor):
    pass


def _dump_messages_and_clean_up(topic_name, pulsar, keep_data):
    msgs_received = []
    client = pulsar.client
    admin = pulsar.admin
    try:
        reader = client.create_reader(topic_name, MessageId.earliest)
        while reader.has_message_available():
            msgs_received.append(reader.read_next().data().decode().strip()) # strip to remove newlines
    finally:
        reader.close() # reader needs to be closed before topic can be deleted without force
        client.close()
        if not keep_data:
            admin.delete_topic(reader.topic())

    logger.debug('Number of messages received from Pulsar = %d', len(msgs_received))
    return msgs_received
