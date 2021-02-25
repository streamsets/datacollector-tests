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
from streamsets.testframework.decorators import stub
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string
import json

logger = logging.getLogger(__name__)


# Reference: https://stackoverflow.com/questions/37062904/what-are-apache-kafka-topic-name-limitations
KAFKA_NAMES = [
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


@pytest.fixture(autouse=True)
def kafka_check(cluster):
    if isinstance(cluster, ClouderaManagerCluster) and not hasattr(cluster, 'kafka'):
        pytest.skip('Kafka tests require Kafka to be installed on the cluster')


@cluster('cdh', 'kafka')
def test_data_types(sdc_builder, sdc_executor, cluster):
    pytest.skip("Kafka Destination doesn't talk to a structured system, so we don't need to test each data type.")


@cluster('cdh', 'kafka')
@pytest.mark.parametrize('test_name, topic_name', KAFKA_NAMES, ids=[t[0] for t in KAFKA_NAMES])
def test_object_names_topic(sdc_builder, sdc_executor, cluster, test_name, topic_name):
    """
    Verify that we can respect all the documented topic names possible.
    """
    raw_data = {'key': 'value'}

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    source = builder.add_stage('Dev Raw Data Source').set_attributes(
        data_format='JSON',
        raw_data=json.dumps(raw_data),
        stop_after_first_batch=True
    )

    destination = builder.add_stage(
        name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
        library=cluster.kafka.standalone_stage_lib
    ).set_attributes(
        topic=topic_name,
        data_format='JSON'
    )

    source >> destination

    pipeline = builder.build(title=f'Kafka Destination Topic Names {test_name}').configure_for_environment(cluster)
    pipeline.configuration['rateLimit'] = 1

    sdc_executor.add_pipeline(pipeline)

    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([topic_name])

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # There should be no messages in Kafka
    msgs_received = [json.loads(msg.value.decode()) for msg in consumer]
    assert 1 == len(msgs_received)
    assert raw_data == msgs_received[0]


@cluster('cdh', 'kafka')
def test_dataflow_events(sdc_builder, sdc_executor, cluster):
    pytest.skip('Kafka Destination does not generate events.')


@cluster('cdh', 'kafka')
def test_multiple_batch(sdc_builder, sdc_executor, cluster):
    """
    Test that we can produce multiple batches and the pipeline produces all the records.
    """
    topic = get_random_string()

    raw_data = {'key': 'value'}

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source').set_attributes(
        data_format='JSON',
        raw_data=json.dumps(raw_data),
        stop_after_first_batch=False
    )

    destination = builder.add_stage(
        name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
        library=cluster.kafka.standalone_stage_lib
    ).set_attributes(
        topic=topic,
        data_format='JSON'
    )

    source >> destination

    pipeline = builder.build(f'Kafka Destination Multiple Batches').configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 100)
    sdc_executor.stop_pipeline(pipeline)

    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([topic])

    msgs_received = [json.loads(message.value.decode()) for message in consumer]

    history = sdc_executor.get_pipeline_history(pipeline)
    history_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    assert len(msgs_received) == history_records
    assert all(msg == raw_data for msg in msgs_received)


@stub
def test_data_format_avro(sdc_builder, sdc_executor):
    pass


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


@cluster('cdh', 'kafka')
def test_push_pull(sdc_builder, sdc_executor, cluster):
    """
    We plan to verify that the connector works fine with Dev Raw Data Source and Dev Data Generator, an example of pull
    and push strategies, so as we already verified Dev Raw Data Source, we will use Dev Data Generator here to complete
    the coverage.
    """

    topic = get_random_string()

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Data Generator').set_attributes(
        batch_size=1,
        fields_to_generate=[{'field': 'stringField', 'type': 'STRING', 'precision': 10, 'scale': 2}]
    )

    destination = builder.add_stage(
        name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
        library=cluster.kafka.standalone_stage_lib
    ).set_attributes(
        topic=topic,
        data_format='JSON'
    )

    source >> destination

    pipeline = builder.build(f'Kafka Destination Push Pull').configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 20)
    sdc_executor.stop_pipeline(pipeline)

    history = sdc_executor.get_pipeline_history(pipeline)
    history_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([topic])

    msgs_received = [json.loads(message.value.decode()) for message in consumer]

    assert len(msgs_received) == history_records

