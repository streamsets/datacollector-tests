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
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string
import datetime

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
    pytest.skip("Kafka Standalone Origin doesn't talk to a structured system, so we don't need to test each data type.")


@cluster('cdh', 'kafka')
@pytest.mark.parametrize('test_name, topic_name', KAFKA_NAMES, ids=[t[0] for t in KAFKA_NAMES])
def test_object_names_topic(sdc_builder, sdc_executor, cluster, test_name, topic_name):
    """
    Verify that we can respect all the documented topic names possible.
    """
    message = 'Hello World from SDC & DPM!'
    expected_output = {'text': 'Hello World from SDC & DPM!'}

    pipeline_builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(
        batch_wait_time_in_ms=20_000,
        data_format='TEXT',
        consumer_group=get_random_string(),
        topic=topic_name
    )

    wiretap = pipeline_builder.add_wiretap()

    kafka_consumer >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Kafka Standalone Origin Topic Names {test_name}')\
        .configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    producer = cluster.kafka.producer()
    producer.send(topic_name, message.encode())
    producer.flush()

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=120)
    sdc_executor.stop_pipeline(pipeline)

    assert [record.field for record in wiretap.output_records] == [expected_output]


@cluster('cdh', 'kafka')
@pytest.mark.parametrize('test_name, consumer_group_name', KAFKA_NAMES, ids=[t[0] for t in KAFKA_NAMES])
def test_object_names_consumer_group(sdc_builder, sdc_executor, cluster, test_name, consumer_group_name):
    """
    Verify that we can respect all the documented topic names possible. There is no documentation about what
    consumer group names are supported, so we try the same pattern as with the topics.
    """
    topic = get_random_string()

    message = 'Hello World from SDC & DPM!'
    expected_output = {'text': 'Hello World from SDC & DPM!'}

    pipeline_builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(
        batch_wait_time_in_ms=20_000,
        data_format='TEXT',
        topic=topic,
        consumer_group=consumer_group_name
    )

    wiretap = pipeline_builder.add_wiretap()

    kafka_consumer >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Kafka Standalone Origin Consumer Group Names {test_name}')\
        .configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    producer = cluster.kafka.producer()
    producer.send(topic, message.encode())
    producer.flush()

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)

    assert [record.field for record in wiretap.output_records] == [expected_output]


@cluster('cdh', 'kafka')
def test_dataflow_events(sdc_builder, sdc_executor, cluster):
    pytest.skip('Kafka Standalone Origin does not generate events.')


@cluster('cdh', 'kafka')
@pytest.mark.parametrize('auto_offset_reset', ['EARLIEST', 'LATEST', 'TIMESTAMP'])
def test_resume_offset(sdc_builder, sdc_executor, cluster, auto_offset_reset):
    """
    Test that we can start our pipeline multiple times without reading any duplicated record neither missing them.
    """

    if auto_offset_reset == 'TIMESTAMP' and any(
            stage_lib in cluster.sdc_stage_libs for stage_lib in ['streamsets-datacollector-apache-kafka_0_9-lib',
                                                                  'streamsets-datacollector-apache-kafka_0_8-lib',
                                                                  'streamsets-datacollector-cdh_kafka_2_1-lib',
                                                                  'streamsets-datacollector-apache-kafka_0_10-lib']):
        pytest.skip('Auto Offset Reset = \'Timestamp\' can only be used for Kafka version >= 0.10.1.0')

    topic = get_random_string()
    batch_wait_time = 20_000 if auto_offset_reset != 'NONE' else 1_000

    # Build pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(
        batch_wait_time_in_ms=batch_wait_time,
        data_format='TEXT',
        topic=topic,
        max_batch_size_in_records=20,
        auto_offset_reset=auto_offset_reset,
        consumer_group=get_random_string()
    )
    if auto_offset_reset == 'TIMESTAMP':
        current_ts = '%f' % datetime.datetime.now().timestamp()
        kafka_consumer.set_attributes(auto_offset_reset_timestamp_in_ms=int(current_ts[0:current_ts.find('.')]))

    delay = pipeline_builder.add_stage('Delay').set_attributes(
        delay_between_batches=5 * 1000
    )

    wiretap = pipeline_builder.add_wiretap()

    kafka_consumer >> delay >> wiretap.destination

    pipeline = pipeline_builder.build(f'Kafka Standalone Origin Resume Offset {auto_offset_reset}')\
        .configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    producer = cluster.kafka.producer()

    try:
        total_data = []
        for _ in range(100):
            actual_data = get_random_string()
            total_data.append(actual_data)
            producer.send(topic, actual_data.encode())
        producer.flush()

        sdc_executor.start_pipeline(pipeline)

        if auto_offset_reset == 'LATEST':
            assert len(wiretap.output_records) == 0

            total_data = []
            for _ in range(100):
                actual_data = get_random_string()
                total_data.append(actual_data)
                producer.send(topic, actual_data.encode())
            producer.flush()

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 20, timeout_sec=60)
        sdc_executor.stop_pipeline(pipeline)
        first_iteration_records = [record.field['text'] for record in wiretap.output_records]

        assert len(first_iteration_records) != 0
        assert len(first_iteration_records) < 100
        assert all(element in total_data for element in first_iteration_records)

        wiretap.reset()

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 100 - len(first_iteration_records),
                                              timeout_sec=300)

        second_iteration_records = [record.field['text'] for record in wiretap.output_records]

        assert len(second_iteration_records) != 0
        assert len(second_iteration_records) + len(first_iteration_records) == len(total_data)
        assert all(element in (second_iteration_records + first_iteration_records) for element in total_data)
        assert all(element in total_data for element in (second_iteration_records + first_iteration_records))
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@cluster('cdh', 'kafka')
def test_multiple_batch(sdc_builder, sdc_executor, cluster):
    """
    Test that we can consume multiple batches and the pipeline produces all the records.
    """
    topic = get_random_string()

    # Build pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(
        batch_wait_time_in_ms=20_000,
        data_format='TEXT',
        topic=topic,
        consumer_group=get_random_string(),
        max_batch_size_in_records=50
    )

    wiretap = pipeline_builder.add_wiretap()

    kafka_consumer >> wiretap.destination

    pipeline = pipeline_builder.build(f'Kafka Standalone Origin Multiple Batches').configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    producer = cluster.kafka.producer()
    
    total_data = []
    for _ in range(100):
        actual_data = get_random_string()
        total_data.append(actual_data)
        producer.send(topic, actual_data.encode())
    producer.flush()

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 100)
    sdc_executor.stop_pipeline(pipeline)

    records = [record.field['text'] for record in wiretap.output_records]

    assert len(records) == len(total_data)
    assert all(element in records for element in total_data)
    assert all(element in total_data for element in records)
