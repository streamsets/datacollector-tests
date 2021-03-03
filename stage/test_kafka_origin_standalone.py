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
import textwrap
import time

import pytest
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def kafka_check(cluster):
    if isinstance(cluster, ClouderaManagerCluster) and not hasattr(cluster, 'kafka'):
        pytest.skip('Kafka tests require Kafka to be installed on the cluster')


@cluster('cdh', 'kafka')
@sdc_min_version('3.7.0')
def test_kafka_origin_including_timestamps(sdc_builder, sdc_executor, cluster):
    """Check that timestamp and timestamp type are included in record header. Verifies that for previous versions of
    kafka (< 0.10), a validation issue is thrown.

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """
    MESSAGE = 'Hello World from SDC & DPM!'
    EXPECTED_OUTPUT = {'text': 'Hello World from SDC & DPM!'}

    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='TEXT',
                                  include_timestamps=True,
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    if any(stage_lib in cluster.sdc_stage_libs for stage_lib in ['streamsets-datacollector-apache-kafka_0_9-lib',
                                                                 'streamsets-datacollector-apache-kafka_0_8-lib']):
        with pytest.raises(Exception) as e:
            sdc_executor.start_pipeline(pipeline)
            assert ('KAFKA_75 - Inherited timestamps from Kafka are enabled but not supported in this Kafka version'
                    in e.value.message)
    else:
        producer = cluster.kafka.producer()
        producer.send(topic, MESSAGE.encode())
        producer.flush()

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert [record.field for record in wiretap.output_records] == [EXPECTED_OUTPUT]
        assert all('timestamp' in record.header.values for record in wiretap.output_records)
        assert all('timestampType' in record.header.values for record in wiretap.output_records)


@cluster('cdh', 'kafka')
@sdc_min_version('3.7.0')
def test_kafka_origin_timestamp_offset_strategy(sdc_builder, sdc_executor, cluster):
    """Accessing a topic for first time using TIMESTAMP offset strategy retrieves messages
    with timestamp >= Auto Offset Reset Timestamp configuration value.
    """
    INPUT_DATA = [f'message{i}' for i in range(5)]
    EXPECTED_OUTPUT = [{'text': f'message{i}'} for i in range(3, 5)]
    topic = get_random_string()

    # Send first 3 messages, save the timestamp to use, then send the last 2.
    for i in range(3):
        producer = cluster.kafka.producer()
        producer.send(topic, INPUT_DATA[i].encode())
        producer.flush()
    time.sleep(5)
    timestamp = int(time.time() * 1000)
    for i in range(3, 5):
        producer = cluster.kafka.producer()
        producer.send(topic, INPUT_DATA[i].encode())
        producer.flush()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(auto_offset_reset='TIMESTAMP',
                                  auto_offset_reset_timestamp_in_ms=timestamp,
                                  consumer_group=get_random_string(),
                                  data_format='TEXT',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)
    if any(stage_lib in cluster.sdc_stage_libs for stage_lib in ['streamsets-datacollector-apache-kafka_0_9-lib',
                                                                 'streamsets-datacollector-apache-kafka_0_8-lib',
                                                                 'streamsets-datacollector-cdh_kafka_2_1-lib',
                                                                 'streamsets-datacollector-apache-kafka_0_10-lib']):
        with pytest.raises(Exception) as e:
            sdc_executor.start_pipeline(pipeline)
        assert ("KAFKA_76 - Auto Offset Reset = 'Timestamp' can only be used for Kafka version >= 0.10.1.0"
                in e.value.message)
    else:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert [record.field for record in wiretap.output_records] == EXPECTED_OUTPUT


@cluster('cdh', 'kafka')
def test_kafka_origin_json_array_error(sdc_builder, sdc_executor, cluster):
    """SDC-15723: Improve behavior on JSON type missconfiguration. When user configures JSON
     parser to parse "Array" but the data aren't array"""

    message = '(not_a_valid_json)'
    expected_error_message = 'KAFKA_37 - Cannot parse record from message : Cannot parse JSON from record'

    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='JSON',
                                  json_content='ARRAY_OBJECTS',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    kafka_consumer >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    producer.send(topic, message.encode())
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)
    assert 1 == len(wiretap.error_records)
    error_message = wiretap.error_records[0].header['errorMessage'].split("'")
    received_error = error_message[0] + error_message[2]
    assert expected_error_message == received_error


@cluster('cdh', 'kafka')
def test_kafka_origin_xml_record_delimiter_element(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer parses XML data with a specified delimiter element."""
    MESSAGE = textwrap.dedent("""\
                              <developers>
                                <developer>Alex</developer>
                                <developer>Xavi</developer>
                              </developers>
                              """)
    EXPECTED_OUTPUT_ROOT_ELEMENT_PRESERVED = [{'developer': {'value': 'Alex'}}, {'developer': {'value': 'Xavi'}}]
    EXPECTED_OUTPUT_ROOT_ELEMENT_DISCARDED = [{'value': 'Alex'}, {'value': 'Xavi'}]
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='XML',
                                  delimiter_element='developer',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    producer.send(topic, MESSAGE.encode())
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert [record.field for record in wiretap.output_records] == (EXPECTED_OUTPUT_ROOT_ELEMENT_PRESERVED
                                                                   if getattr(kafka_consumer,
                                                                              'preserve_root_element',
                                                                              False)
                                                                   else EXPECTED_OUTPUT_ROOT_ELEMENT_DISCARDED)


@cluster('cdh', 'kafka')
@sdc_min_version('3.17.0')
def test_kafka_origin_csv_record_header_missmatch(sdc_builder, sdc_executor, cluster):
    """Write csv messages into Kafka and confirm that Kafka successfully reads them.

    Test delimited data format with 2 records, the first one containing an extra, unexpected column.
    We verify that the first one is sent to error and the second one is processed correctly so we have recovered
    properly from the ParserException

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    data = 'Name,Position\nAlex,Developer,1\nXavi,Developer'
    expected = {'Name': 'Xavi', 'Position': 'Developer'}
    expected_error = {'columns': ['Alex', 'Developer', '1'], 'headers': ['Name', 'Position']}

    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='DELIMITED',
                                  header_line='WITH_HEADER',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    producer.send(topic, data.encode())
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert [record.field for record in wiretap.output_records] == [expected]
    assert [record.field for record in wiretap.error_records] == [expected_error]


@cluster('cdh', 'kafka')
def test_kafka_consumer_null_payload(sdc_builder, sdc_executor, cluster):
    """Check that retrieving a message with null payload from Kafka using Kafka Consumer Origin does not return anything.
    The message should be written to Kafka without using SDC.

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> wiretap.destination
    """
    message = json.dumps({'abc': '123'})
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='JSON')
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    # Add messages, one of them with null payload
    producer = cluster.kafka.producer()
    producer.send(kafka_consumer.topic, message.encode())
    producer.send(kafka_consumer.topic, value=None, key='abc'.encode())
    producer.flush()

    sdc_executor.start_pipeline(pipeline).wait_for_finished()


    # Check the output records, there should be only 1 message, since the null should be discarded
    output_records = [record.field for record in wiretap.output_records]
    assert len(output_records) == 1
    # Check that the null record did not generate an exception either
    error_records = [record.field for record in wiretap.error_records]
    assert len(error_records) == 0