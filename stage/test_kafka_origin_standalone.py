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

import base64
import io
import json
import logging
import os
import textwrap
import time

import avro, avro.datafile
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
def test_kafka_origin_standalone(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer origin parses basic text formatted messages."""
    MESSAGE = 'Hello World from SDC & DPM!'
    EXPECTED = {'text': 'Hello World from SDC & DPM!'}
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='TEXT', topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    producer.send(topic, MESSAGE.encode())
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert [record.field for record in wiretap.output_records] == [EXPECTED]


@cluster('cdh', 'kafka')
@sdc_min_version('3.7.0')
def test_kafka_origin_including_timestamps(sdc_builder, sdc_executor, cluster):
    """Check that timestamp and timestamp type are included in record header. Verifies that for previous versions of
    kafka (< 0.10), a validation issue is thrown.

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """
    stage_libs = cluster.sdc_stage_libs

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


@pytest.mark.parametrize('data_type', ['ARRAY', 'ARRAY_OF_OBJECTS', 'OBJECT'])
@cluster('cdh', 'kafka')
def test_kafka_origin_json(sdc_builder, sdc_executor, data_type, cluster):
    """Kafka Consumer parses JSON in a variety of data types."""
    # We map data_type to input data as well as the expected output.
    DATA_TYPE = {'ARRAY': ['Alex', 'Xavi'],
                 'ARRAY_OF_OBJECTS': [{'Alex': 'Developer'}, {'Xavi': 'Developer'}],
                 'OBJECT': {'Alex': 'Developer', 'Xavi': 'Developer'}}
    message, expected_output = json.dumps(DATA_TYPE[data_type]), DATA_TYPE[data_type]

    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='JSON',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    producer.send(topic, message.encode())
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert [record.field for record in wiretap.output_records] == [expected_output]


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
    sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(pipeline)
    error_message = wiretap.error_records[0].header['errorMessage'].split("'")
    received_error = error_message[0] + error_message[2]
    assert expected_error_message == received_error


@cluster('cdh', 'kafka')
def test_kafka_origin_xml_record(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer parses XML data."""
    MESSAGE = textwrap.dedent("""\
                              <developers>
                                <developer>Alex</developer>
                                <developer>Xavi</developer>
                              </developers>
                              """)
    EXPECTED_OUTPUT_ROOT_ELEMENT_PRESERVED = {'developers': {'developer': [{'value': 'Alex'}, {'value': 'Xavi'}]}}
    EXPECTED_OUTPUT_ROOT_ELEMENT_DISCARDED = {'developer': [{'value': 'Alex'}, {'value': 'Xavi'}]}
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='XML',
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
    assert [record.field for record in wiretap.output_records] == ([EXPECTED_OUTPUT_ROOT_ELEMENT_PRESERVED]
                                                                   if getattr(kafka_consumer,
                                                                              'preserve_root_element',
                                                                              False)
                                                                   else [EXPECTED_OUTPUT_ROOT_ELEMENT_DISCARDED])


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
def test_kafka_origin_csv_record(sdc_builder, sdc_executor, cluster):
    """Write csv messages into Kafka and confirm that Kafka successfully reads them.
    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """
    MESSAGE = 'Alex,Xavi,Tucu,Martin'
    EXPECTED_OUTPUT = {'0': 'Alex', '1': 'Xavi', '2': 'Tucu', '3': 'Martin'}

    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='DELIMITED',
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
    assert [record.field for record in wiretap.output_records] == [EXPECTED_OUTPUT]


@cluster('cdh', 'kafka')
def test_avro_records_with_schema(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer parses Avro records with a schema."""
    DATA = {'name': 'boss', 'age': 60, 'emails': ['boss@company.com', 'boss2@company.com'], 'boss': None}
    SCHEMA = {'namespace': 'example.avro',
              'type': 'record',
              'name': 'Employee',
              'fields': [{'name': 'name', 'type': 'string'},
                         {'name': 'age', 'type': 'int'},
                         {'name': 'emails', 'type': {'type': 'array', 'items': 'string'}},
                         {'name': 'boss', 'type': ['Employee', 'null']}]}
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(avro_schema=json.dumps(SCHEMA),
                                  avro_schema_location='INLINE',
                                  data_format='AVRO',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    binary_stream = io.BytesIO()
    binary_encoder = avro.io.BinaryEncoder(binary_stream)
    datum_writer = avro.io.DatumWriter(avro.schema.Parse(kafka_consumer.avro_schema))
    datum_writer.write(DATA, binary_encoder)
    producer = cluster.kafka.producer()
    producer.send(topic, binary_stream.getvalue())
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert [record.field for record in wiretap.output_records] == [DATA]


@cluster('cdh', 'kafka')
def test_avro_records_without_schema(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer parses Avro records without a schema."""
    DATA = {'name': 'boss', 'age': 60, 'emails': ['boss@company.com', 'boss2@company.com'], 'boss': None}
    SCHEMA = {'namespace': 'example.avro',
              'type': 'record',
              'name': 'Employee',
              'fields': [{'name': 'name', 'type': 'string'},
                         {'name': 'age', 'type': 'int'},
                         {'name': 'emails', 'type': {'type': 'array', 'items': 'string'}},
                         {'name': 'boss', 'type': ['Employee', 'null']}]}
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(avro_schema=json.dumps(SCHEMA),
                                  avro_schema_location='SOURCE',
                                  data_format='AVRO',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    binary_stream = io.BytesIO()
    datum_writer = avro.io.DatumWriter(avro.schema.Parse(json.dumps(SCHEMA)))
    with avro.datafile.DataFileWriter(writer=binary_stream, datum_writer=datum_writer,
                                      writer_schema=avro.schema.Parse(json.dumps(SCHEMA))) as data_file_writer:
        data_file_writer.append(DATA)
        data_file_writer.flush()
        raw_bytes = binary_stream.getvalue()
    producer = cluster.kafka.producer()
    producer.send(topic, raw_bytes)
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert [record.field for record in wiretap.output_records] == [DATA]


@cluster('cdh', 'kafka')
def test_kafka_origin_syslog_message(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer parses syslog data."""
    MESSAGE = ('rO0ABXeOAAAAAQAAAAEAAAAAAAAAAQAJMTI3LjAuMC4xAAALuAAJMTI3LjAuMC4xAAAH0AAAAFw8MzQ+MSAyMDEzLTA2LTI4VDA2Oj'
               'E0OjU2LjAwMCswMjowMCBteW1hY2hpbmUgc3U6ICdzdSByb290JyBmYWlsZWQgZm9yIGxvbnZpY2sgb24gL2Rldi9wdHMvOA==')
    EXPECTED_OUTPUT = {
        'facility': 4,
        'host': 'mymachine',
        'priority': 34,
        'raw': "<34>1 2013-06-28T06:14:56.000+02:00 mymachine su: 'su root' failed for lonvick on /dev/pts/8",
        'receiverAddr': '127.0.0.1:2000',
        'receiverPort': 2000,
        'remaining': "su: 'su root' failed for lonvick on /dev/pts/8",
        'senderAddr': '127.0.0.1:3000',
        'senderPort': 3000,
        'severity': 2,
        'timestamp': 1372392896000,
        'version': 1
    }
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='DATAGRAM',
                                  datagram_packet_format='SYSLOG',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    producer.send(topic, base64.b64decode(MESSAGE))
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert [record.field for record in wiretap.output_records] == [EXPECTED_OUTPUT]


@cluster('cdh', 'kafka')
def test_kafka_origin_binary_record(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer origin reads binary messages."""
    MESSAGES = [b'message 1', b'message 2']
    EXPECTED_OUTPUT = [b'message 1', b'message 2']
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='BINARY',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    for message in MESSAGES:
        producer.send(topic, message)
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert [record.field for record in wiretap.output_records] == EXPECTED_OUTPUT


@cluster('cdh', 'kafka')
def test_kafka_origin_netflow_message(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer origin processes UDP datagram mode NETFLOW messages."""
    MESSAGE = ('rO0ABXoAAAIqAAAAAQAAAAIAAAAAAAAAAQAJMTI3LjAuMC4xAAALuAAJMTI3LjAuMC4xAAAH0AAAAfgABQAKAAAAAFVFcOIBWL'
               'IwAAAAAAAAAAD3waSb49Wa8QAAAAAAAAAAAAAAAQAAAFlnyqItZ8qiLQA1JA8AABEAAAAAAAAAAAD3waSb49Wa8QAAAAAAAAAA'
               'AAAAAQAAAFlnyqItZ8qiLQA1+ioAABEAAAAAAAAAAAD3waSb49Wa8QAAAAAAAAAAAAAAAQAAAFlnyqItZ8qiLQA1SWAAABEAAA'
               'AAAAAAAAD55boV49Wa8QAAAAAAAAAAAAAAAQAAAFlnyqIvZ8qiLwA1q94AABEAAAAAAAAAAAB/472549Wa8QAAAAAAAAAAAAAA'
               'AQAAAFlnyqIvZ8qiLwA1IlYAABEAAAAAAAAAAAB/472549Wa8QAAAAAAAAAAAAAAAQAAAFlnyqIvZ8qiLwA1l5sAABEAAAAAAA'
               'AAAAB/472549Wa8QAAAAAAAAAAAAAAAQAAAFlnyqIvZ8qiLwA1u4EAABEAAAAAAAAAAAD55boV49Wa8QAAAAAAAAAAAAAAAQAA'
               'AFlnyqIvZ8qiLwA14OQAABEAAAAAAAAAAAAtZyl349Wa8QAAAAAAAAAAAAAAAQAAArhnyqIxZ8qiMQA11FQAABEAAAAAAAAAAA'
               'B5SzUv49Wa8QAAAAAAAAAAAAAAAQAAAfhnyqIyZ8qiMgA1FbUAABEAAAAAAAAAAAA=')
    # The expected output is huge, so we assert only number of records processed and the contents of the first message.
    # Note that the 'id' field is generated uniquely, so we exclude it from our expected output.
    EXPECTED_OUTPUT_FIRST_RECORD = {'count': 10, 'dOctets': 89, 'dPkts': 1, 'dst_mask': 0, 'dstaddr': -472540431,
                                    'dstaddr_s': '227.213.154.241', 'dstas': 0, 'dstport': 9231, 'engineid': 0,
                                    'enginetype': 0, 'first': 1432355575059, 'flowseq': 0,
                                    'last': 1432355575059, 'length': 504, 'nanos': 22590000, 'nexthop': 0,
                                    'nexthop_s': '0.0.0.0', 'packetid': '2a9ac4fc-7c25-1000-8080-808080808080',
                                    'proto': 17, 'raw_first': 1741333037, 'raw_last': 1741333037, 'raw_sampling': 0,
                                    'readerId': '/127.0.0.1:2000', 'samplingint': 0, 'samplingmode': 0,
                                    'seconds': 1430614242, 'sender': '/127.0.0.1', 'snmpinput': 0, 'snmponput': 0,
                                    'src_mask': 0, 'srcaddr': -138304357, 'srcaddr_s': '247.193.164.155',
                                    'srcas': 0, 'srcport': 53, 'tcp_flags': 0, 'timestamp': 1430614242022, 'tos': 0,
                                    'uptime': 0, 'version': 5}
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='DATAGRAM',
                                  datagram_data_format='NETFLOW',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    producer.send(topic, base64.b64decode(MESSAGE))
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    output_records = [record.field for record in wiretap.output_records]
    assert len(output_records) == 10 and all(item in output_records[0].items()
                                             for item in EXPECTED_OUTPUT_FIRST_RECORD.items())


@cluster('cdh', 'kafka')
def test_kafka_origin_collectd_message(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer origin parses COLLECTD message."""
    MESSAGE = (
        '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')
    # Only looking at the first record being captured.
    EXPECTED_OUTPUT = {'plugin_instance': 'lo0',
                       'plugin': 'interface',
                       'tx': 0,
                       'rx': 0,
                       'host': 'ip-192-168-42-238.us-west-2.compute.internal',
                       'time_hires': 1543518938371396391,
                       'type': 'if_errors'}
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='DATAGRAM',
                                  datagram_data_format='COLLECTD',
                                  topic=topic)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    kafka_consumer >> [wiretap.destination, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    producer = cluster.kafka.producer()
    producer.send(topic, base64.b64decode(MESSAGE))
    producer.flush()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert wiretap.output_records[0].field == EXPECTED_OUTPUT


@cluster('cdh', 'kafka')
def test_kafka_origin_log_record(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer origin processes Log4j records."""
    MESSAGE = '200 [main] DEBUG org.StreamSets.Log4j unknown - This is a sample log message'
    EXPECTED_OUTPUT = {'category': 'org.StreamSets.Log4j',
                       'message': 'This is a sample log message',
                       'ndc': 'unknown',
                       'relativetime': '200',
                       'severity': 'DEBUG',
                       'thread': 'main'}
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(data_format='LOG',
                                  log_format='LOG4J',
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
    assert [record.field for record in wiretap.output_records] == [EXPECTED_OUTPUT]


@cluster('cdh', 'kafka')
def test_kafka_origin_protobuf_record(sdc_builder, sdc_executor, cluster):
    """Kafka Consumer parses records in protobuf format."""

    # Message comes from generating Python source files from the proto file and using them as follows:
    # >>> import addressbook_pb2
    # >>> from google.protobuf.internal.encoder import _VarintBytes
    # >>> contact = addressbook_pb2.Contact()
    # >>> contact.first_name = 'Martin'
    # >>> contact.last_name = 'Balzamo'
    # >>> _VarintBytes(contact.ByteSize()) + contact.SerializeToString()
    # b'\x11\n\x06Martin\x12\x07Balzamo'
    #
    # Note that the VarintBytes call be omitted if each Kafka message contains one protobuf message
    # (i.e. kafka_consumer.delimited_messages = False).
    MESSAGE = b'\x11\n\x06Martin\x12\x07Balzamo'
    MESSAGE_TYPE = 'Contact'
    EXPECTED_OUTPUT = {'first_name': 'Martin', 'last_name': 'Balzamo'}
    topic = get_random_string()
    protobuf_descriptor_filename = f'{get_random_string()}.desc'

    # We keep a pre-compiled Protobuf descriptor file in the stages/resources/protobuf folder. We write this
    # into the Data Collector instance's SDC_RESOURCES folder.
    try:
        with open(os.path.join(os.path.dirname(__file__), 'resources', 'protobuf', 'addressbook.desc'), 'rb') as f:
            sdc_executor.write_file(f'${{SDC_RESOURCES}}/{protobuf_descriptor_filename}', f.read().decode('latin-1'))
        producer = cluster.kafka.producer()
        producer.send(topic, MESSAGE)
        producer.flush()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        kafka_consumer = pipeline_builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
        kafka_consumer.set_attributes(data_format='PROTOBUF',
                                      message_type=MESSAGE_TYPE,
                                      protobuf_descriptor_file=protobuf_descriptor_filename,
                                      topic=topic)
        wiretap = pipeline_builder.add_wiretap()
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        kafka_consumer >> [wiretap.destination, pipeline_finisher]
        pipeline = pipeline_builder.build().configure_for_environment(cluster)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert [record.field for record in wiretap.output_records] == [EXPECTED_OUTPUT]
    finally:
        sdc_executor.execute_shell(f'rm ${{SDC_RESOURCES}}/{protobuf_descriptor_filename}')


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
