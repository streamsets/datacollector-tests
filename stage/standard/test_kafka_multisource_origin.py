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

import base64
import datetime
import io
import logging
import os
import json
import string
import textwrap

import avro, avro.datafile
import pytest
from streamsets.testframework.decorators import stub
from streamsets.testframework.environments import cloudera
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string

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
    pytest.skip("Kafka Multitopic Origin doesn't talk to a structured system, so we don't need to test each data type.")


@cluster('cdh', 'kafka')
@pytest.mark.parametrize('test_name, topic_name', KAFKA_NAMES, ids=[t[0] for t in KAFKA_NAMES])
def test_object_names_topic(sdc_builder, sdc_executor, cluster, test_name, topic_name):
    """
    Verify that we can respect all the documented topic names possible.
    """
    message = 'Hello World from SDC & DPM!'
    expected_output = {'text': 'Hello World from SDC & DPM!'}

    pipeline_builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic_name])

    wiretap = pipeline_builder.add_wiretap()

    kafka_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(cluster)

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

    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.set_attributes(consumer_group=consumer_group_name)

    wiretap = pipeline_builder.add_wiretap()

    kafka_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(cluster)

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
    records_per_iteration = 100

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

    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.set_attributes(
        batch_wait_time_in_ms=batch_wait_time,
        auto_offset_reset=auto_offset_reset
    )
    if auto_offset_reset == 'TIMESTAMP':
        current_ts = '%f' % datetime.datetime.now().timestamp()
        kafka_consumer.set_attributes(auto_offset_reset_timestamp_in_ms=int(current_ts[0:current_ts.find('.')]))

    wiretap = pipeline_builder.add_wiretap()

    kafka_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    producer = cluster.kafka.producer()
    try:
        # Let's try 5 start/stop cycles
        for iteration in range(5):
            total_data = []
            for _ in range(records_per_iteration):
                actual_data = get_random_string()
                total_data.append(actual_data)
                producer.send(topic, actual_data.encode())
            producer.flush()
            sdc_executor.start_pipeline(pipeline)

            if auto_offset_reset == 'LATEST' and iteration == 0:
                assert len(wiretap.output_records) == 0
                total_data = []
                for _ in range(records_per_iteration):
                    actual_data = get_random_string()
                    total_data.append(actual_data)
                    producer.send(topic, actual_data.encode())
                producer.flush()

            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', records_per_iteration, timeout_sec=300)
            sdc_executor.stop_pipeline(pipeline)

            first_iteration_records = [record.field['text'] for record in wiretap.output_records]
            assert len(first_iteration_records) == records_per_iteration
            assert all(element in total_data for element in first_iteration_records)

            wiretap.reset()
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

    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.set_attributes(max_batch_size_in_records=50)

    wiretap = pipeline_builder.add_wiretap()

    kafka_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    producer = cluster.kafka.producer()

    total_data = []
    for _ in range(1000):
        actual_data = get_random_string()
        total_data.append(actual_data)
        producer.send(topic, actual_data.encode())
    producer.flush()
    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1000)
    sdc_executor.stop_pipeline(pipeline)

    records = [record.field['text'] for record in wiretap.output_records]

    assert len(records) == len(total_data)
    assert all(element in records for element in total_data)
    assert all(element in total_data for element in records)


@cluster('cdh', 'kafka')
def test_data_format_avro_with_schema(sdc_builder, sdc_executor, cluster):
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
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.avro_schema = json.dumps(SCHEMA)
    kafka_consumer.avro_schema_location = 'INLINE'
    kafka_consumer.data_format = 'AVRO'
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
def test_data_format_avro_without_schema(sdc_builder, sdc_executor, cluster):
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
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.avro_schema = json.dumps(SCHEMA)
    kafka_consumer.avro_schema_location = 'SOURCE'
    kafka_consumer.data_format = 'AVRO'
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
def test_data_format_binary(sdc_builder, sdc_executor, cluster):
    MESSAGES = [b'message 1', b'message 2']
    EXPECTED_OUTPUT = [b'message 1', b'message 2']
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.data_format = 'BINARY'
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
def test_data_format_datagram_syslog(sdc_builder, sdc_executor, cluster):
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
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.data_format = 'DATAGRAM'
    kafka_consumer.datagram_packet_format = 'SYSLOG'
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
def test_data_format_datagram_netflow(sdc_builder, sdc_executor, cluster):
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
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.data_format = 'DATAGRAM'
    kafka_consumer.datagram_packet_format = 'NETFLOW'
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
def test_data_format_datagram_collectd(sdc_builder, sdc_executor, cluster):
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
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.data_format = 'DATAGRAM'
    kafka_consumer.datagram_packet_format = 'COLLECTD'
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
@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ['LEGACY_PARSER', 'UNIVOCITY'])
def test_data_format_delimited(sdc_builder, sdc_executor, csv_parser, cluster):
    MESSAGE = 'Alex,Xavi,Tucu,Martin'
    EXPECTED_OUTPUT = {'0': 'Alex', '1': 'Xavi', '2': 'Tucu', '3': 'Martin'}

    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.data_format = 'DELIMITED'
    kafka_consumer.csv_parser = csv_parser

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


@pytest.mark.parametrize('data_type', ['ARRAY', 'ARRAY_OF_OBJECTS', 'OBJECT'])
@cluster('cdh', 'kafka')
def test_data_format_json(sdc_builder, sdc_executor, cluster, data_type):
    """Kafka Consumer parses JSON in a variety of data types."""
    # We map data_type to input data as well as the expected output.
    DATA_TYPE = {'ARRAY': ['Alex', 'Xavi'],
                 'ARRAY_OF_OBJECTS': [{'Alex': 'Developer'}, {'Xavi': 'Developer'}],
                 'OBJECT': {'Alex': 'Developer', 'Xavi': 'Developer'}}
    message, expected_output = json.dumps(DATA_TYPE[data_type]), DATA_TYPE[data_type]

    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.data_format = 'JSON'

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
def test_data_format_log(sdc_builder, sdc_executor, cluster):
    MESSAGE = '200 [main] DEBUG org.StreamSets.Log4j unknown - This is a sample log message'
    EXPECTED_OUTPUT = {'category': 'org.StreamSets.Log4j',
                       'message': 'This is a sample log message',
                       'ndc': 'unknown',
                       'relativetime': '200',
                       'severity': 'DEBUG',
                       'thread': 'main'}
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.data_format = 'LOG'
    kafka_consumer.log_format = 'LOG4J'

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
def test_data_format_protobuf(sdc_builder, sdc_executor, cluster):
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
        kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
        kafka_consumer.data_format = 'PROTOBUF'
        kafka_consumer.message_type = MESSAGE_TYPE
        kafka_consumer.protobuf_descriptor_file = protobuf_descriptor_filename

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
def test_data_format_text(sdc_builder, sdc_executor, cluster):
    MESSAGE = 'Hello World from SDC & DPM!'
    EXPECTED = {'text': 'Hello World from SDC & DPM!'}
    topic = get_random_string()

    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.data_format = 'TEXT'

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


@stub
def test_data_format_sdc_record(sdc_builder, sdc_executor):
    pass


@cluster('cdh', 'kafka')
def test_data_format_xml(sdc_builder, sdc_executor, cluster):
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
    kafka_consumer = _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, [topic])
    kafka_consumer.data_format = 'XML'

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


def _get_kafka_multitopic_consumer_stage(pipeline_builder, cluster, topic_list):
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

    # Default stage configuration.
    kafka_multitopic_consumer.set_attributes(data_format='TEXT',
                                             batch_wait_time_in_ms=20000,
                                             consumer_group=get_random_string(),
                                             topic_list=topic_list)

    return kafka_multitopic_consumer
