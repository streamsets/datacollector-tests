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
import string

import avro
import pytest
from avro.datafile import DataFileWriter

from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000
SNAPSHOT_TIMEOUT_SEC = 120

# Protobuf file path relative to $SDC_RESOURCES.
PROTOBUF_FILE_PATH = 'resources/protobuf/addressbook.desc'

SCHEMA = {
    'namespace': 'example.avro',
    'type': 'record',
    'name': 'Employee',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'age', 'type': 'int'},
        {'name': 'emails', 'type': {'type': 'array', 'items': 'string'}},
        {'name': 'boss', 'type': ['Employee', 'null']}
    ]
}


def get_kafka_consumer_stage(pipeline_builder, cluster):
    """Create and return a Kafka origin stage depending on execution mode for the pipeline."""
    # Default on error action.
    pipeline_builder.add_error_stage('Discard')

    kafka_consumer = pipeline_builder.add_stage('Kafka Consumer',
                                                type='origin',
                                                library=cluster.kafka.standalone_stage_lib)
    # Default stage configuration.
    kafka_consumer.set_attributes(data_format='TEXT',
                                  batch_wait_time_in_ms=20000,
                                  topic=get_random_string(string.ascii_letters, 10),
                                  kafka_configuration=[{'key': 'auto.offset.reset', 'value': 'earliest'}])

    return kafka_consumer


def produce_kafka_messages(topic, cluster, message, data_format):
    """Send basic messages to Kafka"""
    # Get Kafka producer
    producer = cluster.kafka.producer()

    basic_data_formats = ['XML', 'CSV', 'SYSLOG', 'NETFLOW', 'COLLECTD', 'BINARY', 'LOG', 'TEXT', 'JSON']

    # Write records into Kafka depending on the data_format.
    if data_format in basic_data_formats:
        producer.send(topic, message)

    elif data_format == 'WITH_KEY':
        producer.send(topic, message, key=get_random_string(string.ascii_letters, 10).encode())

    elif data_format == 'AVRO':
        writer = avro.io.DatumWriter(avro.schema.Parse(json.dumps(SCHEMA)))
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(message, encoder)
        raw_bytes = bytes_writer.getvalue()
        producer.send(topic, raw_bytes)

    elif data_format == 'AVRO_WITHOUT_SCHEMA':
        bytes_writer = io.BytesIO()
        datum_writer = avro.io.DatumWriter(avro.schema.Parse(json.dumps(SCHEMA)))
        data_file_writer = DataFileWriter(writer=bytes_writer, datum_writer=datum_writer,
                                          writer_schema=avro.schema.Parse(json.dumps(SCHEMA)))
        data_file_writer.append(message)
        data_file_writer.flush()
        raw_bytes = bytes_writer.getvalue()
        data_file_writer.close()
        producer.send(topic, raw_bytes)

    producer.flush()


def verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, message, data_format):
    """Start, stop pipeline and verify results using snapshot"""

    # Start Pipeline.
    snapshot_pipeline_command = sdc_executor.capture_snapshot(kafka_consumer_pipeline, start_pipeline=True, wait=False)

    logger.debug('Finish the snapshot and verify')
    snapshot_command = snapshot_pipeline_command.wait_for_finished(timeout_sec=SNAPSHOT_TIMEOUT_SEC)
    snapshot = snapshot_command.snapshot

    basic_data_formats = ['XML', 'CSV', 'SYSLOG', 'COLLECTD', 'TEXT', 'JSON', 'AVRO', 'AVRO_WITHOUT_SCHEMA']

    # Verify snapshot data.
    if data_format in basic_data_formats:
        record_field = [record.field for record in snapshot[kafka_consumer_pipeline[0].instance_name].output]
        assert message == str(record_field[0])

    elif data_format == 'LOG':
        stage = snapshot[kafka_consumer_pipeline[0].instance_name]
        assert 0 == len(stage.error_records)
        record_field = [record.field for record in snapshot[kafka_consumer_pipeline[0].instance_name].output]
        assert message == str(record_field[0]['originalLine'])

    elif data_format == 'BINARY':
        record_field = [record.field for record in snapshot[kafka_consumer_pipeline[0].instance_name].output]
        assert message == record_field[0]

    elif data_format == 'PROTOBUF':
        record_field = [record.field for record in snapshot[kafka_consumer_pipeline[0].instance_name].output]
        assert message in str(record_field[0])

    elif data_format == 'XML_MULTI_ELEMENT':
        record_field = [record.field for record in snapshot[kafka_consumer_pipeline[0].instance_name].output]
        assert message[0] in str(record_field[0])
        assert message[1] in str(record_field[1])

    elif data_format == 'NETFLOW':
        record_field = [record.field for record in snapshot[kafka_consumer_pipeline[0].instance_name].output]
        assert message[0] in str(record_field)
        assert message[1] in str(record_field)

    elif data_format == 'TEXT_TIMESTAMP':
        record_field = [record.field for record in snapshot[kafka_consumer_pipeline[0].instance_name].output]
        record_header = [record.header for record in snapshot[kafka_consumer_pipeline[0].instance_name].output]
        for element in record_header:
            logger.debug('ELEMENT: %s', element['values'])
            assert 'timestamp' in element['values']
            assert 'timestampType' in element['values']
        assert message == str(record_field[0])


@cluster('cdh', 'kafka')
def test_kafka_origin_standalone(sdc_builder, sdc_executor, cluster):
    """Write simple text messages into Kafka and confirm that Kafka successfully reads them.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = 'Hello World from SDC & DPM!'
    expected = '{\'text\': Hello World from SDC & DPM!}'

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Standalone pipeline').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, message.encode(), 'TEXT')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'TEXT')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
@sdc_min_version('3.6.0')
def test_kafka_origin_including_timestamps(sdc_builder, sdc_executor, cluster):
    """Check that timestamp and timestamp type are included in record header. Verifies that for previous versions of
    kafka (< 0.10), a validation issue is thrown.

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """
    stage_libs = cluster.sdc_stage_libs

    message = 'Hello World from SDC & DPM!'
    expected = '{\'text\': Hello World from SDC & DPM!}'

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster)
    kafka_consumer.set_attributes(include_timestamps=True)

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Standalone pipeline').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    if ('streamsets-datacollector-apache-kafka_0_9-lib' in stage_libs or
            'streamsets-datacollector-apache-kafka_0_8-lib' in stage_libs):
        with pytest.raises(Exception) as e:
            sdc_executor.start_pipeline(kafka_consumer_pipeline)

        assert ('KAFKA_75 - Inherited timestamps from Kafka are enabled but not supported in this Kafka version'
                in e.value.message)
    else:
        try:
            # Publish messages to Kafka and verify using snapshot if the same messages are received.
            produce_kafka_messages(kafka_consumer.topic, cluster, message.encode(), 'TEXT')
            verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'TEXT_TIMESTAMP')
        finally:
            sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_multiple_partitions(sdc_builder, sdc_executor, cluster):
    """Write simple text messages into Kafka and confirm that Kafka successfully reads them using different partitions.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = 'Hello World from SDC & DPM!'
    expected = '{\'text\': Hello World from SDC & DPM!}'

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Standalone pipeline').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, message.encode(), 'WITH_KEY')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'TEXT')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
@sdc_min_version('3.0.0.0')
def test_kafka_multi_origin_standalone(sdc_builder, sdc_executor, cluster):
    """Write simple text messages into Kafka and confirm that MultiTopic origin can read them.
    Specifically, this would look like:

    Kafka Multi Topic Consumer Origin pipeline with standalone mode:
        kafka_multitopic_consumer >> trash
    """

    message = 'Hello World from SDC & DPM!'
    expected = '{\'text\': Hello World from SDC & DPM!}'

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()

    topic_name = get_random_string(string.ascii_letters, 10)
    kafka_multitopic_consumer = builder.add_stage('Kafka Multitopic Consumer')
    kafka_multitopic_consumer.set_attributes(data_format='TEXT',
                                             batch_wait_time_in_ms=2000,
                                             topic_list=[topic_name],
                                             configuration_properties=[{'key': 'auto.offset.reset',
                                                                        'value': 'earliest'}])

    trash = builder.add_stage(label='Trash')
    kafka_multitopic_consumer >> trash
    kafka_multitopic_consumer_pipeline = builder.build(title='Kafka Multitopic').configure_for_environment(cluster)
    kafka_multitopic_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'
    kafka_multitopic_consumer_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(kafka_multitopic_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(topic_name, cluster, message.encode(), 'TEXT')
        verify_kafka_origin_results(kafka_multitopic_consumer_pipeline, sdc_executor, expected, 'TEXT')
    finally:
        sdc_executor.stop_pipeline(kafka_multitopic_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_string_records(sdc_builder, sdc_executor, cluster):
    """Write simple text messages into Kafka and confirm that Kafka successfully reads them.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = 'This is a String record'
    expected = '{\'text\': This is a String record}'

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Standalone pipeline').configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, message.encode(), 'TEXT')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'TEXT')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_multiple_json_objects_single_record(sdc_builder, sdc_executor, cluster):
    """Write json objects messages into Kafka and confirm that Kafka successfully reads them.
    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = {'Alex': 'Developer', 'Xavi': 'Developer'}
    expected = '{\'Alex\': Developer, \'Xavi\': Developer}'

    json_test_basic_structure(sdc_builder, sdc_executor, cluster, message, expected)


@cluster('cdh', 'kafka')
def test_kafka_origin_multiple_json_objects_multiple_records(sdc_builder, sdc_executor, cluster):
    """Write json objects messages into Kafka and confirm that Kafka successfully reads them.
    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = [{'Alex': 'Developer'}, {'Xavi': 'Developer'}]
    expected = '[{\'Alex\': Developer}, {\'Xavi\': Developer}]'

    json_test_basic_structure(sdc_builder, sdc_executor, cluster, message, expected)


@cluster('cdh', 'kafka')
def test_kafka_origin_json_array(sdc_builder, sdc_executor, cluster):
    """Write json array messages into Kafka and confirm that Kafka successfully reads them.
    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = ['Alex', 'Xavi']
    expected = '[Alex, Xavi]'

    json_test_basic_structure(sdc_builder, sdc_executor, cluster, message, expected)


def json_test_basic_structure(sdc_builder, sdc_executor, cluster, message, expected):
    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='JSON')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Origin JSON pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, json.dumps(message).encode(), 'JSON')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'JSON')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_xml_record(sdc_builder, sdc_executor, cluster):
    """Write xml messages into Kafka and confirm that Kafka successfully reads them.
    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = '<developers><developer>Alex</developer><developer>Xavi</developer></developers>'
    expected = '{\'developer\': [{\'value\': Alex}, {\'value\': Xavi}]}'

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='XML')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Origin XML pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, message.encode(), 'XML')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'XML')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_xml_record_delimiter_element(sdc_builder, sdc_executor, cluster):
    """Write xml messages into Kafka and confirm that Kafka successfully reads them. Delimiter element
    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = '<developers><developer>Alex</developer><developer>Xavi</developer></developers>'
    expected = ['{\'value\': Alex}', '{\'value\': Xavi}']

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='XML', delimiter_element='developer')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Origin XML pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, message.encode(), 'XML')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'XML_MULTI_ELEMENT')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_csv_record(sdc_builder, sdc_executor, cluster):
    """Write csv messages into Kafka and confirm that Kafka successfully reads them.
    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = 'Alex,Xavi,Tucu,Martin'
    expected = 'OrderedDict([(\'0\', Alex), (\'1\', Xavi), (\'2\', Tucu), (\'3\', Martin)])'

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='DELIMITED')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Origin CSV pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, message.encode(), 'CSV')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'CSV')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_produce_avro_records_with_schema(sdc_builder, sdc_executor, cluster):
    """Write avro text messages into Kafka standalone and confirm that Kafka successfully reads them.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline:
        kafka_consumer >>  trash
    """

    msg = {'name': 'boss', 'age': 60, 'emails': ['boss@company.com', 'boss2@company.com'], 'boss': None}
    expected = ('OrderedDict([(\'name\', boss), (\'age\', 60), (\'emails\', [boss@company.com, boss2@company.com]),'
                ' (\'boss\', None)])')

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='AVRO',
                                  avro_schema_location='INLINE',
                                  avro_schema=json.dumps(SCHEMA))

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash

    kafka_consumer_pipeline = builder.build(title='Kafka Origin AVRO Pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, msg, 'AVRO')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'AVRO')
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_produce_avro_records_without_schema(sdc_builder, sdc_executor, cluster):
    """Write avro text messages into Kafka standalone configuring the producer without a schema
    and confirm that Kafka successfully reads them.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline:
        kafka_consumer >>  trash
    """

    msg = {'name': 'boss', 'age': 60, 'emails': ['boss@company.com', 'boss2@company.com'], 'boss': None}
    expected = ('OrderedDict([(\'name\', boss), (\'age\', 60), (\'emails\', [boss@company.com, boss2@company.com]),'
                ' (\'boss\', None)])')

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='AVRO',
                                  avro_schema_location='SOURCE')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash

    kafka_consumer_pipeline = builder.build(title='Kafka Origin AVRO Pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, msg, 'AVRO_WITHOUT_SCHEMA')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'AVRO_WITHOUT_SCHEMA')
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_syslog_message(sdc_builder, sdc_executor, cluster):
    """Write a text message using UDP datagram mode SYSLOG.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline:
        kafka_consumer >>  trash
    """

    message = ("rO0ABXeOAAAAAQAAAAEAAAAAAAAAAQAJMTI3LjAuMC4xAAALuAAJMTI3LjAuMC4xAAAH0AAAAFw8MzQ+MSAyMDEzLTA2LTI4VDA2Oj"
               "E0OjU2LjAwMCswMjowMCBteW1hY2hpbmUgc3U6ICdzdSByb290JyBmYWlsZWQgZm9yIGxvbnZpY2sgb24gL2Rldi9wdHMvOA==")
    expected = (
        '{\'severity\': 2, \'senderPort\': 3000, \'receiverAddr\': 127.0.0.1:2000, \'host\': mymachine, \'raw\': '
        '<34>1 2013-06-28T06:14:56.000+02:00 mymachine su: \'su root\' failed for lonvick on /dev/pts/8, '
        '\'senderAddr\': 127.0.0.1:3000, \'priority\': 34, \'facility\': 4, \'version\': 1, \'receiverPort\': 2000, '
        '\'remaining\': su: \'su root\' failed for lonvick on /dev/pts/8, \'timestamp\': 1372392896000}')

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='DATAGRAM', datagram_packet_format='SYSLOG')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash

    kafka_consumer_pipeline = builder.build(
        title='Kafka Origin UDP-Datagram-Syslog Pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, base64.b64decode(message), 'SYSLOG')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'SYSLOG')
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_binary_record(sdc_builder, sdc_executor, cluster):
    """Write binary messages into Kafka and confirm that Kafka successfully reads them.
    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = 'Binary Text Example'
    expected = message.encode()

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='BINARY')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Origin BINARY pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, message.encode(), 'BINARY')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'BINARY')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_netflow_message(sdc_builder, sdc_executor, cluster):
    """Write a text message using UDP datagram mode NETFLOW.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline:
        kafka_consumer >>  trash
    """

    msg64packet = ('rO0ABXoAAAIqAAAAAQAAAAIAAAAAAAAAAQAJMTI3LjAuMC4xAAALuAAJMTI3LjAuMC4xAAAH0AAAAfgABQAKAAAAAFVFcOIBWL'
                   'IwAAAAAAAAAAD3waSb49Wa8QAAAAAAAAAAAAAAAQAAAFlnyqItZ8qiLQA1JA8AABEAAAAAAAAAAAD3waSb49Wa8QAAAAAAAAAA'
                   'AAAAAQAAAFlnyqItZ8qiLQA1+ioAABEAAAAAAAAAAAD3waSb49Wa8QAAAAAAAAAAAAAAAQAAAFlnyqItZ8qiLQA1SWAAABEAAA'
                   'AAAAAAAAD55boV49Wa8QAAAAAAAAAAAAAAAQAAAFlnyqIvZ8qiLwA1q94AABEAAAAAAAAAAAB/472549Wa8QAAAAAAAAAAAAAA'
                   'AQAAAFlnyqIvZ8qiLwA1IlYAABEAAAAAAAAAAAB/472549Wa8QAAAAAAAAAAAAAAAQAAAFlnyqIvZ8qiLwA1l5sAABEAAAAAAA'
                   'AAAAB/472549Wa8QAAAAAAAAAAAAAAAQAAAFlnyqIvZ8qiLwA1u4EAABEAAAAAAAAAAAD55boV49Wa8QAAAAAAAAAAAAAAAQAA'
                   'AFlnyqIvZ8qiLwA14OQAABEAAAAAAAAAAAAtZyl349Wa8QAAAAAAAAAAAAAAAQAAArhnyqIxZ8qiMQA11FQAABEAAAAAAAAAAA'
                   'B5SzUv49Wa8QAAAAAAAAAAAAAAAQAAAfhnyqIyZ8qiMgA1FbUAABEAAAAAAAAAAAA=')

    expected = ['\'srcaddr\': -138304357', '\'first\': 1432355575064']

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='DATAGRAM', datagram_data_format='NETFLOW')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash

    kafka_consumer_pipeline = builder.build(
        title='Kafka Origin UDP-Datagram-Netflow Pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, base64.b64decode(msg64packet), 'NETFLOW')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'NETFLOW')
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_collectd_message(sdc_builder, sdc_executor, cluster):
    """Write a text message using UDP datagram mode COLLECTD.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline:
        kafka_consumer >>  trash
    """

    msg64packet = (
        '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')

    expected = (
        '{\'plugin_instance\': lo0, \'plugin\': interface, \'tx\': 0, \'rx\': 0, \'host\': ip-192-168-42-238.us-west-2.'
        'compute.internal, \'time_hires\': 1543518938371396391, \'type\': if_errors}')

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='DATAGRAM', datagram_data_format='COLLECTD')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash

    kafka_consumer_pipeline = builder.build(
        title='Kafka Origin UDP-Datagram-COLLECTD Pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, base64.b64decode(msg64packet), 'COLLECTD')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'COLLECTD')
    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh', 'kafka')
def test_kafka_origin_log_record(sdc_builder, sdc_executor, cluster):
    """Write log messages into Kafka and confirm that Kafka successfully reads them.
    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = ('+20150320 [15:53:31,161] DEBUG PipelineConfigurationValidator - Pipeline \'test:preview\' validation. '
               'valid=true, canPreview=true, issuesCount=0 - ')

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='LOG',
                                  log_format='LOG4J',
                                  retain_original_line=True,
                                  on_parse_error='INCLUDE_AS_STACK_TRACE')

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Origin LOG pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages(kafka_consumer.topic, cluster, message.encode(), 'LOG')
        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, message, 'LOG')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


def produce_kafka_messages_protobuf(topic, sdc_builder, sdc_executor, cluster, message):
    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = message

    kafka_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.topic = topic
    kafka_destination.set_attributes(data_format='PROTOBUF', message_type='Contact',
                                     protobuf_descriptor_file=PROTOBUF_FILE_PATH, delimited_messages=False)

    dev_raw_data_source >> kafka_destination
    kafka_destination_pipeline = builder.build(
        title='Kafka Origin PROTOBUF pipeline(Producer)').configure_for_environment(cluster)

    kafka_destination_pipeline.configuration['rateLimit'] = 1

    sdc_executor.add_pipeline(kafka_destination_pipeline)
    sdc_executor.start_pipeline(kafka_destination_pipeline).wait_for_pipeline_batch_count(10)

    sdc_executor.stop_pipeline(kafka_destination_pipeline)
    history = sdc_executor.get_pipeline_history(kafka_destination_pipeline)
    msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    assert msgs_sent_count != 0


@cluster('cdh', 'kafka')
def test_kafka_origin_protobuf_record(sdc_builder, sdc_executor, cluster):
    """Write protobuf messages into Kafka and confirm that Kafka successfully reads them.

    For this test, ./resources/protobuf/addressbook.desc needs to be placed in
    $SDC_RESOURCES/resources/protobuf directory.

    Kafka Consumer Origin pipeline with standalone mode:
        kafka_consumer >> trash
    """

    message = '{"first_name": "Martin","last_name": "Balzamo"}'
    expected = '(\'first_name\', Martin), (\'last_name\', Balzamo)'

    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()

    kafka_consumer = get_kafka_consumer_stage(builder, cluster)

    # Override default configuration.
    kafka_consumer.set_attributes(data_format='PROTOBUF', message_type='Contact',
                                  protobuf_descriptor_file=PROTOBUF_FILE_PATH, delimited_messages=True)

    trash = builder.add_stage(label='Trash')
    kafka_consumer >> trash
    kafka_consumer_pipeline = builder.build(title='Kafka Origin PROTOBUF pipeline').configure_for_environment(cluster)

    kafka_consumer_pipeline.configuration['shouldRetry'] = False
    kafka_consumer_pipeline.configuration['executionMode'] = 'STANDALONE'

    sdc_executor.add_pipeline(kafka_consumer_pipeline)

    try:
        # Publish messages to Kafka and verify using snapshot if the same messages are received.
        produce_kafka_messages_protobuf(kafka_consumer.topic, sdc_builder, sdc_executor, cluster, message)

        verify_kafka_origin_results(kafka_consumer_pipeline, sdc_executor, expected, 'PROTOBUF')

    finally:
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)
