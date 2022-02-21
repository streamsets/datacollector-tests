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

import logging
import string
import json
import requests
import pytest
import io
import avro.schema
import avro.io

from pulsar import MessageId
from streamsets.testframework.markers import pulsar, sdc_min_version
from streamsets.testframework.utils import get_random_string
from streamsets import sdk

logger = logging.getLogger(__name__)


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_producer_with_no_schema(sdc_builder, sdc_executor, pulsar):
    """Test for Pulsar producer target stage. We do so by publishing data to a test topic using Pulsar producer
    stage and then read the data from that topic using Pulsar client. We assert the data from the client to what has
    been injected by the producer pipeline. The pipeline looks like:

    Pulsar Producer pipeline:
        dev_raw_data_source >> pulsar_producer
    """
    topic_name = get_random_string(string.ascii_letters, 10)
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=input_text)
    pulsar_producer = builder.add_stage('Pulsar Producer')
    pulsar_producer.set_attributes(topic=topic_name, data_format='TEXT')

    dev_raw_data_source >> pulsar_producer
    producer_dest_pipeline = builder.build(title='Pulsar Producer pipeline').configure_for_environment(pulsar)

    # add pipeline and capture pipeline messages to assert
    sdc_executor.add_pipeline(producer_dest_pipeline)
    sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_batch_count(10)
    sdc_executor.stop_pipeline(producer_dest_pipeline)

    history = sdc_executor.get_pipeline_history(producer_dest_pipeline)
    msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
    logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

    client = pulsar.client
    admin = pulsar.admin
    try:
        reader = client.create_reader(topic_name, MessageId.earliest)
        msgs_received = []
        while reader.has_message_available():
            msgs_received.append(reader.read_next().data().decode().strip()) # strip to remove newlines
    finally:
        reader.close()  # reader needs to be closed before topic can be deleted without force
        client.close()
        admin.delete_topic(reader.topic())
        sdc_executor.remove_pipeline(producer_dest_pipeline)

    logger.debug('Number of messages received from Pulsar = %d', len(msgs_received))
    assert msgs_received == [input_text] * msgs_sent_count


@pulsar
@sdc_min_version('4.5.0')
@pytest.mark.parametrize("data, topic_type_pipeline, topic_type_pulsar, error_code", [
    ("just a string", "STRING", "STRING", None),
    ("just a string", "STRING", "INT32", "PULSAR_21"), # Incompatible schemas
    ("true", "BOOLEAN", "BOOLEAN", "PULSAR_23"), # Not supported types for now
    ("100", "INT32", "INT32", "PULSAR_23"),
    ("100", "INT8", "INT8", "PULSAR_23"),
    ("100", "INT16", "INT16", "PULSAR_23"),
    ("100", "INT64", "INT64", "PULSAR_23"),
    ("100.0", "FLOAT", "FLOAT", "PULSAR_23"),
    ("100.0", "DOUBLE", "DOUBLE", "PULSAR_23"),
    ("100", "TIMESTAMP", "TIMESTAMP", "PULSAR_23"),
    #("just a string", "BYTES", "BYTES", None), # https://github.com/apache/pulsar/issues/10271
])
def test_pulsar_producer_with_primitive_schema(sdc_builder, sdc_executor, pulsar, data, topic_type_pipeline, topic_type_pulsar, error_code):
    topic_name = get_random_string(string.ascii_letters, 10)

    input_text = data
    topic_schema_pipeline = {"name": "primitive-string-schema", "type": topic_type_pipeline, "schema": "","properties": {}}
    topic_schema_pulsar = {"name": "primitive-string-schema", "type": topic_type_pulsar, "schema": "","properties": {}}

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=input_text)
    pulsar_producer = builder.add_stage('Pulsar Producer')
    pulsar_producer.set_attributes(topic=topic_name, 
                                   schema='USER_SCHEMA',
                                   schema_info=json.dumps(topic_schema_pipeline),
                                   data_format='TEXT',
                                   binary_field_path="/")

    dev_raw_data_source >> pulsar_producer
    pipeline = builder.build(title='Pulsar Producer pipeline').configure_for_environment(pulsar)
    _enforce_schema_validation_for_pulsar_topic(pulsar.admin, topic_name, topic_schema_pulsar)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(10)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

        client = pulsar.client
        try:
            reader = client.create_reader(topic_name, MessageId.earliest)
            msgs_received = []
            while reader.has_message_available():
                msgs_received.append(reader.read_next().data().decode().strip())
        finally:
            reader.close()
            client.close()

        assert error_code is None
        assert msgs_sent_count >= 10
        assert msgs_sent_count == len(msgs_received)
        assert msgs_received[0] == input_text
    except (sdk.sdc_api.RunError, sdk.sdc_api.StartError) as e:
        # StageException because in this case the pulsar schema is an stage level configuration
        assert error_code is not None
        assert error_code in e.message
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pulsar
@sdc_min_version('4.5.0')
@pytest.mark.parametrize("input_data_json, input_data_avro, topic_type_pipeline, topic_type_pulsar, error_code", [
    (
        {"name": "Fran", "age": 32}, 
        '\x08Fran@',
        '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}', 
        '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}', 
        None
    ),
    (
        {"name": "Fran", "age": 32}, 
        '\x08Fran@',
        '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}', 
        '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"age","type":"string"}]}', 
        'PULSAR_21'
    ),
    (
        {"name": "Fran", "age": "thirty-two"}, 
        '\x08Fran@',
        '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}', 
        '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}', 
        'PULSAR_24'
    ),
])
def test_pulsar_producer_with_avro_schema(sdc_builder, sdc_executor, pulsar, input_data_json, input_data_avro, topic_type_pipeline, topic_type_pulsar, error_code):
    topic_name = get_random_string(string.ascii_letters, 10)

    topic_schema_pipeline = {"name": "complex-schema", "type": "AVRO", "schema": topic_type_pipeline, "properties": {}}
    topic_schema_pulsar = {"name": "complex-schema", "type": "AVRO", "schema": topic_type_pulsar, "properties": {}}

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  json_content='MULTIPLE_OBJECTS',
                                                                                  raw_data=json.dumps(input_data_json))
    pulsar_producer = builder.add_stage('Pulsar Producer')
    pulsar_producer.set_attributes(topic=topic_name, 
                                   schema='USER_SCHEMA',
                                   schema_info=json.dumps(topic_schema_pipeline),
                                   data_format='AVRO',  # TODO All data format configuration shouldn't be needed
                                   avro_schema_location='INLINE',
                                   avro_schema=topic_type_pipeline,
                                   include_schema=False)

    dev_raw_data_source >> pulsar_producer
    pipeline = builder.build(title='Pulsar Producer pipeline').configure_for_environment(pulsar)
    _enforce_schema_validation_for_pulsar_topic(pulsar.admin, topic_name, topic_schema_pulsar)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(10)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        number_generated_records = history.latest.metrics.counter('stage.DevRawDataSource_01.outputRecords.counter').count
        logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

        client = pulsar.client
        try:
            reader = client.create_reader(topic_name, MessageId.earliest)
            msgs_received = []
            while reader.has_message_available():
                msgs_received.append(reader.read_next().data().decode().strip())
        finally:
            reader.close()  # reader needs to be closed before topic can be deleted without force
            client.close()

        assert error_code is None
        assert msgs_sent_count == number_generated_records
        assert msgs_sent_count == len(msgs_received)
        assert msgs_received[0] == input_data_avro
    except sdk.sdc_api.RunError as e:
        # StageException because in this case the pulsar schema is an stage level configuration
        assert error_code is not None
        assert error_code in e.message
    finally:
        sdc_executor.remove_pipeline(pipeline)

@pulsar
@sdc_min_version('4.5.0')
@pytest.mark.parametrize("input_data_json, input_data_avro, topic_type_pulsar, error_code", [
    (
        {"name": "Fran", "age": 32}, 
        '\x08Fran@',
        '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}', 
        None
    ),
    (
        {"name": "Fran", "age": "32"}, 
        '\x08Fran@',
        '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}', 
        'PULSAR_21'
    ),
])
def test_pulsar_producer_schema_avro_processor(sdc_builder, sdc_executor, pulsar, input_data_json, input_data_avro, topic_type_pulsar, error_code):
    topic_name = get_random_string(string.ascii_letters, 10)

    topic_schema_pulsar = {"name": "complex-schema", "type": "AVRO", "schema": topic_type_pulsar, "properties": {}}

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  json_content='MULTIPLE_OBJECTS',
                                                                                  raw_data=json.dumps(input_data_json))
    schema_processor = builder.add_stage('Schema Generator').set_attributes(schema_name='schema')
    pulsar_producer = builder.add_stage('Pulsar Producer')
    pulsar_producer.set_attributes(topic=topic_name, 
                                   schema='RECORD_SCHEMA',
                                   data_format='AVRO',
                                   avro_schema_location='HEADER',
                                   include_schema=False)

    dev_raw_data_source >> schema_processor >> pulsar_producer

    pipeline = builder.build(title='Pulsar Producer pipeline').configure_for_environment(pulsar)
    _enforce_schema_validation_for_pulsar_topic(pulsar.admin, topic_name, topic_schema_pulsar)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(10)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

        client = pulsar.client
        try:
            reader = client.create_reader(topic_name, MessageId.earliest)
            msgs_received = []
            while reader.has_message_available():
                msgs_received.append(reader.read_next().data().decode().strip())
        finally:
            reader.close()  # reader needs to be closed before topic can be deleted without force
            client.close()

        number_generated_recors = history.latest.metrics.counter('stage.SchemaGenerator_01.outputRecords.counter').count
        if error_code is not None:
            # In this case if there is any schema related error, are record errors
            # Because the schema is at record level
            assert history.latest.metrics.counter('stage.PulsarProducer_01.errorRecords.counter').count == number_generated_recors
            assert msgs_sent_count == 0
        else:
            assert error_code is None
            assert msgs_sent_count == number_generated_recors
            assert msgs_sent_count == len(msgs_received)
            assert msgs_received[0] == input_data_avro
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pulsar
@sdc_min_version('4.5.0')
@pytest.mark.parametrize("input_data_json, input_data_avro, topic_type_pulsar, error_code", [
    (
        {"name": "Fran", "age": 32}, 
        '\x08Fran@',
        '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}', 
        None
    ),
])
def test_pulsar_producer_schema_auto_schema(sdc_builder, sdc_executor, pulsar, input_data_json, input_data_avro, topic_type_pulsar, error_code):
    topic_name = get_random_string(string.ascii_letters, 10)

    topic_schema_pulsar = {"name": "complex-schema", "type": "AVRO", "schema": topic_type_pulsar, "properties": {}}

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  json_content='MULTIPLE_OBJECTS',
                                                                                  raw_data=json.dumps(input_data_json))
    schema_processor = builder.add_stage('Schema Generator').set_attributes(schema_name='schema')
    pulsar_producer = builder.add_stage('Pulsar Producer')
    pulsar_producer.set_attributes(topic=topic_name, 
                                   schema='AUTO_SCHEMA',
                                   data_format='AVRO',
                                   avro_schema_location='HEADER',
                                   include_schema=False)

    dev_raw_data_source >> schema_processor >> pulsar_producer

    pipeline = builder.build(title='Pulsar Producer pipeline').configure_for_environment(pulsar)
    _enforce_schema_validation_for_pulsar_topic(pulsar.admin, topic_name, topic_schema_pulsar)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(10)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        number_generated_records = history.latest.metrics.counter('stage.DevRawDataSource_01.outputRecords.counter').count
        logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

        client = pulsar.client
        try:
            reader = client.create_reader(topic_name, MessageId.earliest)
            msgs_received = []
            while reader.has_message_available():
                msgs_received.append(reader.read_next().data().decode().strip())
        finally:
            reader.close()  # reader needs to be closed before topic can be deleted without force
            client.close()

        assert error_code is None
        assert msgs_sent_count == number_generated_records
        assert msgs_sent_count == len(msgs_received)
        assert msgs_received[0] == input_data_avro
    except sdk.sdc_api.RunError as e:
        # StageException because in this case the pulsar schema is an stage level configuration
        assert error_code is not None
        assert error_code in e.message
    finally:
        sdc_executor.remove_pipeline(pipeline)

@pulsar
@sdc_min_version('4.5.0')
def test_pulsar_producer_schema_topic_in_record(sdc_builder, sdc_executor, pulsar):
    # Creating the topics with schema
    topic1_avro_schema = '{"type":"record","name":"schema","doc":"","fields":[{"name":"name","type":"string"},{"name":"topic","type":"string"}]}'
    topic2_avro_schema = '{"type":"record","name":"schema","doc":"","fields":[{"name":"number","type":"int"},{"name":"topic","type":"string"}]}' 
    topic1_schema_pulsar = {"name": "complex-schema", "type": "AVRO", "schema": topic1_avro_schema, "properties": {}}
    topic2_schema_pulsar = {"name": "complex-schema", "type": "AVRO", "schema": topic2_avro_schema, "properties": {}}

    topic1_name = get_random_string(string.ascii_letters, 10)
    topic2_name = get_random_string(string.ascii_letters, 10)

    _enforce_schema_validation_for_pulsar_topic(pulsar.admin, topic1_name, topic1_schema_pulsar)
    _enforce_schema_validation_for_pulsar_topic(pulsar.admin, topic2_name, topic2_schema_pulsar)

    # Creating the testing data
    records_data_json = [
        {"name": "Anthony", "topic": topic1_name},
        {"number": 100, "topic": topic2_name},
        {"name": "Brent", "topic": topic1_name},
        {"number": 150, "topic": topic2_name}
    ]
    records_raw_data = "\n".join([json.dumps(r) for r in records_data_json])
    records_data_avro_topic1 = [_json_to_avro(r, topic1_avro_schema) for r in records_data_json if r["topic"] == topic1_name]
    records_data_avro_topic2 = [_json_to_avro(r, topic2_avro_schema) for r in records_data_json if r["topic"] == topic2_name]

    # Creating the pipeline 
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       stop_after_first_batch=True,
                                       json_content='MULTIPLE_OBJECTS',
                                       raw_data=records_raw_data)
    schema_processor = builder.add_stage('Schema Generator').set_attributes(schema_name='schema')
    pulsar_producer = builder.add_stage('Pulsar Producer')
    pulsar_producer.set_attributes(topic='${record:value("/topic")}', 
                                   schema='RECORD_SCHEMA',
                                   data_format='AVRO',
                                   avro_schema_location='HEADER',
                                   include_schema=False)

    dev_raw_data_source >> schema_processor >> pulsar_producer

    pipeline = builder.build(title='Pulsar Producer pipeline').configure_for_environment(pulsar)
    sdc_executor.add_pipeline(pipeline)

    # Run the pipeline and assert all went well
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        client = pulsar.client
        try:
            for topic_name, expected_msgs in [(topic1_name, records_data_avro_topic1), (topic2_name, records_data_avro_topic2)]:
                reader = client.create_reader(topic_name, MessageId.earliest)
                msgs_received = []
                while reader.has_message_available():
                    msgs_received.append(reader.read_next().data())

                assert len(msgs_received) == len(expected_msgs)
                assert msgs_received == expected_msgs
        finally:
            reader.close()  # reader needs to be closed before topic can be deleted without force
            client.close()
    finally:
        sdc_executor.remove_pipeline(pipeline)

def _enforce_schema_validation_for_pulsar_topic(pulsar_admin_client, topic_name, topic_schema):
    try:
        pulsar_admin_client.put(f"persistent/public/default/{topic_name}")
        pulsar_admin_client.session.headers = {'Content-type': 'application/json'}
        pulsar_admin_client.post(f"schemas/public/default/{topic_name}/schema", data=topic_schema)
        pulsar_admin_client.post(f"namespaces/public/default/schemaValidationEnforced", data=True)
        pulsar_admin_client.post(f"namespaces/public/default/isAllowAutoUpdateSchema", data=False)
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error configuring the Pulsar environment")
        raise e


def _json_to_avro(json_data, avro_schema):
    bytes_writer = io.BytesIO()
    writer = avro.io.DatumWriter(avro.schema.Parse(avro_schema))
    writer.write(json_data, avro.io.BinaryEncoder(bytes_writer))
    return bytes_writer.getvalue()
