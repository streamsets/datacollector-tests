# Copyright 2017 StreamSets Inc.
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

# Kafka Schema Registry Exchange tests.
#
# This test ensures that end-to-end behavior of pushing avro data through Kafka
# with exchanging schema in Confluent Schema Registry works as expected for all
# various combinations the schema can be configured.
#
# Currently the permutation contains the following axes:
# * Two different Kafka origins (Single Threaded, Multi Threaded)
# * Three different schema locations on generation side (header, inline, registry)

import base64
import io
import logging
import string

import avro
import confluent_kafka.avro
import pytest
from avro.io import DatumReader, BinaryDecoder
from confluent_kafka.avro import AvroProducer
from streamsets.testframework.markers import cluster, confluent, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

AVRO_SCHEMA = '{"type":"record","name":"Brno","doc":"","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}'


@pytest.fixture(scope='function')
def topic():
    """Topic name used for this specific test."""
    topic = get_random_string(string.ascii_letters, 10)
    logger.info('Using Topic: %s', topic)
    return topic


def consumer_single(sdc_builder, topic, cluster, confluent):
    """Single threaded Kafka consumer configured to read schema from the registry."""
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    kafka_consumer = builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(topic=topic,
                                  consumer_group=get_random_string(),
                                  data_format='AVRO',
                                  avro_schema_location='REGISTRY',
                                  lookup_schema_by='AUTO',
                                  key_deserializer='CONFLUENT',
                                  value_deserializer='CONFLUENT',
                                  max_batch_size_in_records=10,
                                  kafka_configuration=[{'key': 'auto.offset.reset', 'value': 'earliest'}])

    wiretap = builder.add_wiretap()
    kafka_consumer >> wiretap.destination

    return (wiretap, builder.build().configure_for_environment(cluster, confluent))


def consumer_multi(sdc_builder, topic, cluster, confluent):
    """Multithreaded threaded Kafka consumer configured to read schema from the registry."""
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    kafka_consumer = builder.add_stage('Kafka Multitopic Consumer')
    kafka_consumer.set_attributes(topic_list=[topic],
                                  consumer_group=get_random_string(),
                                  data_format='AVRO',
                                  avro_schema_location='REGISTRY',
                                  lookup_schema_by='AUTO',
                                  key_deserializer='CONFLUENT',
                                  max_batch_size_in_records=10,
                                  value_deserializer='CONFLUENT',
                                  configuration_properties=[{'key': 'auto.offset.reset', 'value': 'earliest'}])

    wiretap = builder.add_wiretap()
    kafka_consumer >> wiretap.destination

    return (wiretap, builder.build().configure_for_environment(cluster, confluent))


def producer_header(sdc_builder, topic, cluster, confluent):
    """Kafka producer that receives avro schema in record header."""
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"a": 1, "b": "Text"}',
                                       stop_after_first_batch=True)

    schema_generator = builder.add_stage('Schema Generator')
    schema_generator.schema_name = 'Brno'

    kafka_destination = builder.add_stage('Kafka Producer',
                                          library=cluster.kafka.standalone_stage_lib)

    kafka_destination.set_attributes(topic=topic,
                                     data_format='AVRO',
                                     avro_schema_location='HEADER',
                                     include_schema=False,
                                     register_schema=True,
                                     schema_subject=topic,
                                     key_serializer='CONFLUENT',
                                     value_serializer='CONFLUENT')

    dev_raw_data_source >> schema_generator >> kafka_destination
    return builder.build().configure_for_environment(cluster, confluent)


def producer_inline(sdc_builder, topic, cluster, confluent):
    """Kafka producer that receives avro schema the pipeline configuration."""
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"a": 1, "b": "Text"}',
                                       stop_after_first_batch=True)

    kafka_destination = builder.add_stage('Kafka Producer',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.set_attributes(topic=topic,
                                     data_format='AVRO',
                                     avro_schema_location='INLINE',
                                     avro_schema=AVRO_SCHEMA,
                                     include_schema=False,
                                     register_schema=True,
                                     schema_subject=topic,
                                     key_serializer='CONFLUENT',
                                     value_serializer='CONFLUENT')

    dev_raw_data_source >> kafka_destination
    return builder.build().configure_for_environment(cluster, confluent)


def producer_registry(sdc_builder, topic, cluster, confluent):
    """Kafka producer that receives avro schema from schema registry (must exists before pipeline run)."""
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"a": 1, "b": "Text"}',
                                       stop_after_first_batch=True)

    kafka_destination = builder.add_stage('Kafka Producer',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.set_attributes(topic=topic,
                                     data_format='AVRO',
                                     avro_schema_location='REGISTRY',
                                     include_schema=False,
                                     schema_subject=topic,
                                     key_serializer='CONFLUENT',
                                     value_serializer='CONFLUENT')

    dev_raw_data_source >> kafka_destination
    return builder.build().configure_for_environment(cluster, confluent)


@cluster('cdh', 'kafka')
@confluent
@sdc_min_version('3.1.0.0')
def test_single_header(sdc_builder, sdc_executor, topic, cluster, confluent):
    producer = producer_header(sdc_builder, topic, cluster, confluent)
    (wiretap, consumer) = consumer_single(sdc_builder, topic, cluster, confluent)
    perform_test(sdc_executor, producer, consumer, wiretap)


@cluster('cdh', 'kafka')
@confluent
@sdc_min_version('3.1.0.0')
def test_multi_header(sdc_builder, sdc_executor, topic, cluster, confluent):
    producer = producer_header(sdc_builder, topic, cluster, confluent)
    (wiretap, consumer) = consumer_multi(sdc_builder, topic, cluster, confluent)
    perform_test(sdc_executor, producer, consumer, wiretap)


@cluster('cdh', 'kafka')
@confluent
@sdc_min_version('3.1.0.0')
def test_single_inline(sdc_builder, sdc_executor, topic, cluster, confluent):
    producer = producer_inline(sdc_builder, topic, cluster, confluent)
    (wiretap, consumer) = consumer_single(sdc_builder, topic, cluster, confluent)
    perform_test(sdc_executor, producer, consumer, wiretap)


@cluster('cdh', 'kafka')
@confluent
@sdc_min_version('3.1.0.0')
def test_multi_inline(sdc_builder, sdc_executor, topic, cluster, confluent):
    producer = producer_inline(sdc_builder, topic, cluster, confluent)
    (wiretap, consumer) = consumer_multi(sdc_builder, topic, cluster, confluent)
    perform_test(sdc_executor, producer, consumer, wiretap)


@cluster('cdh', 'kafka')
@confluent
@sdc_min_version('3.1.0.0')
def test_single_registry(sdc_builder, sdc_executor, topic, cluster, confluent):
    # We need to register the schema before running the pipelines
    schema = avro.schema.Parse(AVRO_SCHEMA)
    confluent.schema_registry.register(topic, schema)

    producer = producer_registry(sdc_builder, topic, cluster, confluent)
    (wiretap, consumer) = consumer_single(sdc_builder, topic, cluster, confluent)

    perform_test(sdc_executor, producer, consumer, wiretap)


@cluster('cdh', 'kafka')
@confluent
@sdc_min_version('3.1.0.0')
def test_multi_registry(sdc_builder, sdc_executor, topic, cluster, confluent):
    # We need to register the schema before running the pipelines
    schema = avro.schema.Parse(AVRO_SCHEMA)
    confluent.schema_registry.register(topic, schema)

    producer = producer_registry(sdc_builder, topic, cluster, confluent)
    (wiretap, consumer) = consumer_multi(sdc_builder, topic, cluster, confluent)

    perform_test(sdc_executor, producer, consumer, wiretap)


def perform_test(sdc_executor, producer, consumer, wiretap):
    """Run the producer -> consumer pipeline and validate that we can properly read all the records."""
    # Add all pipelines
    sdc_executor.add_pipeline(producer, consumer)

    # Run them!
    sdc_executor.start_pipeline(producer).wait_for_finished()
    sdc_executor.start_pipeline(consumer)
    sdc_executor.wait_for_pipeline_metric(consumer, 'input_record_count', 1)

    # Validate result
    output = wiretap.output_records

    assert output is not None
    assert len(output) == 1
    assert output[0].field['a'].value == 1
    assert output[0].field['b'].value == 'Text'


@pytest.mark.parametrize('key_capture_mode', ['RECORD_FIELD', 'RECORD_HEADER', 'RECORD_HEADER_AND_FIELD'])
@cluster('kafka')
@confluent
@sdc_min_version('3.11.0')
def test_kafka_consumer_key_capture_modes(sdc_builder, sdc_executor, cluster, confluent, topic, key_capture_mode):
    """
    Test the KeyCaptureMode introduced in SDC-4813

    We will first publish an Avro message (with separate key and value schema) to Kafka, then try to read it from a
    pipeline, and ensure the key is captured correctly as per configuration.
    """
    if 'streamsets-datacollector-apache-kafka_0_9-lib' in cluster.sdc_stage_libs:
        pytest.skip('Test only designed to run on Kafka version >= 0.10.0')

    broker_url = cluster.kafka.brokers[0].replace("kafka://", "")

    avro_producer = AvroProducer({
        'bootstrap.servers': broker_url,
        'schema.registry.url': confluent._registry_urls[0]
    })

    value_schema_str = """
    {
       "namespace": "",
       "name": "valueType",
       "type": "record",
       "fields" : [
         {
           "name" : "f1",
           "type" : "string"
         }, {
           "name" : "f2",
           "type" : "float"
         }
       ]
    }
    """

    key_schema_str = """
    {
       "namespace": "",
       "name": "keyType",
       "type": "record",
       "fields" : [
         {
           "name" : "k1",
           "type" : "long"
         }, {
           "name" : "k2",
           "type" : "int"
         }
       ]
    }
    """

    value_schema = confluent_kafka.avro.loads(value_schema_str)
    key_schema = confluent_kafka.avro.loads(key_schema_str)

    k1_val = 17
    k2_val = 8
    f1_val = "foo"
    f2_val = 18.6

    key = {"k1": k1_val, "k2": k2_val}
    value = {"f1": f1_val, "f2": f2_val}

    # publish the Avro key/value message to Kafka
    avro_producer.produce(topic=topic, value=value, key=key, key_schema=key_schema, value_schema=value_schema)
    avro_producer.flush()

    # now, build a pipeline to read the message
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    key_capture_field = '/mySpecialKey'
    key_capture_attribute = 'mySpecialKey'

    kafka_consumer = builder.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer.set_attributes(topic=topic,
                                  consumer_group=get_random_string(),
                                  data_format='AVRO',
                                  avro_schema_location='REGISTRY',
                                  lookup_schema_by='AUTO',
                                  key_deserializer='CONFLUENT',
                                  value_deserializer='CONFLUENT',
                                  key_capture_mode=key_capture_mode,
                                  key_capture_field=key_capture_field,
                                  key_capture_header_attribute=key_capture_attribute,
                                  kafka_configuration=[{'key': 'auto.offset.reset', 'value': 'earliest'}])

    wiretap = builder.add_wiretap()
    kafka_consumer >> wiretap.destination

    pipeline = builder.build().configure_for_environment(cluster, confluent)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)

    output = wiretap.output_records

    assert output is not None
    assert len(output) == 1
    record = output[0]

    verify_kafka_avro_messages(record, key_capture_mode, key_capture_field, key_capture_attribute, key_schema, key,
                               value)


@pytest.mark.parametrize('key_capture_mode', ['RECORD_HEADER'])
@cluster('kafka')
@confluent
@sdc_min_version('3.11.0')
def test_kafka_producer_key_capture(sdc_builder, sdc_executor, cluster, confluent, topic, key_capture_mode):
    """
    Test the Kafka producer with avro key

    We will have 2 pipelines:
    1. kafka_consumer >> kafka_producer
    2. kafka_consumer >> wiretap

    In the first pipeline, we will first publish an Avro message (with separate key and value schema) to Kafka,
    then read it and ensure that the key is captured correctly as per configuration and then write the key and value
    back to kafka in a separate topic.
    In the second pipeline, we will read the avro key and value written by the first pipeline and assert if the key
    and value is correct
    """
    if ('streamsets-datacollector-apache-kafka_0_9-lib' in cluster.sdc_stage_libs):
        pytest.skip('Test only designed to run on Kafka version >= 0.10.0')

    broker_url = cluster.kafka.brokers[0].replace("kafka://", "")

    avro_producer = AvroProducer({
        'bootstrap.servers': broker_url,
        'schema.registry.url': confluent._registry_urls[0]
    })

    value_schema_str = """
    {
       "namespace": "",
       "name": "valueType",
       "type": "record",
       "fields" : [
         {
           "name" : "f1",
           "type" : "string"
         }, {
           "name" : "f2",
           "type" : "float"
         }
       ]
    }
    """

    key_schema_str = """
    {
       "namespace": "",
       "name": "keyType",
       "type": "record",
       "fields" : [
         {
           "name" : "k1",
           "type" : "long"
         }, {
           "name" : "k2",
           "type" : "int"
         }
       ]
    }
    """

    value_schema = confluent_kafka.avro.loads(value_schema_str)
    key_schema = confluent_kafka.avro.loads(key_schema_str)

    k1_val = 17
    k2_val = 8
    f1_val = "foo"
    f2_val = 18.6

    key = {"k1": k1_val, "k2": k2_val}
    value = {"f1": f1_val, "f2": f2_val}

    # publish the Avro key/value message to Kafka
    avro_producer.produce(topic=topic, value=value, key=key, key_schema=key_schema, value_schema=value_schema)
    avro_producer.flush()

    # now, build a pipeline to read the message
    builder1 = sdc_builder.get_pipeline_builder()
    builder1.add_error_stage('Discard')

    key_capture_attribute = 'kafkaMessageKey'

    kafka_consumer1 = builder1.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)
    kafka_consumer1.set_attributes(topic=topic,
                                   data_format='AVRO',
                                   avro_schema_location='REGISTRY',
                                   lookup_schema_by='AUTO',
                                   key_deserializer='CONFLUENT',
                                   value_deserializer='CONFLUENT',
                                   key_capture_mode=key_capture_mode,
                                   key_capture_header_attribute=key_capture_attribute,
                                   kafka_configuration=[{'key': 'auto.offset.reset', 'value': 'earliest'}])

    topic_producer = get_random_string(string.ascii_letters, 10)
    kafka_producer = builder1.add_stage('Kafka Producer', library=cluster.kafka.standalone_stage_lib)
    kafka_producer.set_attributes(topic=topic_producer,
                                  data_format='AVRO',
                                  avro_schema_location='REGISTRY',
                                  include_schema=False,
                                  register_schema=True,
                                  schema_subject=f'{topic}-value',
                                  message_key_format='AVRO',
                                  key_serializer='CONFLUENT',
                                  value_serializer='CONFLUENT')

    kafka_consumer1 >> kafka_producer

    pipeline1 = builder1.build().configure_for_environment(cluster, confluent)
    sdc_executor.add_pipeline(pipeline1)
    sdc_executor.start_pipeline(pipeline1)
    sdc_executor.stop_pipeline(pipeline1)

    # Build another pipeline to read the avro that was written into the topic(topic_producer)
    builder2 = sdc_builder.get_pipeline_builder()
    builder2.add_error_stage('Discard')
    kafka_consumer2 = builder2.add_stage('Kafka Consumer', library=cluster.kafka.standalone_stage_lib)

    # Kafka consumer to read the avro written by kafka_producer
    kafka_consumer2.set_attributes(topic=topic_producer,
                                   data_format='AVRO',
                                   avro_schema_location='REGISTRY',
                                   lookup_schema_by='AUTO',
                                   key_deserializer='CONFLUENT',
                                   value_deserializer='CONFLUENT',
                                   key_capture_mode=key_capture_mode,
                                   key_capture_header_attribute=key_capture_attribute,
                                   kafka_configuration=[{'key': 'auto.offset.reset', 'value': 'earliest'}])

    wiretap = builder2.add_wiretap()
    kafka_consumer2 >> wiretap.destination

    pipeline2 = builder2.build().configure_for_environment(cluster, confluent)

    sdc_executor.add_pipeline(pipeline2)
    sdc_executor.start_pipeline(pipeline2)
    sdc_executor.wait_for_pipeline_metric(pipeline2, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline2)

    output = wiretap.output_records

    assert output is not None
    assert len(output) == 1
    record = output[0]

    verify_kafka_avro_messages(record, key_capture_mode, None, key_capture_attribute, key_schema, key, value)


def verify_kafka_avro_messages(record, key_capture_mode, key_capture_field, key_capture_attribute, key_schema,
                               message_keys, message_values):
    for key, value in message_values.items():
        assert record.get_field_data(f'/{key}').value == value

    # validate message key fields/attribute based on configuration
    if key_capture_mode in ['RECORD_FIELD', 'RECORD_HEADER_AND_FIELD']:
        # the message key should have been captured into the configured field
        for key, value in message_keys.items():
            assert record.get_field_data(f"{key_capture_field}/{key}").value == value

    if key_capture_mode in ['RECORD_HEADER', 'RECORD_HEADER_AND_FIELD']:
        # get the base64 encoded Avro message key
        encoded = record.header['values'][key_capture_attribute]
        # decode into bytes
        key_bytes = base64.standard_b64decode(encoded)
        # create an Avro binary decoder based on those bytes
        decoder = BinaryDecoder(io.BytesIO(key_bytes))
        # parse the key schema out of the record header
        decoded_key_schema = confluent_kafka.avro.loads(record.header['values']['avroKeySchema'])
        # ensure the parsed key schema matches the one we actually produced, earlier
        assert decoded_key_schema == key_schema
        # create a DatumReader to read a full Avro record (the key)
        reader = DatumReader(decoded_key_schema)
        decoded_avro_key = reader.read(decoder)
        # assert the values from the Avro record match what's expected
        for key, value in message_keys.items():
            assert decoded_avro_key[key] == value
