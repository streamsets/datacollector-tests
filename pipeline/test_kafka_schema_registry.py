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
def test_multi_header(sdc_builder, sdc_executor, topic, cluster, confluent):
    producer = producer_header(sdc_builder, topic, cluster, confluent)
    (wiretap, consumer) = consumer_multi(sdc_builder, topic, cluster, confluent)
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
def test_multi_registry(sdc_builder, sdc_executor, topic, cluster, confluent):
    # We need to register the schema before running the pipelines
    schema = avro.schema.parse(AVRO_SCHEMA)
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

