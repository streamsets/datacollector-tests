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
import string

import avro
import pytest
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.environments.kafka import KafkaCluster
from streamsets.testframework.markers import cluster, confluent, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def kafka_check(cluster):
    if isinstance(cluster, ClouderaManagerCluster) and not hasattr(cluster, 'kafka'):
        pytest.skip('Kafka tests require Kafka to be installed on the cluster')


# Kafka Destination
#

@cluster('cdh', 'kafka')
def test_kafka_destination_topic_resolution(sdc_builder, sdc_executor, cluster):
    """Test topic resolution in Kafka destination. We configure a pipeline which sends messages to the Kafka topic
    specified by the record field 'topic'. Then we check that the number of messages available in Kafka topics
    matches the total number of records processed by the pipeline, and that messages are delivered to the
    correct topic.

    Pipeline: dev_raw_data_source >> kafka_destination

    """
    # Input data.
    topic1 = f'stf_{get_random_string(string.ascii_letters, 10)}'
    topic2 = f'stf_{get_random_string(string.ascii_letters, 10)}'
    topic3 = f'stf_{get_random_string(string.ascii_letters, 10)}'
    text1 = get_random_string(string.ascii_letters, 10)
    text2 = get_random_string(string.ascii_letters, 10)
    text3 = get_random_string(string.ascii_letters, 10)
    raw_data = [{'topic': topic1, 'text': text1},
                {'topic': topic2, 'text': text2},
                {'topic': topic3, 'text': text3}]

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in raw_data))

    kafka_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.set_attributes(runtime_topic_resolution=True,
                                     topic_expression='${record:value("/topic")}',
                                     data_format='TEXT',
                                     text_field_path='/text')

    dev_raw_data_source >> kafka_destination
    pipeline = builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    # Setup Kafka consumers, one for each topic. Specify timeout so that iteration of consumer is stopped
    # after that time and specify auto_offset_reset to get messages from beginning.
    consumer1 = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer2 = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer3 = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer1.subscribe([topic1])
    consumer2.subscribe([topic2])
    consumer3.subscribe([topic3])

    # Run pipeline.
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(10)
    sdc_executor.stop_pipeline(pipeline)

    # Check each topic receives the expected messages and number of records matches total of messages
    # consumed by Kafka clients.
    history = sdc_executor.get_pipeline_history(pipeline)
    record_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
    messages1 = [msg.value.decode().strip() for msg in consumer1]
    messages2 = [msg.value.decode().strip() for msg in consumer2]
    messages3 = [msg.value.decode().strip() for msg in consumer3]

    assert all(msg == text1 for msg in messages1)
    assert all(msg == text2 for msg in messages2)
    assert all(msg == text3 for msg in messages3)
    assert record_count == len(messages1) + len(messages2) + len(messages3)


@cluster('cdh', 'kafka')
@sdc_min_version('3.0.0.0')
def test_kafka_destination_expression_partitioner(sdc_builder, sdc_executor, cluster):
    """This test generates three message all targeting the same partition in Kafka. We verify that all records indeed
        were stored in the same partition.
    """
    topic = get_random_string(string.ascii_letters, 10)
    logger.debug('Kafka topic name: %s', topic)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = '{"part" : 0}\n{"part" : 0}\n{"part" : 0}'

    destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                    library=cluster.kafka.standalone_stage_lib)
    destination.topic = topic
    destination.data_format = 'JSON'
    destination.partition_strategy = 'EXPRESSION'
    destination.partition_expression = "${record:value('/part')}"

    source >> destination
    pipeline = builder.build(title='Kafka Destination pipeline with Expression Partitioner').configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([topic])

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    msgs_received = [message for message in consumer]
    assert 3 == len(msgs_received)
    assert msgs_received[0].partition == msgs_received[1].partition
    assert msgs_received[0].partition == msgs_received[2].partition


@cluster('cdh', 'kafka')
@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('expr', ['666', "${record:value('/') % 3}"])
def test_kafka_destination_invalid_partition_expression(expr, sdc_builder, sdc_executor, cluster):
    """Validate that records with invalid partition expression(s) will end up in error stream."""
    topic = get_random_string(string.ascii_letters, 10)
    logger.debug('Kafka topic name: %s', topic)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'TEXT'
    source.raw_data = 'random text'

    destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                    library=cluster.kafka.standalone_stage_lib)
    destination.topic = topic
    destination.data_format = 'TEXT'
    destination.partition_strategy = 'EXPRESSION'
    destination.partition_expression = expr

    source >> destination
    pipeline = builder.build(title='Kafka Destination Invalid Partition Expression').configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([topic])

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # There should be no messages in Kafka
    msgs_received = [message for message in consumer]
    assert 0 == len(msgs_received)

    # And that one record should have ended up in error stream
    history = sdc_executor.get_pipeline_history(pipeline)
    assert 1 == history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count


@cluster('cdh', 'kafka')
@sdc_min_version('3.0.0.0')
def test_kafka_destination_xml(sdc_builder, sdc_executor, cluster):
    """Text XML format with Kafka destination."""
    topic = get_random_string(string.ascii_letters, 10)
    logger.debug('Kafka topic name: %s', topic)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = '{"key" : "value"}'

    destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                    library=cluster.kafka.standalone_stage_lib)
    destination.topic = topic
    destination.data_format = 'XML'

    source >> destination
    pipeline = builder.build(title='Kafka Destination XML').configure_for_environment(cluster)
    pipeline.configuration['rateLimit'] = 1

    sdc_executor.add_pipeline(pipeline)

    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([topic])

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # There should be no messages in Kafka
    msgs_received = [message for message in consumer]
    assert 1 == len(msgs_received)
    assert '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<key>value</key>\n' == msgs_received[0].value.decode()


#
# Schema Registry related tests
#

@cluster('cdh', 'kafka')
@confluent
@sdc_min_version('3.1.0.0')
def test_register_schema_from_pipeline_config(sdc_builder, sdc_executor, cluster, confluent):
    """Ensure that schema specified inside the pipeline configuration will be properly registered."""
    topic = get_random_string(string.ascii_letters, 10)
    logger.debug('Kafka topic name: %s', topic)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"a": 1, "b": "Text"}'
    dev_raw_data_source.stop_after_first_batch = True

    kafka_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.topic = topic
    kafka_destination.data_format = 'AVRO'
    kafka_destination.avro_schema_location = 'INLINE'
    kafka_destination.avro_schema = ('{"type":"record","name":"Brno","doc":"","fields":[{"name":"a","type":"int"},'
                                     '{"name":"b","type":"string"}]}')
    kafka_destination.register_schema = True
    kafka_destination.schema_subject = topic

    dev_raw_data_source >> kafka_destination
    kafka_destination_pipeline = (builder.build(title='Schema Registry: Register from Pipeline Config')
                                         .configure_for_environment(cluster, confluent))

    # Run the single batch pipeline
    sdc_executor.add_pipeline(kafka_destination_pipeline)
    sdc_executor.start_pipeline(kafka_destination_pipeline).wait_for_finished()

    # Validate that schema was properly registered
    validate_schema_was_registered(topic, confluent)


@cluster('cdh', 'kafka')
@confluent
@sdc_min_version('3.1.0.0')
def test_register_schema_from_header(sdc_builder, sdc_executor, cluster, confluent):
    """Ensure that schema specified inside record header will be properly registered."""
    topic = get_random_string(string.ascii_letters, 10)
    logger.debug('Kafka topic name: %s', topic)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"a": 1, "b": "Text"}'
    dev_raw_data_source.stop_after_first_batch = True

    # Generate schema for that record
    schema_generator = builder.add_stage('Schema Generator')
    schema_generator.schema_name = 'Brno'

    kafka_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.topic = topic
    kafka_destination.data_format = 'AVRO'
    kafka_destination.avro_schema_location = 'HEADER'
    kafka_destination.register_schema = True
    kafka_destination.schema_subject = topic

    dev_raw_data_source >> schema_generator >> kafka_destination
    kafka_destination_pipeline = (builder.build(title='Schema Registry: Register from Record Header')
                                         .configure_for_environment(cluster, confluent))

    # Run the single batch pipeline
    sdc_executor.add_pipeline(kafka_destination_pipeline)
    sdc_executor.start_pipeline(kafka_destination_pipeline).wait_for_finished()

    # Validate that schema was properly registered
    validate_schema_was_registered(topic, confluent)


@cluster('cdh', 'kafka')
def test_kafka_write_string_records_round_robin(sdc_builder, sdc_executor, cluster):
    """Send three text messages into Kafka Destination from Dev Raw Data Source and
       confirm that Kafka successfully reads them using KafkaConsumer from cluster.
       Specifically, this would look like:

       Kafka Destination pipeline:
           dev_raw_data_source >> kafka_destination

    """

    topic = get_random_string(string.ascii_letters, 10)
    logger.debug('Kafka topic name: %s', topic)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    data = "\n".join(['Xavi','Ale','Tucu'])

    # Add Dev Raw Data Source stage to pipeline.
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=data)

    kafka_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.topic = topic
    kafka_destination.data_format = 'TEXT'
    kafka_destination.set_attributes(partition_strategy ='ROUND_ROBIN',
                                     batch_wait_time_in_ms=20000,
                                     topic=topic)

    dev_raw_data_source >> kafka_destination

    kafka_destination_pipeline = builder.build(title='Kafka Destination pipeline').configure_for_environment(cluster)
    kafka_destination_pipeline.configuration['rateLimit'] = 1

    sdc_executor.add_pipeline(kafka_destination_pipeline)

    # Specify timeout to stop consumer iteration after that time and auto_offset_reset to get messages from beginning.
    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([topic])

    try:

        # Send messages using pipeline to Kafka Destination.
        logger.debug('Starting Kafka Destination pipeline and waiting for it to produce 10 records ...')
        sdc_executor.start_pipeline(kafka_destination_pipeline).wait_for_pipeline_batch_count(10)

        logger.debug('Stopping Kafka Destination pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(kafka_destination_pipeline)

        history = sdc_executor.get_pipeline_history(kafka_destination_pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.debug('No. of messages sent in the pipeline = %s', msgs_sent_count)

        msgs_received = [message.value.decode().strip() for message in consumer]
        logger.debug('No. of messages received in Kafka Consumer = %d', (len(msgs_received)))

        logger.debug('Verifying messages with Kafka consumer client ...')
        assert msgs_sent_count == len(msgs_received)

        # Check that the received messages are the same that the send ones.
        assert msgs_received == ((dev_raw_data_source.raw_data.split("\n")) *
                                 (msgs_sent_count // len(dev_raw_data_source.raw_data.split("\n"))))

    finally:
        # Remove pipeline
        sdc_executor.remove_pipeline(kafka_destination_pipeline)

#
# Kafka Error Destination
#

@cluster('cdh', 'kafka')
@sdc_min_version('3.0.0.0')
def test_kafka_error_destination(sdc_builder, sdc_executor, cluster):
    """Validate 'To Error' Kafka destination."""
    topic = get_random_string(string.ascii_letters, 10)
    logger.debug('Kafka topic name: %s', topic)

    builder = sdc_builder.get_pipeline_builder()
    error = builder.add_error_stage('Write to Kafka')
    error.topic = topic
    error.broker_uri = (f'{cluster.kafka.brokers[0]}' if isinstance(cluster, KafkaCluster)
                        else f'{cluster.server_host}:{cluster.kafka.broker_port}')

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'TEXT'
    source.raw_data = 'Hello World!'
    source.stop_after_first_batch = True

    to_error = builder.add_stage('To Error')

    source >> to_error

    pipeline = builder.build('To Error Kafka').configure_for_environment(cluster)

    sdc_executor.add_pipeline(pipeline)

    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([topic])

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    msgs_received = [message for message in consumer]
    assert 1 == len(msgs_received)


def validate_schema_was_registered(name, confluent):
    # Validate that schema has been registered
    registered = confluent.schema_registry.get_latest_schema(name)
    assert registered is not None
    assert 1 == registered[2]

    schema = registered[1]
    assert schema is not None
    assert schema.avro_name == avro.schema.Name('Brno')
    assert len(schema.fields) == 2
    assert schema.fields[0].name == 'a'
    assert schema.fields[0].type.name == 'int'
    assert schema.fields[1].name == 'b'
    assert schema.fields[1].type.name == 'string'


@cluster('cdh', 'kafka')
def test_pipeline_retry_for_exceptions_with_on_error_record_action_stop_pipeline(sdc_builder, sdc_executor, cluster):
    """ STF test for SDC-8738.
        Pipeline configuration is set to retry on error.
        Kafka Producer Destination's onErrorRecord action is set to "STOP PIPELINE"
        Now, when the pipeline raises a stageException or starting exception,
        the pipeline should be restarted based on if retry is set or not.

        Previously, *the pipeline was stopped and never retried*,
        because the OnErrorRecord action was set to "STOP PIPELINE"
        i.e. the logic for errorRecordException was conflated with StageException and StartingException.
        But since, this is not a on error record exception,
        the new and *correct* behavior is to retry the pipeline based on retry logic.

        Now the behavior has been changed to make sure that for StageExceptions and other runtime Exceptions,
        the pipeline will be retried as per retry settings.

        Check in the history to see that the pipeline transitions from starting to start_error and then retries again

        Kafka Destination Origin pipeline:
           dev_raw_data_source >> kafka_destination

    """
    topic = get_random_string(string.ascii_letters, 10)
    logger.debug('Kafka topic name: %s', topic)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = 'Hello World!'

    destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=cluster.kafka.standalone_stage_lib)
    destination.topic = topic
    destination.data_format = 'TEXT'
    # Set up some invalid compression type in Kafka Configuration,
    # so that the pipeline never starts, and throws a STARTING_ERROR instead
    destination.kafka_configuration = [{'key': 'compression.type', 'value': 'invalid.value'}]

    # Set the on record error handling to 'STOP PIPELINE'
    # Even with this setting, the expected outcome is that the pipeline should retry on failure,
    # since the failure here will be a STARTING_ERROR and not a on record error
    destination.on_record_error = 'STOP_PIPELINE'

    dev_raw_data_source >> destination

    pipeline = builder.build().configure_for_environment(cluster)

    # This is important, since without this the pipeline wouldn't retry
    pipeline.configuration['shouldRetry'] = True

    sdc_executor.add_pipeline(pipeline)

    try:
        logger.debug('Starting Pipeline')
        sdc_executor.start_pipeline(pipeline=pipeline, wait_for_statuses=['RETRY', 'START_ERROR'])

        history = sdc_executor.get_pipeline_history(pipeline)
        # The pipeline could have potentially moved on to STARTING state again.
        # So cannot check for RETRY == history.entries.latest
        # Check instead if RETRY is in the list of entries. That is validation enough
        assert 'RETRY' in [entry['status'] for entry in history.entries]
    finally:
        sdc_executor.stop_pipeline(pipeline)
