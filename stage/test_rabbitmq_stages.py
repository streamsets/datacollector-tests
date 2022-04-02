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

import json
import logging
import string

import pika
import pytest
import time
from streamsets.testframework.markers import rabbitmq, sdc_min_version
from streamsets.testframework.utils import get_random_string
from streamsets.sdk.sdc_api import StartError

logger = logging.getLogger(__name__)


@rabbitmq
def test_rabbitmq_rabbitmq_consumer(sdc_builder, sdc_executor, rabbitmq):
    """Test for RabbitMQ consumer origin stage. We do so by publishing data to a test queue using RabbitMQ client and
    having a pipeline which reads that data using RabbitMQ consumer origin stage. Data is then asserted for what is
    published at RabbitMQ client and what we read in the pipeline wiretap. The pipeline looks like:

    RabbitMQ Consumer pipeline:
        rabbitmq_consumer >> wiretap
    """
    # Build consumer pipeline.
    name = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    # We set to use default exchange and hence exchange does not need to be pre-created or given.
    rabbitmq_consumer = builder.add_stage('RabbitMQ Consumer').set_attributes(name=name,
                                                                              data_format='TEXT',
                                                                              durable=True,
                                                                              auto_delete=False,
                                                                              bindings=[])
    wiretap = builder.add_wiretap()

    rabbitmq_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='RabbitMQ Consumer pipeline').configure_for_environment(rabbitmq)
    sdc_executor.add_pipeline(consumer_origin_pipeline)
    sdc_executor.start_pipeline(consumer_origin_pipeline)

    expected_messages = set()
    connection = rabbitmq.blocking_connection
    channel = connection.channel()

    # About default exchange routing: https://www.rabbitmq.com/tutorials/amqp-concepts.html
    channel.queue_declare(queue=name, durable=True, exclusive=False, auto_delete=False)
    channel.confirm_delivery()
    for i in range(10):
        expected_message = 'Message {0}'.format(i)
        expected_messages.add(expected_message)
        try:
            channel.basic_publish(exchange="",
                                  routing_key=name,  # Routing key has to be same as queue name.
                                  body=expected_message,
                                  properties=pika.BasicProperties(content_type='text/plain', delivery_mode=1),
                                  mandatory=True)
        except:
            logger.warning('Message %s could not be sent.', expected_message)

    channel.close()
    connection.close()

    # Messages are published, read through the pipeline and assert.
    sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(consumer_origin_pipeline)
    output_records = [record.field['text'].value
                      for record in wiretap.output_records]

    assert set(output_records) == expected_messages


@rabbitmq
def test_rabbitmq_producer_target(sdc_builder, sdc_executor, rabbitmq):
    """Test for RabbitMQ producer target stage. We do so by publishing data to a test queue using RabbitMQ producer
    stage and then read the data from that queue using RabbitMQ client. We assert the data from the client to what has
    been injected by the producer pipeline. The pipeline looks like:

    RabbitMQ Producer pipeline:
        dev_raw_data_source >> rabbitmq_producer
    """
    # build producer pipeline
    name = get_random_string(string.ascii_letters, 10)
    exchange_name = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=raw_str)

    rabbitmq_producer = builder.add_stage('RabbitMQ Producer')
    rabbitmq_producer.set_attributes(name=name, data_format='TEXT',
                                     durable=False, auto_delete=True,
                                     bindings=[dict(name=exchange_name,
                                                    type='DIRECT',
                                                    durable=False,
                                                    autoDelete=True)])

    dev_raw_data_source >> rabbitmq_producer
    producer_dest_pipeline = builder.build(title='RabbitMQ Producer pipeline').configure_for_environment(rabbitmq)
    producer_dest_pipeline.rate_limit = 1

    # add pipeline and capture pipeline messages to assert
    sdc_executor.add_pipeline(producer_dest_pipeline)
    sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_batch_count(10)
    sdc_executor.stop_pipeline(producer_dest_pipeline)

    history = sdc_executor.get_pipeline_history(producer_dest_pipeline)
    msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
    logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

    # read data from RabbitMQ to assert it is what got ingested into the pipeline
    connection = rabbitmq.blocking_connection
    channel = connection.channel()
    try:
        # Get one message at a time from RabbitMQ.
        # Returns a sequence with the method frame, message properties, and body.
        msgs_received = [channel.basic_get(name, False)[2].decode().replace('\n', '')
                         for _ in range(msgs_sent_count)]
    finally:
        channel.close()
        connection.close()

    logger.debug('Number of messages received from RabbitMQ = %d', (len(msgs_received)))

    assert msgs_received == [raw_str] * msgs_sent_count


@rabbitmq
@sdc_min_version('3.10.0')
@pytest.mark.parametrize('set_expiration', [True, False])
def test_rabbitmq_producer_msg_expiration(sdc_builder, sdc_executor, rabbitmq, set_expiration):
    """Test expiration time in the messages sent by RabbitMQ Producer.

    In SDC 3.10.0 the "Set Expiration" option is introduced, which allows SDC users to enable/disable the
    Expiration Time in the AMQP Message Properties. Prior to that version, users were forced to set an Expiration
    Time when AMQP Message Properties were actived. This test checks that messages will be expired within the
    configured milliseconds only when the "Set Expiration" is enabled.

    Pipeline:
        dev_raw_data_source >> rabbitmq_producer

    """
    queue_name = get_random_string(string.ascii_letters, 10)
    exchange_name = get_random_string(string.ascii_letters, 10)
    input_str = 'Hello World!'
    expiration_ms = 2000
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=input_str)

    rabbitmq_producer = builder.add_stage('RabbitMQ Producer')
    rabbitmq_producer.set_attributes(name=queue_name, data_format='TEXT',
                                     set_amqp_message_properties=True,
                                     set_expiration=set_expiration,
                                     expiration=expiration_ms,
                                     bindings=[dict(name=exchange_name,
                                                    type='DIRECT',
                                                    durable=False,
                                                    autoDelete=True)])

    dev_raw_data_source >> rabbitmq_producer
    pipeline = builder.build().configure_for_environment(rabbitmq)
    sdc_executor.add_pipeline(pipeline)

    # Set up RabbitMQ client to consume messages sent by SDC
    connection = rabbitmq.blocking_connection
    channel = connection.channel()

    try:
        # Send a message and consume it within `expiration_ms` milliseconds.
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        msg_read = channel.basic_get(queue_name, False)[2].decode().replace('\n', '')
        assert msg_read == input_str

        # Send a message, wait `expiration_ms` milliseconds, and consume RabbitMQ queue. If the "Set Expiration"
        # option is enabled, the queue will be empty and no message will be consumed.
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        time.sleep(expiration_ms * 0.001)
        msg_read = channel.basic_get(queue_name, False)[2]
        if set_expiration:
            assert msg_read == None
        else:
            assert msg_read.decode().replace('\n', '') == input_str

    finally:
        channel.queue_delete(queue_name)
        channel.close()
        connection.close()

@rabbitmq
def test_rabbitmq_rabbitmq_consumer_no_queue(sdc_builder, sdc_executor, rabbitmq):
    """
    Test for RabbitMQ consumer use case when queue is not specified. A validation error should be raised,
    as the consumer is unable to read without the queue.

    RabbitMQ Consumer pipeline:
        rabbitmq_consumer >> wiretap
    """

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    # We set to use default exchange and hence exchange does not need to be pre-created or given.
    rabbitmq_consumer = builder.add_stage('RabbitMQ Consumer').set_attributes(data_format='TEXT',
                                                                              durable=True,
                                                                              auto_delete=False,
                                                                              bindings=[])
    trash = builder.add_stage('Trash')
    rabbitmq_consumer >> trash

    consumer_origin_pipeline = builder.build(title='RabbitMQ Consumer pipeline').configure_for_environment(rabbitmq)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    # We don't need to publish messages, we shouldn't be able to start the pipeline
    try:
        sdc_executor.start_pipeline(consumer_origin_pipeline)
    except StartError as e:
        assert e.message.startswith("RABBITMQ_10")
    else:
        assert False


@rabbitmq
def test_rabbitmq_rabbitmq_consumer_wrong_format(sdc_builder, sdc_executor, rabbitmq):
    """Test for RabbitMQ consumer origin stage. We do so by publishing data to a test queue using RabbitMQ client and
    having a pipeline which reads that data using RabbitMQ consumer origin stage. Data is then asserted for what is
    published at RabbitMQ client and what we read in the pipeline wiretap.
    Ten records are treated. The second have wrong format an should be sent to error. The rest ones should be read.
    The batch size is set up to 1. It makes the connector to fail SDC-14644
    The pipeline looks like:

    RabbitMQ Consumer pipeline:
        rabbitmq_consumer >> wiretap
    """
    # Build consumer pipeline.
    name = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    # We set to use default exchange and hence exchange does not need to be pre-created or given.
    rabbitmq_consumer = builder.add_stage('RabbitMQ Consumer')
    rabbitmq_consumer.set_attributes(name=name,
                                     data_format='JSON',
                                     durable=True,
                                     auto_delete=False,
                                     on_record_error='TO_ERROR',
                                     max_batch_size_in_records=1,
                                     bindings=[dict(name=name,
                                                    type='DIRECT',
                                                    durable=True,
                                                    autoDelete=False)])
    wiretap = builder.add_wiretap()

    rabbitmq_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='RabbitMQ Consumer pipeline').configure_for_environment(rabbitmq)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    # Create input message and expected message.
    expected_messages = [{'msg': f'Message {i}'} for i in range(10) if i != 2]
    input_messages = [json.dumps(msg) for msg in expected_messages]
    input_messages.insert(1, '{"msg":')  #  Bad formatted JSON: no closing brace, no value.

    connection = rabbitmq.blocking_connection
    channel = connection.channel()

    # About default exchange routing: https://www.rabbitmq.com/tutorials/amqp-concepts.html
    channel.queue_declare(queue=name, durable=True, exclusive=False, auto_delete=False)
    channel.confirm_delivery()
    for msg in input_messages:
        try:
            channel.basic_publish(exchange="",
                                  routing_key=name,  # Routing key has to be same as queue name.
                                  body=msg,
                                  properties=pika.BasicProperties(content_type='text/plain', delivery_mode=1),
                                  mandatory=True)
        except:
            logger.warning('Message %s could not be sent.', msg)

    channel.close()
    connection.close()

    # Messages are published, read through the pipeline and assert.
    sdc_executor.start_pipeline(consumer_origin_pipeline)
    sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'data_batch_count', 10)
    sdc_executor.stop_pipeline(consumer_origin_pipeline)

    # Second message produced an error - the last error in the list

    history = sdc_executor.get_pipeline_history(consumer_origin_pipeline)
    assert history.latest.metrics.counter('stage.RabbitMQConsumer_01.stageErrors.counter').count > 0

    output_records = [record.field
                      for record in wiretap.output_records]

    # Datacollector does not guarantee the order of the messages, so we sort them.
    assert sorted(output_records, key=lambda rec: rec['msg'].value) == expected_messages


@rabbitmq
@sdc_min_version('4.1.0')
def test_rabbitmq_rabbitmq_consumer_quorum_queue(sdc_builder, sdc_executor, rabbitmq):
    """Test for RabbitMQ consumer with quorum queue. Similar to origin test but set using x-queue-type.

    RabbitMQ Consumer pipeline:
        rabbitmq_consumer >> wiretap
    """
    name = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    # Specify queue type in the additional configuration.
    rabbitmq_consumer = builder.add_stage('RabbitMQ Consumer').set_attributes(name=name,
                                                                              data_format='TEXT',
                                                                              durable=True,
                                                                              auto_delete=False,
                                                                              bindings=[],
                                                                              declaration_properties=[{
                                                                                "key": "x-queue-type",
                                                                                "value": "quorum"
                                                                              }])
    wiretap = builder.add_wiretap()

    rabbitmq_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='RabbitMQ Consumer pipeline').configure_for_environment(rabbitmq)
    sdc_executor.add_pipeline(consumer_origin_pipeline)
    sdc_executor.start_pipeline(consumer_origin_pipeline)

    expected_messages = set()
    connection = rabbitmq.blocking_connection
    channel = connection.channel()

    # Set the queue type as quorum in the additional arguments.
    channel.queue_declare(queue=name,
                          durable=True,
                          exclusive=False,
                          auto_delete=False,
                          arguments={"x-queue-type": "quorum"})
    channel.confirm_delivery()

    for i in range(10):
        expected_message = 'Message {0}'.format(i)
        expected_messages.add(expected_message)
        try:
            channel.basic_publish(exchange="",
                                  routing_key=name,  # Routing key has to be same as queue name.
                                  body=expected_message,
                                  properties=pika.BasicProperties(content_type='text/plain', delivery_mode=1),
                                  mandatory=True)
        except:
            logger.warning('Message %s could not be sent.', expected_message)

    channel.close()
    connection.close()

    sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(consumer_origin_pipeline)
    output_records = [record.field['text'].value
                      for record in wiretap.output_records]

    assert set(output_records) == expected_messages


@rabbitmq
def test_rabbitmq_producer_target_no_queue(sdc_builder, sdc_executor, rabbitmq):
    """Test for RabbitMQ producer target stage where user wants to connect to Exchange only. This is achieved by
    specifying only the configuration for the Exchange. The Exchange is configured on the RabbitMQ side to bind to a
    queue. We test by publishing data using RabbitMQ producer to a test Exchange that is bound to a specific queue
    and then read the data from that queue using RabbitMQ client. We assert the data from the client to what has
    been injected by the producer pipeline. The pipeline looks like:

    RabbitMQ Producer pipeline:
        dev_raw_data_source >> rabbitmq_producer
    """
    # build producer pipeline
    queue_name = get_random_string(string.ascii_letters, 10)
    exchange_name = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=raw_str)

    rabbitmq_producer = builder.add_stage('RabbitMQ Producer')
    rabbitmq_producer.set_attributes(data_format='TEXT',
                                     durable=True, auto_delete=False,
                                     bindings=[dict(name=exchange_name,
                                                    type='DIRECT',
                                                    durable=True,
                                                    autoDelete=False)])

    dev_raw_data_source >> rabbitmq_producer

    producer_dest_pipeline = builder.build(title='RabbitMQ Producer pipeline').configure_for_environment(rabbitmq)
    producer_dest_pipeline.rate_limit = 1

    try:
        # Declare a queue and an exchange and bind the queue to the exchange
        connection = rabbitmq.blocking_connection
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True, auto_delete=False)
        channel.exchange_declare(exchange=exchange_name, durable=True, auto_delete=False)
        channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key="")

        # add pipeline and capture pipeline messages to assert
        sdc_executor.add_pipeline(producer_dest_pipeline)
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_batch_count(10)
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        history = sdc_executor.get_pipeline_history(producer_dest_pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

        # Get one message at a time from RabbitMQ.
        # Returns a sequence with the method frame, message properties, and body.
        msgs_received = [channel.basic_get(queue_name, False)[2].decode().replace('\n', '')
                         for _ in range(msgs_sent_count)]

        logger.debug('Number of messages received from RabbitMQ = %d', (len(msgs_received)))
        assert msgs_received == [raw_str] * msgs_sent_count

    finally:
        # Unbind the queue to the exchange and delete the queue and the exchange
        channel.queue_unbind(queue_name, exchange_name)
        channel.queue_delete(queue_name)
        channel.exchange_delete(exchange_name)
        # Close the connection
        channel.close()
        connection.close()
