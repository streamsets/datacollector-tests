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


import logging
from string import ascii_letters

import pytest
# Import TestListener with a leading underscore to prevent pytest from
# thinking that it's a test class.
from stomp.listener import TestListener as _TestListener
from streamsets.testframework.markers import jms, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

DEFAULT_PASSWORD = 'admin'
DEFAULT_USERNAME = 'admin'
EXPECTED_PRETTY_XML = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<info>\n  <companies>\n    '
                       '<Company>Streamsets</Company>\n    <State>California</State>\n  </companies>\n  '
                       '<companies>\n    <Company>Cloudera</Company>\n    <State>California</State>\n  '
                       '</companies>\n</info>\n')
EXPECTED_TEXT = 'Hello World!\n'
EXPECTED_XML = ('<?xml version="1.0" encoding="UTF-8" standalone="no"?><info><companies><Company>Streamsets'
                '</Company><State>California</State></companies><companies><Company>Cloudera</Company><State>'
                'California</State></companies></info>')
JMS_DESTINATION_TYPE = 'QUEUE'
JMS_INITIAL_CONTEXT_FACTORY = 'org.apache.activemq.jndi.ActiveMQInitialContextFactory'
JNDI_CONNECTION_FACTORY = 'ConnectionFactory'


@jms('activemq')
def test_jms_consumer_origin(sdc_builder, sdc_executor, jms):
    """
    Send simple text messages to JMS using ActiveMQ python client that uses STOMP
    and confirm that JMS origin reads them.

    The pipeline looks like:
        jms_consumer >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Configure the jms_consumer stage
    jms_consumer = pipeline_builder.add_stage('JMS Consumer')
    destination_name = get_random_string(ascii_letters, 5)
    jms_consumer.set_attributes(data_format='TEXT',
                                jms_destination_name=destination_name,
                                jms_destination_type=JMS_DESTINATION_TYPE,
                                jms_initial_context_factory=JMS_INITIAL_CONTEXT_FACTORY,
                                jndi_connection_factory=JNDI_CONNECTION_FACTORY,
                                password=DEFAULT_PASSWORD,
                                username=DEFAULT_USERNAME)
    wiretap = pipeline_builder.add_wiretap()
    pipeline_builder.add_error_stage('Discard')
    jms_consumer >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(jms)
    sdc_executor.add_pipeline(pipeline)

    connection = jms.client_connection

    try:
        logger.info('Sending messages to JMS using ActiveMQ client ...')
        connection.start()
        connection.connect(login=DEFAULT_USERNAME, passcode=DEFAULT_PASSWORD)
        message_data = 'Hello World from SDC & DPM!'
        for _ in range(10):
            connection.send(destination_name, message_data, persistent='false')

        # Verify the messages are received correctly.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)
        lines_from_records = [record.field['text'].value
                               for record in wiretap.output_records]

        assert lines_from_records == [message_data] * 10

    finally:
        connection.send(destination_name, 'SHUTDOWN', persistent='false')
        connection.disconnect()


@sdc_min_version("3.9.0")
@jms('activemq')
def test_jms_consumer_origin_durable_topic_sub(sdc_builder, sdc_executor, jms):
    """
    Send simple text messages to JMS using ActiveMQ python client that uses STOMP
    and confirm that JMS origin, configured with a durable topic subscription, reads them.

    The pipeline looks like:
        jms_consumer >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Configure the jms_consumer stage
    jms_consumer = pipeline_builder.add_stage('JMS Consumer')
    destination_name = get_random_string(ascii_letters, 5)
    jms_consumer.set_attributes(data_format='TEXT',
                                jms_destination_name=destination_name,
                                jms_destination_type='TOPIC',
                                jms_initial_context_factory=JMS_INITIAL_CONTEXT_FACTORY,
                                jndi_connection_factory=JNDI_CONNECTION_FACTORY,
                                password=DEFAULT_PASSWORD,
                                username=DEFAULT_USERNAME,
                                client_id='client' + destination_name,
                                durable_subscription=True,
                                durable_subscription_name='sub' + destination_name)

    wiretap = pipeline_builder.add_wiretap()
    pipeline_builder.add_error_stage('Discard')
    jms_consumer >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(jms)
    sdc_executor.add_pipeline(pipeline)

    connection = jms.client_connection

    try:
        """ 
        pipeline is started to create the topic and durable subscription
        before the STOMP publisher sends messages 
        """
        sdc_executor.start_pipeline(pipeline)
        """ and then stopped so we test the durable aspect of this subscription """
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Sending messages to JMS using ActiveMQ client ...')
        connection.start()
        connection.connect(login=DEFAULT_USERNAME, passcode=DEFAULT_PASSWORD)
        message_data = 'Hello World from SDC & DPM!'
        for _ in range(10):
            connection.send('/topic/' + destination_name, message_data, persistent='false')

        # Verify the messages are received correctly.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)
        lines_from_records = [record.value['value']['text']['value']
                               for record in wiretap.output_records]

        assert lines_from_records == [message_data] * 10

    finally:
        connection.disconnect()


def get_dev_raw_data_source_stage_text_input(pipeline_builder):
    """Create Dev Raw Data Source stage with TEXT as input."""
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data='Hello World!')
    return dev_raw_data_source


def get_dev_raw_data_source_stage_json_input(pipeline_builder):
    """Create Dev Raw Data Source stage with XML as input."""
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = """
        {
            "info" : {
                "companies": [
                    {
                        "Company" : "Streamsets",
                        "State" : "California"
                    }, {
                        "Company" : "Cloudera",
                        "State" : "California"
                    }
                ]
            }
       }
    """
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data)
    return dev_raw_data_source


@jms('activemq')
@pytest.mark.parametrize('input_type, pretty_format, expected', [
    ('TEXT', True, EXPECTED_TEXT),
    ('JSON', True, EXPECTED_PRETTY_XML),
    ('JSON', False, EXPECTED_XML)
])
def test_jms_producer_destination(sdc_builder, sdc_executor, jms, input_type, pretty_format, expected):
    """
    Send simple text messages into JMS Producer Destination from Dev Raw Data Source and
    confirm that JMS successfully reads them using ActiveMQ python client that uses STOMP.
    Specifically, this would look like:

    JMS Producer pipeline:
           dev_raw_data_source >> jms_producer
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    if input_type == 'TEXT':
        dev_raw_data_source = get_dev_raw_data_source_stage_text_input(pipeline_builder)
    else:
        dev_raw_data_source = get_dev_raw_data_source_stage_json_input(pipeline_builder)

    # Configure the jms_producer stage.
    jms_producer = pipeline_builder.add_stage('JMS Producer', type='destination')
    destination_name = get_random_string(ascii_letters, 5)
    jms_producer.set_attributes(data_format='XML' if input_type == 'JSON' else 'TEXT',
                                jms_destination_name=destination_name,
                                jms_destination_type=JMS_DESTINATION_TYPE,
                                jms_initial_context_factory=JMS_INITIAL_CONTEXT_FACTORY,
                                jndi_connection_factory=JNDI_CONNECTION_FACTORY,
                                password=DEFAULT_PASSWORD,
                                username=DEFAULT_USERNAME)
    if input_type == 'JSON':
        jms_producer.pretty_format = pretty_format
    pipeline_builder.add_error_stage('Discard')
    dev_raw_data_source >> jms_producer
    pipeline_title = f'{input_type}-{pretty_format} JMS Producer pipeline'
    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(jms)
    sdc_executor.add_pipeline(pipeline)

    connection = jms.client_connection
    try:
        logger.info('Subscribing to queue ...')
        listener = _TestListener()
        connection.set_listener('', listener)
        connection.start()
        connection.connect(login=DEFAULT_USERNAME, passcode=DEFAULT_PASSWORD)
        connection.subscribe(destination=f'/queue/{destination_name}', id=destination_name)

        # Send messages using pipeline to JMS Destination.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.stop_pipeline(pipeline)
        history = sdc_executor.get_pipeline_history(pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.debug('No. of messages sent in the pipeline = %s', msgs_sent_count)

        # Verify messages received using python client.
        msgs_received = [message[1] for message in listener.message_list]
        assert msgs_received == [expected] * msgs_sent_count

    finally:
        connection.send(destination_name, 'SHUTDOWN', persistent='false')
        connection.disconnect()


# SDC-15870: Serialize JMS Headers into Record Headers
@sdc_min_version("3.19.0")
@jms('activemq')
def test_jms_consumer_origin_headers(sdc_builder, sdc_executor, jms):
    """Validating that JMS standard headers are serialized into Record headers."""
    destination_name = get_random_string(ascii_letters, 5)
    builder = sdc_builder.get_pipeline_builder()

    # Configure the jms_consumer stage
    origin = builder.add_stage('JMS Consumer')
    origin.set_attributes(data_format='TEXT',
                          jms_destination_name=destination_name,
                          jms_destination_type=JMS_DESTINATION_TYPE,
                          jms_initial_context_factory=JMS_INITIAL_CONTEXT_FACTORY,
                          jndi_connection_factory=JNDI_CONNECTION_FACTORY,
                          password=DEFAULT_PASSWORD,
                          username=DEFAULT_USERNAME)

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(jms)
    sdc_executor.add_pipeline(pipeline)

    connection = jms.client_connection

    try:
        logger.info('Sending messages to JMS using ActiveMQ client ...')
        connection.start()
        connection.connect(login=DEFAULT_USERNAME, passcode=DEFAULT_PASSWORD)
        connection.send(destination_name, 'something', persistent='false')

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 1

        # JMS Headers
        assert 'jms.header.messageId' in records[0].header.values
        assert 'jms.header.timestamp' in records[0].header.values
        assert 'jms.header.correlationId' in records[0].header.values
        assert 'jms.header.deliveryMode' in records[0].header.values
        assert 'jms.header.redelivered' in records[0].header.values
        assert 'jms.header.type' in records[0].header.values
        assert 'jms.header.expiration' in records[0].header.values
        assert 'jms.header.priority' in records[0].header.values
    finally:
        connection.send(destination_name, 'SHUTDOWN', persistent='false')
        connection.disconnect()

