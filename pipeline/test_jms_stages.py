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

JMS_DESTINATION_TYPE = 'QUEUE'
JMS_INITIAL_CONTEXT_FACTORY = 'org.apache.activemq.jndi.ActiveMQInitialContextFactory'
JNDI_CONNECTION_FACTORY = 'ConnectionFactory'


@jms('activemq')
@sdc_min_version("4.4.0")
@pytest.mark.parametrize('include_headers, prefix_all_headers', [[True, True], [True, False], [False, False]])
def test_jms_headers_origin_to_destination(sdc_builder, sdc_executor, jms, include_headers, prefix_all_headers):
    """Validating that JMS standard headers are serialized into Record headers."""
    message_data = 'This will be a message sent to JMS'
    custom_header = 'This is my custom header'
    custom_header_from_origin = 'This is my custom header from origin'
    headers = {'custom_hedader_from_origin': custom_header_from_origin}
    first_queue = get_random_string(ascii_letters, 5)
    second_queue = get_random_string(ascii_letters, 5)
    builder = sdc_builder.get_pipeline_builder()

    # Configure the jms_consumer stage
    jms_consumer = builder.add_stage('JMS Consumer')
    jms_consumer.set_attributes(data_format='TEXT',
                                jms_destination_name=first_queue,
                                jms_destination_type=JMS_DESTINATION_TYPE,
                                jms_initial_context_factory=JMS_INITIAL_CONTEXT_FACTORY,
                                jndi_connection_factory=JNDI_CONNECTION_FACTORY,
                                password=DEFAULT_PASSWORD,
                                username=DEFAULT_USERNAME,
                                prefix_all_headers=prefix_all_headers)

    jms_producer = builder.add_stage('JMS Producer', type='destination')
    jms_producer.set_attributes(data_format='TEXT',
                                jms_destination_name=second_queue,
                                jms_destination_type=JMS_DESTINATION_TYPE,
                                jms_initial_context_factory=JMS_INITIAL_CONTEXT_FACTORY,
                                jndi_connection_factory=JNDI_CONNECTION_FACTORY,
                                password=DEFAULT_PASSWORD,
                                username=DEFAULT_USERNAME,
                                include_headers=include_headers)

    expression_evaluator = builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {
            "attributeToSet": "jms.header.customHeader",
            "headerAttributeExpression": custom_header
        }
    ])

    jms_consumer >> expression_evaluator >> jms_producer

    pipeline = builder.build().configure_for_environment(jms)
    sdc_executor.add_pipeline(pipeline)

    connection = jms.client_connection
    try:
        logger.info('Sending messages to JMS using ActiveMQ client ...')

        listener = _TestListener()
        connection.set_listener('', listener)
        connection.start()
        connection.connect(login=DEFAULT_USERNAME, passcode=DEFAULT_PASSWORD)
        connection.send(first_queue, message_data, headers=headers, persistent='false')
        connection.subscribe(destination=f'/queue/{second_queue}', id=second_queue)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(listener.message_list) == 1

        message = listener.message_list[0]
        assert message_data + '\n' in message  # JMS introduces a new line at the end of the message

        message = str(message)
        if include_headers:
            assert custom_header in message
            assert 'jms.header.messageId' in message
            assert 'jms.header.timestamp' in message
            assert 'jms.header.correlationId' in message
            assert 'jms.header.deliveryMode' in message
            assert 'jms.header.redelivered' in message
            assert 'jms.header.type' in message
            assert 'jms.header.expiration' in message
            assert 'jms.header.priority' in message

            if prefix_all_headers:
                assert custom_header_from_origin in message
            else:
                assert custom_header_from_origin not in message
        else:
            assert custom_header not in message
            assert 'jms.header.messageId' not in message
            assert 'jms.header.timestamp' not in message
            assert 'jms.header.correlationId' not in message
            assert 'jms.header.deliveryMode' not in message
            assert 'jms.header.redelivered' not in message
            assert 'jms.header.type' not in message
            assert 'jms.header.expiration' not in message
            assert 'jms.header.priority' not in message

    finally:
        connection.send(first_queue, 'SHUTDOWN', persistent='false')
        connection.send(second_queue, 'SHUTDOWN', persistent='false')
        connection.disconnect()
