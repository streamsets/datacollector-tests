# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import logging
from string import ascii_letters

from testframework.markers import jms
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)

@jms('activemq')
def test_jms_origin_simple(sdc_builder, sdc_executor, jms):
    """
    Send simple text messages to JMS using ActiveMQ python client that uses stomp
    and confirm that JMS origin reads them.

    The pipeline looks like:
        jms_origin >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Configure the jms_origin stage
    jms_origin = pipeline_builder.add_stage('JMS Consumer')
    destination_name = get_random_string(ascii_letters, 5)
    username = 'admin'
    password = 'admin'
    jms_origin.set_attributes(data_format='TEXT',
                              jms_destination_name=destination_name,
                              jms_destination_type='QUEUE',
                              jms_initial_context_factory='org.apache.activemq.jndi.ActiveMQInitialContextFactory',
                              jndi_connection_factory='ConnectionFactory',
                              password=password,
                              username=username)

    trash = pipeline_builder.add_stage('Trash')
    pipeline_builder.add_error_stage('Discard')
    jms_origin >> trash
    pipeline = pipeline_builder.build().configure_for_environment(jms)
    sdc_executor.add_pipeline(pipeline)

    connection = jms.client_connection

    try:
        logger.info('Sending messages to JMS using ActiveMQ client...')
        connection.start()
        connection.connect(login=username, passcode=password)
        message_data = 'Hello World from SDC & DPM!'
        for _ in range(10):
            connection.send(destination_name, message_data, persistent='false')

        # Verify the messages are received correctly.
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline)
        lines_from_snapshot = [record.value['value']['text']['value']
                               for record in snapshot[pipeline[0].instance_name].output]

        assert lines_from_snapshot == [message_data] * 10

    finally:
        if connection is not None:
            connection.send(destination_name, 'SHUTDOWN', persistent='false')
            connection.disconnect()
