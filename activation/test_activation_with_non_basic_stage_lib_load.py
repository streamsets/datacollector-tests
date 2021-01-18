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

# This module loads a non-basic stage lib jms in SDC while starting and runs pipelines for tests.

import logging
from string import ascii_letters

import pytest
import requests
from streamsets.testframework.markers import jms, sdc_activation, sdc_min_version
from streamsets.testframework.utils import get_random_string

from .utils.utils_activation import ACTIVATION_SUPPORT_SDC_MIN_VERSION, register_and_activate_sdc

# For jms
DEFAULT_PASSWORD = 'admin'
DEFAULT_USERNAME = 'admin'
JMS_DESTINATION_TYPE = 'QUEUE'
JMS_INITIAL_CONTEXT_FACTORY = 'org.apache.activemq.jndi.ActiveMQInitialContextFactory'
JNDI_CONNECTION_FACTORY = 'ConnectionFactory'


# Skip all tests in this module if --sdc-version < 3.15.0
pytestmark = sdc_min_version(ACTIVATION_SUPPORT_SDC_MIN_VERSION)

logger = logging.getLogger(__name__)


@jms('activemq')
@sdc_activation
@pytest.mark.parametrize('activate_sdc', [False, True])
def test_with_non_basic_stage_loaded(sdc_executor, activate_sdc, jms):
    """SDC gets loaded with jms stage-lib which is not a basic stage.

    The pipelines should not run when SDC is not activated. activate_sdc = False
    The pipelines should be able to run when SDC is activated. activate_sdc = true
    """
    if activate_sdc:
        register_and_activate_sdc(sdc_executor)
        _test_basic_stage(sdc_executor)
        _test_jms_consumer_origin(sdc_executor, jms)
    else:
        expected = "403 Client Error: Forbidden for url:"
        with pytest.raises(requests.exceptions.HTTPError, match=expected):
            # Note here: the following is expected to produce 403 even though it is a basic stage.
            # Reason is : STF loaded jms stage lib while starting and so now it needs registration and activation
            # in order to run any pipelines.
            _test_basic_stage(sdc_executor)
        with pytest.raises(requests.exceptions.HTTPError, match=expected):
            _test_jms_consumer_origin(sdc_executor, jms)


def _test_basic_stage(sdc_executor):
    pipeline_builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"emp_id" :"123456"}'
    dev_raw_data_source.stop_after_first_batch = True
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].field['emp_id'].value == '123456'


def _test_jms_consumer_origin(sdc_executor, jms):
    """
    Send simple text messages to JMS using ActiveMQ python client that uses STOMP
    and confirm that JMS origin reads them.

    The pipeline looks like:
        jms_consumer >> trash
    """
    pipeline_builder = sdc_executor.get_pipeline_builder()

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
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field['text'].value for record in wiretap.output_records] == [message_data] * 10

    finally:
        connection.send(destination_name, 'SHUTDOWN', persistent='false')
        connection.disconnect()
