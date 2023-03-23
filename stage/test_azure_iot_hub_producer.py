# Copyright 2022 StreamSets Inc.
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

import pytest
from azure import servicebus

from streamsets.sdk.sdc_api import StartError
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@azure('iot')
def test_azure_iot_hub_producer_invalid_config(sdc_builder, sdc_executor):
    """Verify that the Azure IoT Hub Producer fails for invalid config."""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    azure_iot_hub_producer = pipeline_builder.add_stage('Azure IoT Hub Producer')
    azure_iot_hub_producer.set_attributes(data_format='JSON',
                                          iot_hub_name='inValidIoTHubName',
                                          device_id='inValidDeviceId',
                                          shared_access_key='inValidSasKey')
    dev_data_generator >> azure_iot_hub_producer
    pipeline = pipeline_builder.build()
    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        pytest.fail("Test should not reach here. It should have failed with StartError.")
    except StartError as e:
        assert "IOT_HUB_02" in e.message


@azure('iot')
@sdc_min_version('2.7.1.0')
def test_azure_iot_hub_producer(sdc_builder, sdc_executor, azure):
    """Test for Azure IoT Hub producer stage. Assumption is we setup Azure IoT Hub to route data
    to a Azure Service Bus topic and then have the IoT Hub producer stage publish data which can be read at the
    Service Bus topic to assert. We use a record deduplicator processor in between dev raw data source origin
    and IoT Hub producer destination in order to determine exactly what has been ingested. The pipeline looks like:

        dev_raw_data_source >> record_deduplicator >> azure_iot_hub_producer
                               record_deduplicator >> trash
    """
    # Note: Test will fail till SDC-7638 is addressed/fixed
    device_id = get_random_string(string.ascii_letters, 10)
    subscriber_id = get_random_string(string.ascii_letters, 10)
    raw_records = [{'Body': f'Hello {msg}'} for msg in range(10)]

    iot_hub = azure.iot_hub
    topic_name = azure.iot_hub_topic
    sb_service = azure.service_bus.service

    try:
        logger.info('Creating %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        device_info = iot_hub.create_device_id(device_id)
        device_access_key = device_info['authentication']['symmetricKey']['primaryKey']

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                      json_content='ARRAY_OBJECTS',
                                                                                      raw_data=json.dumps(raw_records))
        record_deduplicator = builder.add_stage('Record Deduplicator')
        trash = builder.add_stage('Trash')
        azure_iot_hub_producer = builder.add_stage('Azure IoT Hub Producer')
        azure_iot_hub_producer.set_attributes(data_format='JSON', device_id=device_id, json_content='MULTIPLE_OBJECTS',
                                              shared_access_key=device_access_key)

        dev_raw_data_source >> record_deduplicator >> azure_iot_hub_producer
        record_deduplicator >> trash
        producer_dest_pipeline = builder.build(title='IoT Producer pipeline').configure_for_environment(azure)

        # Note: set topic subscriber first, else data will not be read.
        logger.info('Creating %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.create_subscription(topic_name, subscriber_id)
        # Use a Azure Rule filter to read topic for our specific Device ID.
        rule = servicebus.Rule()
        rule.filter_type = 'SqlFilter'
        rule.filter_expression = f"[$.cdid] = '{device_id}'"
        sb_service.create_rule(topic_name, subscriber_id, f'{subscriber_id}Filter', rule)
        sb_service.delete_rule(topic_name, subscriber_id, '$Default')

        sdc_executor.add_pipeline(producer_dest_pipeline)
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_output_records_count(len(raw_records))
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        # read data from Azure Service Bus Topic and assert
        logger.info('Reading data from Azure Service Bus topic %s ...', topic_name)
        messages = [json.loads(sb_service.receive_subscription_message(topic_name=topic_name,
                                                                       subscription_name=subscriber_id, peek_lock=True,
                                                                       timeout=10).body.decode().strip())
                    for _ in range(len(raw_records))]
        assert len(raw_records) == len(messages)
        assert all(item in messages for item in raw_records)

    finally:
        logger.info('Deleting %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.delete_rule(topic_name, subscriber_id, servicebus.DEFAULT_RULE_NAME)
        sb_service.delete_subscription(topic_name, subscriber_id)
        logger.info('Deleting %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        iot_hub.delete_device_id(device_id)
