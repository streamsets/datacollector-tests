# Copyright 2019 StreamSets Inc.
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

"""A module to test various SDC stages of Azure."""

import json
import logging
import string

from azure import servicebus
import pytest
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# To workaround the stage label tweak introduced in 3.0.1.0 (SDC-8077), we use the
# Azure IoT/Event Hub Consumer stage's full name in tests.
AZURE_IOT_EVENT_HUB_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_eventhubs_EventHubConsumerDSource'


@azure('eventhub')
@sdc_min_version('2.7.1.0')
def test_azure_event_hub_consumer(sdc_builder, sdc_executor, azure):
    """Test for Azure IoT/Event Hub consumer origin stage. We do so by publishing data to a test event hub Azure client
    and having a pipeline which reads that data using Azure IoT/Event Hub consumer origin stage. Data is then asserted
    for what is published at Azure client and what we read in the pipeline snapshot. The pipeline looks like:

    azure_iot_event_hub_consumer >> trash
    """
    # Azure container names are lowercased. Ref. http://tinyurl.com/ya9y9mm6
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name, data_format='JSON',
                                                event_hub_name=event_hub_name)
    trash = builder.add_stage('Trash')

    azure_iot_event_hub_consumer >> trash

    consumer_origin_pipeline = builder.build(title='Azure Event Hub Consumer pipeline').configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        send_records = [{'Body': f'Event {msg}'} for msg in range(10)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records))

        snapshot = sdc_executor.capture_snapshot(
            consumer_origin_pipeline, start_pipeline=True, timeout_sec=120
        ).snapshot
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_record = snapshot[azure_iot_event_hub_consumer.instance_name].output[0].field
        results = [{key: value for key, value in record.items()} for record in result_record]
        assert results == send_records
    finally:
        logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        eh_service_bus.delete_event_hub(event_hub_name)

        logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
        azure.storage.delete_blob_container(container_name)


@azure('eventhub')
@sdc_min_version('2.7.1.0')
def test_azure_event_hub_consumer_resume_offset(sdc_builder, sdc_executor, azure):
    """Test for Azure IoT/Event Hub consumer origin stage. We do so by publishing data to a test event hub Azure client
    and having a pipeline which reads that data using Azure IoT/Event Hub consumer origin stage. Data is then asserted
    for what is published at Azure client and what we read in the pipeline snapshot. We then create more data, restart
    the pipeline, and take another snapshot to ensure that the stage properly resumes from where the offset left off.
    The pipeline looks like:

    azure_iot_event_hub_consumer >> trash
    """
    # Azure container names are lowercased. Ref. http://tinyurl.com/ya9y9mm6
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name, data_format='JSON',
                                                event_hub_name=event_hub_name)
    trash = builder.add_stage('Trash')

    azure_iot_event_hub_consumer >> trash

    consumer_origin_pipeline = builder.build(title='Azure Event Hub Consumer pipeline').configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        send_records = [{'Body': f'Event {msg}'} for msg in range(10)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records))

        snapshot = sdc_executor.capture_snapshot(consumer_origin_pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=True)

        result_record = snapshot[azure_iot_event_hub_consumer.instance_name].output[0].field
        results = [{key: value for key, value in record.items()} for record in result_record]
        assert results == send_records

        # Try adding more data and resuming from the offset
        send_records2 = [{'Body': f'Event {msg}'} for msg in range(10, 20)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records2))

        snapshot = sdc_executor.capture_snapshot(consumer_origin_pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_record = snapshot[azure_iot_event_hub_consumer.instance_name].output[0].field
        results = [{key: value for key, value in record.items()} for record in result_record]
        assert results == send_records2
    finally:
        logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        eh_service_bus.delete_event_hub(event_hub_name)

        logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
        azure.storage.delete_blob_container(container_name)


@azure('eventhub')
@sdc_min_version('2.7.1.0')
@pytest.mark.parametrize('destination_data_format', ['JSON', 'XML'])
def test_azure_event_hub_producer(sdc_builder, sdc_executor, azure, destination_data_format):
    """Test for Azure Event Hub producer destination stage. We do so by using two pipelines. The 1st, Event Hub
    producer pipeline which publishes data which is captured by 2nd, Event Hub consumer. We then assert data at
    the 2nd pipeline by doing a snapshot and comparing it to what was ingested at the 1st pipeline. We use a
    record deduplicator processor in between dev raw data source origin and Event Hub producer destination in order
    to determine exactly what has been ingested. The pipelines looks like:

        dev_raw_data_source >> record_deduplicator >> azure_event_hub_producer
                                                   >> trash
        azure_iot_event_hub_consumer >> trash
    """
    # Note: Test will fail till SDC-7627 is addressed/fixed
    # Note: Cannot use Azure SDK https://github.com/Azure/azure-event-hubs-python as it requires native build,
    # specific for a platform.

    # Support for XML data format for Azure Event Hub producer is only available for SDC_VERSION >= 3.12
    if destination_data_format == 'XML' and Version(sdc_builder.version) < Version("3.12.0"):
        pytest.skip('XML data format for Azure Event Hub Producer not available for sdc_version {sdc_builder.version}.')

    if destination_data_format == 'XML':
        # XML Data conversion requires having a root element
        # The example for destination_data_format = JSON has more than 1 root element
        # Use a simpler single element dictionary instead for XML testcase
       raw_data = '{"key":"value"}'
       EXPECTED_XML_OUTPUT = ['<?xml version="1.0" encoding="UTF-8" standalone="no"?>','<key>value</key>']
    else:
        raw_list = [dict(name='Jane Smith', phone=2124050000, zip_code=27023)]
        raw_data = json.dumps(raw_list)

    # Azure container names are lowercased. Ref. http://tinyurl.com/ya9y9mm6
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_letters, 10)

    # build Event Hub producer
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')

    json_content_type = 'MULTIPLE_OBJECTS' if destination_data_format == 'XML' else 'ARRAY_OBJECTS'
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, json_content=json_content_type)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    producer_trash = builder.add_stage('Trash')

    azure_event_hub_producer = builder.add_stage('Azure Event Hub Producer')
    if destination_data_format == 'JSON':
        azure_event_hub_producer.set_attributes(data_format='JSON', event_hub_name=event_hub_name,
                                                json_content='ARRAY_OBJECTS')
    elif destination_data_format == 'XML':
        azure_event_hub_producer.set_attributes(data_format='XML', event_hub_name=event_hub_name)

    dev_raw_data_source >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = builder.build(title='Azure Event Hub Producer').configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)

    # Setting the Consumer Pipeline' data format as Text, so that the XML Header line can be verfied
    consumer_data_format = 'TEXT' if destination_data_format == 'XML' else 'JSON'
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name, data_format=consumer_data_format,
                                                event_hub_name=event_hub_name)
    consumer_trash = builder.add_stage('Trash')

    azure_iot_event_hub_consumer >> consumer_trash
    consumer_origin_pipeline = builder.build(title='Azure Event Hub Consumer pipeline').configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        # publish events and read through the consumer pipeline to assert
        sdc_executor.start_pipeline(producer_dest_pipeline)
        snapshot = sdc_executor.capture_snapshot(consumer_origin_pipeline, start_pipeline=True, timeout_sec=120).snapshot

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = snapshot[azure_iot_event_hub_consumer.instance_name].output
        if destination_data_format == 'JSON':
            assert len(result_records) == 1
            assert result_records[0].field == raw_list
        elif destination_data_format == 'XML':
            assert [record.field['text'] for record in result_records] == EXPECTED_XML_OUTPUT
    finally:
        logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        eh_service_bus.delete_event_hub(event_hub_name)

        logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
        azure.storage.delete_blob_container(container_name)


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
    raw_records = [{'Body': f'Hello {msg}'} for msg in range(10)]

    try:
        iot_hub = azure.iot_hub
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
        topic_name = azure.iot_hub_topic
        subscriber_id = get_random_string(string.ascii_letters, 10)

        sb_service = azure.service_bus.service
        logger.info('Creating %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.create_subscription(topic_name, subscriber_id)
        # Use a Azure Rule filter to read topic for our specific Device ID.
        rule = servicebus.Rule()
        rule.filter_type = 'SqlFilter'
        rule.filter_expression = f'iothub-connection-device-id = "{device_id}"'
        sb_service.create_rule(topic_name, subscriber_id, f'{subscriber_id}Filter', rule)

        sdc_executor.add_pipeline(producer_dest_pipeline)
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_output_records_count(len(raw_records))
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        # read data from Azure Service Bus Topic and assert
        logger.info('Reading data from Azure Service Bus topic %s ...', topic_name)
        messages = [json.loads(sb_service.receive_subscription_message(topic_name=topic_name,
                                                                       subscription_name=subscriber_id, peek_lock=False,
                                                                       timeout=10).body.decode().strip())
                    for _ in range(len(raw_records))]
        assert raw_records == messages
    finally:
        logger.info('Deleting %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.delete_rule(topic_name, subscriber_id, servicebus.DEFAULT_RULE_NAME)
        sb_service.delete_subscription(topic_name, subscriber_id)
        logger.info('Deleting %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        iot_hub.delete_device_id(device_id)
