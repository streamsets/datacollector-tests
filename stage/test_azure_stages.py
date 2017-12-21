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

"""A module to test various SDC stages of Azure."""

import json
import logging
import string

from testframework.markers import azure, sdc_min_version
from testframework.utils import get_random_string

from azure import servicebus

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# To workaround the stage label tweak introduced in 3.0.1.0 (SDC-8077), we use the
# Azure IoT/Event Hub Consumer stage's full name in tests.
AZURE_IOT_EVENT_HUB_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_eventhubs_EventHubConsumerDSource'


@azure('datalake')
@sdc_min_version('2.2.0.0')
def test_datalake_destination(sdc_builder, sdc_executor, azure):
    """Test for Data Lake Store target stage. We do so by running a dev raw data source generator to Data Lake Store
    destination with its provided account FQDN and then reading Data Lake Store using STF client to assert data
    between the client to what has been ingested by the pipeline. We use a record deduplicator processor in
    between dev raw data source origin and Data Lake Store destination in order to determine exactly what has
    been ingested. The pipeline looks like:

    Data Lake Store Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> azure_data_lake_store_destination
                                                   >> to_error
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = get_random_string(string.ascii_letters, 10)
    raw_list = [dict(contact=dict(name='Jane Smith', phone=2124050000, zip_code=27023)),
                dict(contact=dict(name='San', phone=2120998998, zip_code=14305))]
    raw_data = json.dumps(raw_list)

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    azure_data_lake_store_destination = builder.add_stage('Azure Data Lake Store', type='destination')
    azure_data_lake_store_destination.set_attributes(directory_template=directory_name, files_prefix=files_prefix,
                                                     files_suffix=files_suffix)

    dev_raw_data_source >> record_deduplicator >> azure_data_lake_store_destination
    record_deduplicator >> to_error

    datalake_dest_pipeline = builder.build(title='Azure Data Lake dest pipeline').configure_for_environment(azure)
    sdc_executor.add_pipeline(datalake_dest_pipeline)

    try:
        # start pipeline and capture pipeline messages to assert
        logger.info('Azure Data Lake directory %s will be created with files prefix %s', directory_name, files_prefix)
        sdc_executor.start_pipeline(datalake_dest_pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(datalake_dest_pipeline)

        dl_fs = azure.datalake.file_system
        dl_files = dl_fs.ls(directory_name)
        # assert Data Lake files generated
        assert len(dl_files) == 1

        # assert file prefix and suffix
        dl_file_name = dl_files[0].split('/')[-1]
        assert dl_file_name.startswith(files_prefix) and dl_file_name.endswith(files_suffix)

        # Assert file content. File will have len(raw_list) JSON formatted records, delimited by newline (\n).
        dl_file_contents = dl_fs.cat(dl_files[0]).decode()
        result_list = [json.loads(line) for line in dl_file_contents.split('\n')]

        assert raw_list == result_list
    finally:
        # Note: Non-empty directory is not allowed to be removed, hence remove all files first.
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        for dl_file in dl_files:
            dl_fs.rm(dl_file)
        dl_fs.rmdir(directory_name)


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

        snapshot = sdc_executor.capture_snapshot(consumer_origin_pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_record = snapshot[azure_iot_event_hub_consumer.instance_name].output[0].value['value']
        results = [{key: value['value'] for key, value in record['value'].items()} for record in result_record]
        assert results == send_records
    finally:
        logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        eh_service_bus.delete_event_hub(event_hub_name)

        logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
        azure.storage.delete_blob_container(container_name)


@azure('eventhub')
@sdc_min_version('2.7.1.0')
def test_azure_event_hub_producer(sdc_builder, sdc_executor, azure):
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
    raw_list = [dict(contact=dict(name='Jane Smith', phone=2124050000, zip_code=27023)),
                dict(contact=dict(name='San', phone=2120998998, zip_code=14305))]
    raw_data = json.dumps(raw_list)

    # Azure container names are lowercased. Ref. http://tinyurl.com/ya9y9mm6
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_letters, 10)

    # build Event Hub producer
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, json_content='ARRAY_OBJECTS')

    record_deduplicator = builder.add_stage('Record Deduplicator')
    producer_trash = builder.add_stage('Trash')

    azure_event_hub_producer = builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='JSON', event_hub_name=event_hub_name,
                                            json_content='ARRAY_OBJECTS')

    dev_raw_data_source >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = builder.build(title='Azure Event Hub Producer').configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name, data_format='JSON',
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
        snapshot = sdc_executor.capture_snapshot(consumer_origin_pipeline, start_pipeline=True).snapshot

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_record = snapshot[azure_iot_event_hub_consumer.instance_name].output[0].value['value']

        results = [{key: value['value'] for key, value in record['value'].items()} for record in result_record]
        assert results == raw_data
    finally:
        logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        eh_service_bus.delete_event_hub(event_hub_name)

        logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
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
        azure_iot_hub_producer.set_attributes(data_format='JSON', device_id=device_id, json_content='ARRAY_OBJECTS',
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
