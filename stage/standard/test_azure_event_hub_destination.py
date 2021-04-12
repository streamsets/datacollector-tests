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
import string
import json

import pytest
from streamsets.testframework.decorators import stub
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import azure
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

AZURE_IOT_EVENT_HUB_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_eventhubs_EventHubConsumerDSource'


DATA_TYPES = [
    ('true', 'BOOLEAN', True),
    ('a', 'CHAR', 'a'),
#   ('a', 'BYTE', None), # Not supported today
    (120, 'SHORT', 120),
    (120, 'INTEGER', 120),
    (120, 'LONG', 120),
    (20.1, 'FLOAT', 20.1),
    (20.1, 'DOUBLE', 20.1),
    (20.1, 'DECIMAL', 20.1),
    ('2020-01-01 10:00:00', 'DATE', 1577872800000),
    ('2020-01-01 10:00:00', 'TIME', 1577872800000),
    ('2020-01-01 10:00:00', 'DATETIME', 1577872800000),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', '2020-01-01T10:00:00Z'),
    ('string', 'STRING', 'string'),
#   ('string', 'BYTE_ARRAY', 'string') # Not supported today
]
@azure('eventhub')
@pytest.mark.parametrize('input,converter_type,expected', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, azure, input, converter_type, expected):
    # Azure container names are lowercased. Ref. http://tinyurl.com/ya9y9mm6
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    origin = pipeline_builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON',
                          raw_data=json.dumps({"value": input}))

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    producer_trash = pipeline_builder.add_stage('Trash')

    converter = pipeline_builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_FIELD',
                             field_type_converter_configs=[{
                                'fields': ['/value'],
                                'targetType': converter_type,
                                'dataLocale': 'en,US',
                                'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
                                'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
                                'scale': 2
                              }])

    azure_event_hub_producer = pipeline_builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='JSON',
                                            event_hub_name=event_hub_name,
                                            json_content='ARRAY_OBJECTS')

    origin >> record_deduplicator >> converter >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name,
                                                data_format='JSON',
                                                event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

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
        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = wiretap.output_records
        results = [{key: value for key, value in record.items()} for record in result_records[0].field]

        assert len(result_records) == 1
        assert results[0]['value'] == expected

    finally:
        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name,
                        azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


# Reference http://tinyurl.com/ya9y9mm6
EVENT_HUB_CONTAINERS_NAMES = [
    ('minsize', lambda: get_random_string(string.ascii_lowercase, 3)),
    ('maxsize', lambda: get_random_string(string.ascii_lowercase, 63)),
    ('lowercase', lambda: get_random_string(string.ascii_lowercase)),
    ('hypen', lambda: get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('digits', lambda: get_random_string(string.digits)),
    ('hexadecimal', lambda: get_random_string(string.hexdigits).lower())
]
@azure('eventhub')
@pytest.mark.parametrize('container_name_category, index', EVENT_HUB_CONTAINERS_NAMES, ids=[i[0] for i in EVENT_HUB_CONTAINERS_NAMES])
def test_object_names_container(sdc_builder, sdc_executor, azure, container_name_category, index):
    """
    Verify that we can respect all the documented containers names possible
    """
    container_name = container_name_category
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    raw_list = [dict(name='Jane Smith', phone=2124050000, zip_code=27023)]
    raw_data = json.dumps(raw_list)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    origin = pipeline_builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON',
                          raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    producer_trash = pipeline_builder.add_stage('Trash')

    azure_event_hub_producer = pipeline_builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='JSON',
                                            event_hub_name=event_hub_name,
                                            json_content='ARRAY_OBJECTS')

    origin >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name,
                                                data_format='JSON',
                                                event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

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
        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = wiretap.output_records[0].field
        assert len(result_records) == 1
        assert result_records[0] == raw_list

    finally:
        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name,
                        azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


EVENT_HUB_BLOB_NAMES = [
    ('minsize', lambda: get_random_string(string.ascii_lowercase, 1)),
    ('maxsize', lambda: get_random_string(string.ascii_lowercase, 1024)),
    ('lowercase', lambda: get_random_string(string.ascii_lowercase)),
    ('uppercase', lambda: get_random_string(string.ascii_uppercase)),
    ('hypen', lambda: get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('period', lambda: get_random_string(string.ascii_lowercase) + '.' + get_random_string(string.ascii_lowercase)),
    ('start_period', lambda: get_random_string(string.ascii_lowercase) + '.' + get_random_string(string.ascii_lowercase)),
    ('forward_slash', lambda: get_random_string(string.ascii_lowercase) + '/' + get_random_string(string.ascii_lowercase)),
    ('start_forward_slash', lambda: get_random_string(string.ascii_lowercase) + '/' + get_random_string(string.ascii_lowercase)),
    ('digits', lambda: get_random_string(string.digits)),
    ('hexadecimal', lambda: get_random_string(string.hexdigits).lower())
]
@azure('eventhub')
@pytest.mark.parametrize('blob_name_category, index', EVENT_HUB_BLOB_NAMES, ids=[i[0] for i in EVENT_HUB_BLOB_NAMES])
def test_object_names_blob(sdc_builder, sdc_executor, azure, blob_name_category, index):
    """
    Verify that we can respect all the documented containers names possible
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = blob_name_category

    raw_list = [dict(name='Jane Smith', phone=2124050000, zip_code=27023)]
    raw_data = json.dumps(raw_list)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    origin = pipeline_builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON',
                          raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    producer_trash = pipeline_builder.add_stage('Trash')

    azure_event_hub_producer = pipeline_builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='JSON',
                                            event_hub_name=event_hub_name,
                                            json_content='ARRAY_OBJECTS')

    origin >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name,
                                                data_format='JSON',
                                                event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

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
        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = wiretap.output_records[0].field
        assert len(result_records) == 1
        assert result_records[0] == raw_list

    finally:
        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name,
                        azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@azure('eventhub')
def test_dataflow_events(sdc_builder, sdc_executor, azure):
    pytest.skip("""Azure EventHub destination doesn't produce events, so we don't need to test dataflow events.""")


@azure('eventhub')
@pytest.mark.parametrize('batch_size', [1, 10])
def test_multiple_batches(sdc_builder, sdc_executor, azure, batch_size):
    """
    Test read multiple batches.
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)
    batches = 3
    number_records = batches * batch_size

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    origin = pipeline_builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    producer_trash = pipeline_builder.add_stage('Trash')

    azure_event_hub_producer = pipeline_builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='JSON',
                                            event_hub_name=event_hub_name)

    origin >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name,
                                                data_format='JSON',
                                                event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

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
        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', number_records, timeout_sec=120)

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = wiretap.output_records
        assert len(result_records) == number_records

    finally:
        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name,
                        azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@stub
@azure('eventhub')
def test_data_format_binary(sdc_builder, sdc_executor, azure, data_type):
    pass


@azure('eventhub')
def test_data_format_json(sdc_builder, sdc_executor, azure):
    """
    Test Azure EventHub origin parses Binary in a variety of data types.
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    raw_list = [dict(name='Jane Smith', phone=2124050000, zip_code=27023)]
    raw_data = json.dumps(raw_list)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    origin = pipeline_builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON',
                          raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    producer_trash = pipeline_builder.add_stage('Trash')

    azure_event_hub_producer = pipeline_builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='JSON',
                                            event_hub_name=event_hub_name,
                                            json_content='ARRAY_OBJECTS')

    origin >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name,
                                                data_format='JSON',
                                                event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

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
        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = wiretap.output_records[0].field
        assert len(result_records) == 1
        assert result_records[0] == raw_list

    finally:
        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name,
                        azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@stub
@azure('eventhub')
def test_data_format_sdc_record(sdc_builder, sdc_executor, azure, data_type):
    pass


@azure('eventhub')
def test_data_format_text(sdc_builder, sdc_executor, azure):
    """
    Test Azure EventHub origin parses Binary in a variety of data types.
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    raw_data = 'Hello World!'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    origin = pipeline_builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='TEXT',
                          raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    producer_trash = pipeline_builder.add_stage('Trash')

    azure_event_hub_producer = pipeline_builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='TEXT',
                                            event_hub_name=event_hub_name)

    origin >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name,
                                                data_format='TEXT',
                                                event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

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
        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = wiretap.output_records[0].field
        assert len(result_records) == 1
        assert result_records['text'] == raw_data

    finally:
        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name,
                        azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@azure('eventhub')
def test_data_format_xml(sdc_builder, sdc_executor, azure):
    """
    Test Azure EventHub origin parses Binary in a variety of data types.
    """
    # Support for XML data format for Azure Event Hub producer is only available for SDC_VERSION >= 3.12
    if Version(sdc_builder.version) < Version("3.12.0"):
        pytest.skip('XML data format for Azure Event Hub Producer not available for sdc_version {sdc_builder.version}.')

    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    raw_data = '{"key":"value"}'
    EXPECTED_XML_OUTPUT = ['<?xml version="1.0" encoding="UTF-8" standalone="no"?>', '<key>value</key>']

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    origin = pipeline_builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON',
                          raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    producer_trash = pipeline_builder.add_stage('Trash')

    azure_event_hub_producer = pipeline_builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='XML',
                                            event_hub_name=event_hub_name)

    origin >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name,
                                                data_format='TEXT',
                                                event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

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
        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = wiretap.output_records
        assert [record.field['text'] for record in result_records] == EXPECTED_XML_OUTPUT

    finally:
        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name,
                        azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@azure('eventhub')
@pytest.mark.parametrize('no_of_threads', [1, 10])
def test_multithreading(sdc_builder, sdc_executor, azure, no_of_threads):
    """
    Test read multiple batches.
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)
    no_of_batches = 3
    no_of_records = no_of_threads * no_of_batches

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    origin = pipeline_builder.add_stage('Dev Data Generator')
    origin.batch_size = 1
    origin.number_of_threads = no_of_threads
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    producer_trash = pipeline_builder.add_stage('Trash')

    azure_event_hub_producer = pipeline_builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='JSON',
                                            event_hub_name=event_hub_name)

    origin >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name,
                                                data_format='JSON',
                                                event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

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
        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', no_of_records, timeout_sec=120)

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = wiretap.output_records
        assert len(result_records) >= no_of_records

    finally:
        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name,
                        azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@azure('eventhub')
def test_push_pull(sdc_builder, sdc_executor, azure):
    """
    We plan to verify that the connector works fine with Dev Raw Data Source and Dev Data Generator, an example of pull
    and push strategies, so as we already verified Dev Raw Data Source, we will use Dev Data Generator here to complete
    the coverage.
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      fields_to_generate=[
                                          {'field': 'stringField', 'type': 'STRING', 'precision': 10, 'scale': 2}])

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    producer_trash = pipeline_builder.add_stage('Trash')

    azure_event_hub_producer = pipeline_builder.add_stage('Azure Event Hub Producer')
    azure_event_hub_producer.set_attributes(data_format='JSON',
                                            event_hub_name=event_hub_name,
                                            json_content='ARRAY_OBJECTS')

    dev_data_generator >> record_deduplicator >> azure_event_hub_producer
    record_deduplicator >> producer_trash

    producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(producer_dest_pipeline)

    # build Event Hub consumer
    builder = sdc_builder.get_pipeline_builder()
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME)
    azure_iot_event_hub_consumer.set_attributes(container_name=container_name,
                                                data_format='JSON',
                                                event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

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
        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'output_record_count', 25, timeout_sec=120)

        sdc_executor.stop_pipeline(producer_dest_pipeline)
        sdc_executor.stop_pipeline(consumer_origin_pipeline, wait=False)

        result_records = wiretap.output_records[0].field
        assert len(result_records) == 1

    finally:
        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name,
                        azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.debug('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)
