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
from azure import servicebus
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import azure
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

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


@azure('iot')
@pytest.mark.parametrize('input,converter_type,expected', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, azure, input, converter_type, expected):
    device_id = get_random_string(string.ascii_letters, 10)

    try:
        iot_hub = azure.iot_hub
        logger.info('Creating %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        device_info = iot_hub.create_device_id(device_id)
        device_access_key = device_info['authentication']['symmetricKey']['primaryKey']

        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')

        origin = pipeline_builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              raw_data=json.dumps({"value": input}))

        record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
        trash = pipeline_builder.add_stage('Trash')

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

        azure_iot_hub_producer = pipeline_builder.add_stage('Azure IoT Hub Producer')
        azure_iot_hub_producer.set_attributes(data_format='JSON',
                                              device_id=device_id,
                                              json_content='MULTIPLE_OBJECTS',
                                              shared_access_key=device_access_key)

        origin >> converter >> record_deduplicator >> azure_iot_hub_producer
        record_deduplicator >> trash

        producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)

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
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_output_records_count(1, timeout_sec=120)
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        # read data from Azure Service Bus Topic and assert
        logger.info('Reading data from Azure Service Bus topic %s ...', topic_name)
        messages = json.loads(sb_service.receive_subscription_message(topic_name=topic_name,
                                                                      subscription_name=subscriber_id,
                                                                      peek_lock=False,
                                                                      timeout=10).body.decode().strip())
        assert messages['value'] == expected

    finally:
        logger.info('Deleting %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.delete_rule(topic_name, subscriber_id, servicebus.DEFAULT_RULE_NAME)
        sb_service.delete_subscription(topic_name, subscriber_id)
        logger.info('Deleting %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        iot_hub.delete_device_id(device_id)


# Referencehttps://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry
IOT_HUB_DEVICE_NAMES = [
    ('minsize', get_random_string(string.ascii_lowercase, 1)),
    ('maxsize', get_random_string(string.ascii_lowercase, 128)),
    ('lowercase', get_random_string(string.ascii_lowercase)),
    ('uppercase', get_random_string(string.ascii_uppercase)),
    ('dot', get_random_string(string.ascii_lowercase, 2) + '.' + get_random_string(string.ascii_lowercase, 2)),
    ('coma', get_random_string(string.ascii_lowercase, 2) + ',' + get_random_string(string.ascii_lowercase, 2)),
    ('hyphen', get_random_string(string.ascii_lowercase, 2) + '-' + get_random_string(string.ascii_lowercase, 2)),
    ('underscored', get_random_string(string.ascii_lowercase, 2) + '_' + get_random_string(string.ascii_lowercase, 2)),
    ('asterisk', get_random_string(string.ascii_lowercase, 2) + '*' + get_random_string(string.ascii_lowercase, 2)),
    ('exclamation', get_random_string(string.ascii_lowercase, 2) + '!' + get_random_string(string.ascii_lowercase, 2)),
    ('parentesis', get_random_string(string.ascii_lowercase, 2) + '(' + get_random_string(string.ascii_lowercase, 2)),
    ('colon', get_random_string(string.ascii_lowercase, 2) + ':' + get_random_string(string.ascii_lowercase, 2)),
    ('equal', get_random_string(string.ascii_lowercase, 2) + '=' + get_random_string(string.ascii_lowercase, 2)),
    ('atsign', get_random_string(string.ascii_lowercase, 2) + '@' + get_random_string(string.ascii_lowercase, 2)),
    ('dollar', get_random_string(string.ascii_lowercase, 2) + '$' + get_random_string(string.ascii_lowercase, 2)),
    ('digits', get_random_string(string.digits)),
    ('hexadecimal', get_random_string(string.hexdigits).lower())
]
@azure('iot')
@pytest.mark.parametrize('device_name_category, device_name', IOT_HUB_DEVICE_NAMES,
                         ids=[i[0] for i in IOT_HUB_DEVICE_NAMES])
def test_object_names_device(sdc_builder, sdc_executor, azure, device_name_category, device_name):
    """
    Verify that we can respect all the documented containers names possible
    """
    device_id = device_name
    raw_records = [{'Body': f'Hello {msg}'} for msg in range(10)]

    try:
        iot_hub = azure.iot_hub
        logger.info('Creating %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        device_info = iot_hub.create_device_id(device_id)
        device_access_key = device_info['authentication']['symmetricKey']['primaryKey']

        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')

        origin = pipeline_builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              raw_data=json.dumps(raw_records))

        record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
        trash = pipeline_builder.add_stage('Trash')

        azure_iot_hub_producer = pipeline_builder.add_stage('Azure IoT Hub Producer')
        azure_iot_hub_producer.set_attributes(data_format='JSON',
                                              device_id=device_id,
                                              json_content='MULTIPLE_OBJECTS',
                                              shared_access_key=device_access_key)

        origin >> record_deduplicator >> azure_iot_hub_producer
        record_deduplicator >> trash

        producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)

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
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_output_records_count(1, timeout_sec=120)
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        # read data from Azure Service Bus Topic and assert
        logger.info('Reading data from Azure Service Bus topic %s ...', topic_name)
        messages = json.loads(sb_service.receive_subscription_message(topic_name=topic_name,
                                                                      subscription_name=subscriber_id,
                                                                      peek_lock=False,
                                                                      timeout=10).body.decode().strip())
        assert messages == raw_records

    finally:
        logger.info('Deleting %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.delete_rule(topic_name, subscriber_id, servicebus.DEFAULT_RULE_NAME)
        sb_service.delete_subscription(topic_name, subscriber_id)
        logger.info('Deleting %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        iot_hub.delete_device_id(device_id)


@azure('iot')
def test_dataflow_events(sdc_builder, sdc_executor, azure):
    pytest.skip("""Azure EventHub destination doesn't produce events, so we don't need to test dataflow events.""")


@azure('iot')
@pytest.mark.parametrize('batch_size', [1, 10])
def test_multiple_batches(sdc_builder, sdc_executor, azure, batch_size):
    """
    Test read multiple batches.
    """
    device_id = get_random_string(string.ascii_letters, 10)
    batches = 3
    no_of_records = batches * batch_size

    try:
        iot_hub = azure.iot_hub
        logger.info('Creating %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        device_info = iot_hub.create_device_id(device_id)
        device_access_key = device_info['authentication']['symmetricKey']['primaryKey']

        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')

        origin = pipeline_builder.add_stage('Dev Data Generator')
        origin.batch_size = batch_size
        origin.fields_to_generate = [{
            "type": "LONG_SEQUENCE",
            "field": "seq"
        }]

        record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
        trash = pipeline_builder.add_stage('Trash')

        azure_iot_hub_producer = pipeline_builder.add_stage('Azure IoT Hub Producer')
        azure_iot_hub_producer.set_attributes(data_format='JSON',
                                              device_id=device_id,
                                              json_content='MULTIPLE_OBJECTS',
                                              shared_access_key=device_access_key)

        origin >> record_deduplicator >> azure_iot_hub_producer
        record_deduplicator >> trash

        producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)

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
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_batch_count(batches)
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        # read data from Azure Service Bus Topic and assert
        logger.info('Reading data from Azure Service Bus topic %s ...', topic_name)
        messages = [json.loads(sb_service.receive_subscription_message(topic_name=topic_name,
                                                                       subscription_name=subscriber_id,
                                                                       peek_lock=False,
                                                                       timeout=10).body.decode().strip())
                    for _ in range(no_of_records)]
        assert len(messages) == no_of_records

    finally:
        logger.info('Deleting %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.delete_rule(topic_name, subscriber_id, servicebus.DEFAULT_RULE_NAME)
        sb_service.delete_subscription(topic_name, subscriber_id)
        logger.info('Deleting %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        iot_hub.delete_device_id(device_id)


@stub
@azure('iot')
def test_data_format_binary(sdc_builder, sdc_executor, azure, data_type):
    pass


@azure('iot')
def test_data_format_json(sdc_builder, sdc_executor, azure):
    """
    Test Azure IoT Hub origin parses JSON in a variety of data types.
    """
    device_id = get_random_string(string.ascii_letters, 10)
    raw_records = [{'Body': f'Hello {msg}'} for msg in range(10)]

    try:
        iot_hub = azure.iot_hub
        logger.info('Creating %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        device_info = iot_hub.create_device_id(device_id)
        device_access_key = device_info['authentication']['symmetricKey']['primaryKey']

        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')

        origin = pipeline_builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              raw_data=json.dumps(raw_records))

        record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
        trash = pipeline_builder.add_stage('Trash')

        azure_iot_hub_producer = pipeline_builder.add_stage('Azure IoT Hub Producer')
        azure_iot_hub_producer.set_attributes(data_format='JSON',
                                              device_id=device_id,
                                              json_content='MULTIPLE_OBJECTS',
                                              shared_access_key=device_access_key)

        origin >> record_deduplicator >> azure_iot_hub_producer
        record_deduplicator >> trash

        producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)

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
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_output_records_count(1, timeout_sec=120)
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        # read data from Azure Service Bus Topic and assert
        logger.info('Reading data from Azure Service Bus topic %s ...', topic_name)
        messages = json.loads(sb_service.receive_subscription_message(topic_name=topic_name,
                                                                      subscription_name=subscriber_id,
                                                                      peek_lock=False,
                                                                      timeout=10).body.decode().strip())
        assert messages == raw_records

    finally:
        logger.info('Deleting %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.delete_rule(topic_name, subscriber_id, servicebus.DEFAULT_RULE_NAME)
        sb_service.delete_subscription(topic_name, subscriber_id)
        logger.info('Deleting %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        iot_hub.delete_device_id(device_id)


@stub
@azure('iot')
def test_data_format_sdc_record(sdc_builder, sdc_executor, azure, data_type):
    pass


@azure('iot')
def test_data_format_text(sdc_builder, sdc_executor, azure):
    """
    Test Azure EventHub origin parses Text in a variety of data types.
    """
    device_id = get_random_string(string.ascii_letters, 10)
    raw_record = 'Hello World!'

    try:
        iot_hub = azure.iot_hub
        logger.info('Creating %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        device_info = iot_hub.create_device_id(device_id)
        device_access_key = device_info['authentication']['symmetricKey']['primaryKey']

        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')

        origin = pipeline_builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='TEXT',
                              raw_data=raw_record)

        record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
        trash = pipeline_builder.add_stage('Trash')

        azure_iot_hub_producer = pipeline_builder.add_stage('Azure IoT Hub Producer')
        azure_iot_hub_producer.set_attributes(data_format='TEXT',
                                              device_id=device_id,
                                              shared_access_key=device_access_key)

        origin >> record_deduplicator >> azure_iot_hub_producer
        record_deduplicator >> trash

        producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)

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
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_output_records_count(1, timeout_sec=120)
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        # read data from Azure Service Bus Topic and assert
        logger.info('Reading data from Azure Service Bus topic %s ...', topic_name)
        messages = sb_service.receive_subscription_message(topic_name=topic_name,
                                                           subscription_name=subscriber_id,
                                                           peek_lock=False,
                                                           timeout=10).body.decode("utf-8").rstrip()
        assert messages == raw_record

    finally:
        logger.info('Deleting %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.delete_rule(topic_name, subscriber_id, servicebus.DEFAULT_RULE_NAME)
        sb_service.delete_subscription(topic_name, subscriber_id)
        logger.info('Deleting %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        iot_hub.delete_device_id(device_id)


@azure('iot')
def test_data_format_xml(sdc_builder, sdc_executor, azure):
    """
    Test Azure EventHub origin parses XML in a variety of data types.
    """
    device_id = get_random_string(string.ascii_letters, 10)
    raw_data = '{"key":"value"}'
    EXPECTED_XML_OUTPUT = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<key>value</key>'

    try:
        iot_hub = azure.iot_hub
        logger.info('Creating %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        device_info = iot_hub.create_device_id(device_id)
        device_access_key = device_info['authentication']['symmetricKey']['primaryKey']

        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')

        origin = pipeline_builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              raw_data=raw_data)

        record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
        trash = pipeline_builder.add_stage('Trash')

        azure_iot_hub_producer = pipeline_builder.add_stage('Azure IoT Hub Producer')
        azure_iot_hub_producer.set_attributes(data_format='XML',
                                              device_id=device_id,
                                              shared_access_key=device_access_key)

        origin >> record_deduplicator >> azure_iot_hub_producer
        record_deduplicator >> trash

        producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)

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
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_output_records_count(1, timeout_sec=120)
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        # read data from Azure Service Bus Topic and assert
        logger.info('Reading data from Azure Service Bus topic %s ...', topic_name)
        messages = sb_service.receive_subscription_message(topic_name=topic_name,
                                                           subscription_name=subscriber_id,
                                                           peek_lock=False,
                                                           timeout=10).body.decode().rstrip()
        assert messages == EXPECTED_XML_OUTPUT

    finally:
        logger.info('Deleting %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.delete_rule(topic_name, subscriber_id, servicebus.DEFAULT_RULE_NAME)
        sb_service.delete_subscription(topic_name, subscriber_id)
        logger.info('Deleting %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        iot_hub.delete_device_id(device_id)


@stub
@azure('iot')
def test_multithreading(sdc_builder, sdc_executor, azure):
    pass


@azure('iot')
def test_push_pull(sdc_builder, sdc_executor, azure):
    """
    We plan to verify that the connector works fine with Dev Raw Data Source and Dev Data Generator, an example of pull
    and push strategies, so as we already verified Dev Raw Data Source, we will use Dev Data Generator here to complete
    the coverage.
    """
    device_id = get_random_string(string.ascii_letters, 10)

    try:
        iot_hub = azure.iot_hub
        logger.info('Creating %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        device_info = iot_hub.create_device_id(device_id)
        device_access_key = device_info['authentication']['symmetricKey']['primaryKey']

        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')

        dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
        dev_data_generator.set_attributes(batch_size=1,
                                          fields_to_generate=[
                                              {'field': 'stringField', 'type': 'STRING', 'precision': 10, 'scale': 2}])

        record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
        trash = pipeline_builder.add_stage('Trash')

        azure_iot_hub_producer = pipeline_builder.add_stage('Azure IoT Hub Producer')
        azure_iot_hub_producer.set_attributes(data_format='JSON',
                                              device_id=device_id,
                                              json_content='MULTIPLE_OBJECTS',
                                              shared_access_key=device_access_key)

        dev_data_generator >> record_deduplicator >> azure_iot_hub_producer
        record_deduplicator >> trash

        producer_dest_pipeline = pipeline_builder.build().configure_for_environment(azure)

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
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_output_records_count(25, timeout_sec=120)
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        # read data from Azure Service Bus Topic and assert
        logger.info('Reading data from Azure Service Bus topic %s ...', topic_name)
        messages = json.loads(sb_service.receive_subscription_message(topic_name=topic_name,
                                                                      subscription_name=subscriber_id,
                                                                      peek_lock=False,
                                                                      timeout=10).body.decode().strip())
        assert len(messages) == 1

    finally:
        logger.info('Deleting %s Service Bus subscriber on topic %s', subscriber_id, topic_name)
        sb_service.delete_rule(topic_name, subscriber_id, servicebus.DEFAULT_RULE_NAME)
        sb_service.delete_subscription(topic_name, subscriber_id)
        logger.info('Deleting %s IoT Hub device on %s IoT Hub', device_id, iot_hub.namespace)
        iot_hub.delete_device_id(device_id)
