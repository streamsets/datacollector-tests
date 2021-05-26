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
from streamsets.testframework.markers import azure
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# To workaround the stage label tweak introduced in 3.0.1.0 (SDC-8077), we use the
# Azure IoT/Event Hub Consumer stage's full name in tests.
AZURE_IOT_EVENT_HUB_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_eventhubs_EventHubConsumerDSource'


@azure('eventhub')
def test_data_types(sdc_builder, sdc_executor, azure):
    pytest.skip("""Azure EventHub origin don't talk to a structured system, so we don't need to test each data type.""")


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

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME).set_attributes(
        container_name=container_name,
        data_format='JSON',
        event_hub_name=event_hub_name)

    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        send_records = [{'Body': f'Event {msg}'} for msg in range(10)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records))

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        results = [{key: value for key, value in record.items()} for record in wiretap.output_records[0].field]
        assert results == send_records
    finally:
        try:
            if sdc_executor.get_pipeline_status(consumer_origin_pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(consumer_origin_pipeline)
        except Exception as err:
            logger.error('Could not stop pipeline. Reason found: %s', err)

        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
            event_hub_exists = True
            while event_hub_exists:
                eh_service_bus.delete_event_hub(event_hub_name)
                try:
                    eh_service_bus.get_event_hub(event_hub_name)
                except Exception:
                    event_hub_exists = False
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
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

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME).set_attributes(
        container_name=container_name,
        data_format='JSON',
        event_hub_name=event_hub_name)

    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        send_records = [{'Body': f'Event {msg}'} for msg in range(10)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records))

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        results = [{key: value for key, value in record.items()} for record in wiretap.output_records[0].field]
        assert results == send_records
    finally:
        try:
            if sdc_executor.get_pipeline_status(consumer_origin_pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(consumer_origin_pipeline)
        except Exception as err:
            logger.error('Could not stop pipeline. Reason found: %s', err)

        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
            eh_service_bus.delete_event_hub(event_hub_name)
        except Exception as err:
            event_hub_exists = True
            while event_hub_exists:
                eh_service_bus.delete_event_hub(event_hub_name)
                try:
                    eh_service_bus.get_event_hub(event_hub_name)
                except Exception:
                    event_hub_exists = False

        try:
            logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@azure('eventhub')
def test_dataflow_events(sdc_builder, sdc_executor, azure):
    pytest.skip("""Azure EventHub origin doesn't produce events, so we don't need to test dataflow events.""")


@azure('eventhub')
@pytest.mark.parametrize('number_batches', [1, 15, 20])
def test_multiple_batch(sdc_builder, sdc_executor, azure, number_batches):
    """
    Test read multiple batches.
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    number_records = 1000

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME).set_attributes(
        container_name=container_name,
        data_format='JSON',
        event_hub_name=event_hub_name)

    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        total_send_records = []
        for i in range(0, number_records):
            send_records = [{'Body': get_random_string(string.ascii_lowercase, 10)}]
            total_send_records.append(send_records)
            eh_service_bus.send_event(event_hub_name, json.dumps(send_records))

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'data_batch_count', number_batches,
                                              timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        records = [record.field for record in wiretap.output_records]
        items = [r[0].items() for r in records]
        assert len(items) >= number_batches

        batches = wiretap.batches
        len_records_in_batches = 0
        for bat in batches:
            len_records_in_batches = len_records_in_batches + len(bat)
        assert len(items) == len_records_in_batches

    finally:
        try:
            if sdc_executor.get_pipeline_status(consumer_origin_pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(consumer_origin_pipeline)
        except Exception as err:
            logger.error('Could not stop pipeline. Reason found: %s', err)

        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
            event_hub_exists = True
            while event_hub_exists:
                eh_service_bus.delete_event_hub(event_hub_name)
                try:
                    eh_service_bus.get_event_hub(event_hub_name)
                except Exception:
                    event_hub_exists = False
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@azure('eventhub')
def test_data_format_binary(sdc_builder, sdc_executor, azure):
    """
    Test Azure EventHub origin parses Binary in a variety of data types.
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    MESSAGES = [b'message 1', b'message 2']
    EXPECTED_OUTPUT = "[b'message 1', b'message 2']"

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = _get_azure_eventhub_consumer_stage(builder, container_name, event_hub_name,  'BINARY')

    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        eh_service_bus.send_event(event_hub_name, MESSAGES)

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        results = wiretap.output_records[0].field.value.decode()
        assert results == EXPECTED_OUTPUT
    finally:
        try:
            if sdc_executor.get_pipeline_status(consumer_origin_pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(consumer_origin_pipeline)
        except Exception as err:
            logger.error('Could not stop pipeline. Reason found: %s', err)

        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
            event_hub_exists = True
            while event_hub_exists:
                eh_service_bus.delete_event_hub(event_hub_name)
                try:
                    eh_service_bus.get_event_hub(event_hub_name)
                except Exception:
                    event_hub_exists = False
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@azure('eventhub')
@pytest.mark.parametrize('data_type',
                         ['ARRAY',
                          'ARRAY_OF_OBJECTS',
                          'OBJECT'
                         ])
def test_data_format_json(sdc_builder, sdc_executor, azure, data_type):
    """Azure EventHub origin parses JSON in a variety of data types."""
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    # We map data_type to input data as well as the expected output.
    DATA_TYPE = {'ARRAY': ['Alex', 'Xavi'],
                 'ARRAY_OF_OBJECTS': [{'Alex': 'Developer'}, {'Xavi': 'Developer'}],
                 'OBJECT': {'Alex': 'Developer', 'Xavi': 'Developer'}}
    expected_output = DATA_TYPE[data_type]

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = _get_azure_eventhub_consumer_stage(builder, container_name, event_hub_name,  'JSON')

    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        eh_service_bus.send_event(event_hub_name, json.dumps(DATA_TYPE[data_type]))

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        assert [record.field for record in wiretap.output_records] == [expected_output]
    finally:
        try:
            if sdc_executor.get_pipeline_status(consumer_origin_pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(consumer_origin_pipeline)
        except Exception as err:
            logger.error('Could not stop pipeline. Reason found: %s', err)

        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
            event_hub_exists = True
            while event_hub_exists:
                eh_service_bus.delete_event_hub(event_hub_name)
                try:
                    eh_service_bus.get_event_hub(event_hub_name)
                except Exception:
                    event_hub_exists = False
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@azure('eventhub')
def test_data_format_text(sdc_builder, sdc_executor, azure):
    """Azure EventHub origin parses Text in a variety of data types."""
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    MESSAGE = 'Hello World!'
    EXPECTED = {'text': 'Hello World!'}

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = _get_azure_eventhub_consumer_stage(builder, container_name, event_hub_name,  'TEXT')

    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        eh_service_bus.send_event(event_hub_name, MESSAGE)

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        results = wiretap.output_records[0].field
        assert results == EXPECTED
    finally:
        try:
            if sdc_executor.get_pipeline_status(consumer_origin_pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(consumer_origin_pipeline)
        except Exception as err:
            logger.error('Could not stop pipeline. Reason found: %s', err)

        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
            event_hub_exists = True
            while event_hub_exists:
                eh_service_bus.delete_event_hub(event_hub_name)
                try:
                    eh_service_bus.get_event_hub(event_hub_name)
                except Exception:
                    event_hub_exists = False
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@stub
@azure('eventhub')
def test_data_format_sdc_record(sdc_builder, sdc_executor, azure, data_type):
    pass


def _get_azure_eventhub_consumer_stage(builder, container_name, event_hub_name, data_format):
    """Create and return a Azure EventHub origin stage depending on execution mode for the pipeline."""
    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME).set_attributes(
                                                     container_name=container_name,
                                                     event_hub_name=event_hub_name)
    azure_iot_event_hub_consumer.data_format = data_format

    return azure_iot_event_hub_consumer


@azure('eventhub')
@pytest.mark.parametrize('max_threads', [1, 3])
def test_multithreading(sdc_builder, sdc_executor, azure, max_threads):
    """
    Test that using multithreaded pipeline we can start our pipeline multiple times adding more objects in between
    without reading any duplicated record neither missing them.
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME).set_attributes(
        container_name=container_name,
        data_format='JSON',
        event_hub_name=event_hub_name,
        max_threads=max_threads)

    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        send_records = [{'Body': f'Event {msg}'} for msg in range(10)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records))

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        results = [{key: value for key, value in record.items()} for record in wiretap.output_records[0].field]
        assert len(results) == 10
        assert results == send_records
    finally:
        try:
            if sdc_executor.get_pipeline_status(consumer_origin_pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(consumer_origin_pipeline)
        except Exception as err:
            logger.error('Could not stop pipeline. Reason found: %s', err)

        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
            event_hub_exists = True
            while event_hub_exists:
                eh_service_bus.delete_event_hub(event_hub_name)
                try:
                    eh_service_bus.get_event_hub(event_hub_name)
                except Exception:
                    event_hub_exists = False
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)


@azure('eventhub')
def test_resume_offset(sdc_builder, sdc_executor, azure):
    """
    Test that we can start our pipeline multiple times without reading any duplicated record neither missing them.
    """
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME).set_attributes(
        container_name=container_name,
        data_format='JSON',
        event_hub_name=event_hub_name)

    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating container %s on storage account %s', container_name, azure.storage.account_name)
        assert azure.storage.create_blob_container(container_name)

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        send_records = [{'Body': f'Event {msg}'} for msg in range(10)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records))

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        first_iteration_records = wiretap.output_records[0].field

        assert len(first_iteration_records) != 0
        results = [{key: value for key, value in record.items()} for record in first_iteration_records]
        assert results == send_records

        wiretap.reset()

        # Try adding more data and resuming from the offset
        send_records2 = [{'Body': f'Event {msg}'} for msg in range(10, 20)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records2))

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        second_iteration_records = wiretap.output_records[0].field
        assert len(second_iteration_records) != 0
        results = [{key: value for key, value in record.items()} for record in second_iteration_records]
        assert results == send_records2

        assert len(second_iteration_records) + len(first_iteration_records) == len(send_records) + len(send_records2)

    finally:
        try:
            if sdc_executor.get_pipeline_status(consumer_origin_pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(consumer_origin_pipeline)
        except Exception as err:
            logger.error('Could not stop pipeline. Reason found: %s', err)

        try:
            logger.info('Deleting event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
            event_hub_exists = True
            while event_hub_exists:
                eh_service_bus.delete_event_hub(event_hub_name)
                try:
                    eh_service_bus.get_event_hub(event_hub_name)
                except Exception:
                    event_hub_exists = False
        except Exception as err:
            logger.error('Failure deleting event hub %s. Reason found: %s', event_hub_name, err)

        try:
            logger.info('Deleting container %s on storage account %s', container_name, azure.storage.account_name)
            azure.storage.delete_blob_container(container_name)
        except Exception as err:
            logger.error('Failure deleting container %s. Reason found: %s', container_name, err)
