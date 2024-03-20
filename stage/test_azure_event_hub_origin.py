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


import json
import logging
import string

import pytest
from streamsets.sdk.exceptions import StartError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

from .utils.utils_azure import create_blob_container

logger = logging.getLogger(__name__)

# To workaround the stage label tweak introduced in 3.0.1.0 (SDC-8077), we use the
# Azure IoT/Event Hub Consumer stage's full name in tests.
AZURE_IOT_EVENT_HUB_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_eventhubs_EventHubConsumerDSource'


@azure('eventhub')
def test_azure_event_hub_consumer_invalid_config(sdc_builder, sdc_executor):
    """Verify that the Azure Event Hub Consumer fails for invalid config."""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = pipeline_builder.add_stage('Azure IoT/Event Hub Consumer')
    azure_iot_event_hub_consumer.set_attributes(data_format='JSON',
                                                shared_access_policy_name='inValidSasKeyName',
                                                connection_string_key='inValidSas',
                                                container_name='inValidContainerName',
                                                event_hub_name='inValidEventHub',
                                                namespace_name='inValidNamespace',
                                                storage_account_key='inValidStorageAccountKey',
                                                storage_account_name='inValidStorageAccountName')
    trash = pipeline_builder.add_stage('Trash')

    azure_iot_event_hub_consumer >> trash
    pipeline = pipeline_builder.build()
    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        pytest.fail("Test should not reach here. It should have failed with StartError.")
    except Exception as e:
        assert "EVENT_HUB_02" in e.message


@azure('eventhub')
@sdc_min_version('2.7.1.0')
@pytest.mark.parametrize('use_websockets', [True, False])
def test_azure_event_hub_consumer(sdc_builder, sdc_executor, azure, use_websockets):
    """Test for Azure IoT/Event Hub consumer origin stage. We do so by publishing data to a test event hub Azure client
    and having a pipeline which reads that data using Azure IoT/Event Hub consumer origin stage. Data is then asserted
    for what is published at Azure client and what we read from wiretap. The pipeline looks like:

    azure_iot_event_hub_consumer >> wiretap
    """
    # Azure container names are lowercased. Ref. http://tinyurl.com/ya9y9mm6
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()

    if use_websockets and Version(sdc_builder.version) < Version("3.21.0"):
        pytest.skip(
            'AMQP over WebSockets for Azure Event Hub Consumer not available for sdc_version {sdc_builder.version}.')

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME).set_attributes(
        container_name=container_name,
        data_format='JSON',
        event_hub_name=event_hub_name)

    # We already have proper skip logic above based on version and this new
    # argument. We can however set it only on the right version and hence doing it
    # conditionally.
    if use_websockets:
        azure_iot_event_hub_consumer.use_amqp_over_websockets = use_websockets

    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='Azure Event Hub Consumer pipeline').configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    create_blob_container(azure, container_name)

    try:
        eh_service_bus = azure.event_hubs.service_bus

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


@azure('eventhub')
@sdc_min_version('2.7.1.0')
def test_azure_event_hub_consumer_resume_offset(sdc_builder, sdc_executor, azure):
    """Test for Azure IoT/Event Hub consumer origin stage. We do so by publishing data to a test event hub Azure client
    and having a pipeline which reads that data using Azure IoT/Event Hub consumer origin stage. Data is then asserted
    for what is published at Azure client and what we read the data from wiretap. We then create more data, restart
    the pipeline, reset the wiretap to ensure that the stage properly resumes from where the offset left off.
    The pipeline looks like:

    azure_iot_event_hub_consumer >> trash
    """
    # Azure container names are lowercased. Ref. http://tinyurl.com/ya9y9mm6
    container_name = get_random_string(string.ascii_lowercase, 10)
    event_hub_name = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = builder.add_stage(name=AZURE_IOT_EVENT_HUB_STAGE_NAME).set_attributes(
        container_name=container_name,
        data_format='JSON',
        event_hub_name=event_hub_name)
    wiretap = builder.add_wiretap()

    azure_iot_event_hub_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='Azure Event Hub Consumer pipeline').configure_for_environment(azure)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    create_blob_container(azure, container_name)

    try:
        eh_service_bus = azure.event_hubs.service_bus

        logger.info('Creating event hub %s under event hub namespace %s', event_hub_name, azure.event_hubs.namespace)
        assert eh_service_bus.create_event_hub(event_hub_name)

        send_records = [{'Body': f'Event {msg}'} for msg in range(10)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records))

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        result_record = wiretap.output_records[0].field
        results = [{key: value for key, value in record.items()} for record in result_record]
        assert results == send_records

        wiretap.reset()

        # Try adding more data and resuming from the offset
        send_records2 = [{'Body': f'Event {msg}'} for msg in range(10, 20)]
        eh_service_bus.send_event(event_hub_name, json.dumps(send_records2))

        sdc_executor.start_pipeline(consumer_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_origin_pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        result_record = wiretap.output_records[0].field
        results = [{key: value for key, value in record.items()} for record in result_record]
        assert results == send_records2
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
@pytest.mark.parametrize('include_plain_text_credentials', [True, False])
def test_azure_event_hub_consumer_export(sdc_builder, sdc_executor, include_plain_text_credentials):
    """Verify that the Azure IoT/Event Hub Consumer includes/masks sensitive fields correctly."""
    EXPECTED_CONNECTION_STRING_KEY = 'Connection String Key' if include_plain_text_credentials else ''
    EXPECTED_STORAGE_ACCOUNT_KEY = 'Storage Account Key' if include_plain_text_credentials else ''

    pipeline_builder = sdc_builder.get_pipeline_builder()

    azure_iot_event_hub_consumer = pipeline_builder.add_stage('Azure IoT/Event Hub Consumer').set_attributes(
        connection_string_key='Connection String Key',
        container_name='Container Name',
        data_format='JSON',
        event_hub_name='Event Hub Name',
        namespace_name='Namespace Name',
        shared_access_policy_name='Shared Access Policy Name',
        storage_account_key='Storage Account Key',
        storage_account_name='Storage Account Name')

    trash = pipeline_builder.add_stage('Trash')

    azure_iot_event_hub_consumer >> trash
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    exported_json = sdc_executor.export_pipeline(pipeline,
                                                 include_plain_text_credentials=include_plain_text_credentials)

    # After exporting the pipeline, we import it into a PipelineBuilder to make accessing attributes easier.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.import_pipeline(pipeline=exported_json)
    azure_iot_event_hub_consumer = pipeline_builder.build().origin_stage

    assert azure_iot_event_hub_consumer.connection_string_key == EXPECTED_CONNECTION_STRING_KEY
    assert azure_iot_event_hub_consumer.storage_account_key == EXPECTED_STORAGE_ACCOUNT_KEY
