# Copyright 2024 StreamSets Inc.
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
from time import sleep

import pytest

from streamsets.testframework.markers import sdc_min_version, websocket

logger = logging.getLogger(__name__)
pytestmark = [websocket, sdc_min_version('5.10.0')]

INPUT_DATA = [{'name': 'Roger Federer', 'country': 'Switzerland'},
              {'name': 'Rafa Nadal', 'country': 'Spain'},
              {'name': 'Pete Sampras', 'country': 'USA'}]


@pytest.mark.parametrize('use_proxy', [True, False])
def test_websocket_client_origin_proxy_support(sdc_builder, sdc_executor, use_proxy):
    """
    Test sending messages between Websocket Client and Server stages with proxy enabled.

    We do so by sending some records with a Websocket Client Origin and checking that the Websocket Server
    correctly receives them. We use two pipelines, one for each websocket stage, defined in the respective
    fixtures.

    The pipelines look like this:

    Websocket Server
        websocket_server >> wiretap

    Websocket Client Origin
        websocket_client >> trash
    """
    # Client pipeline creation
    pipeline_builder = sdc_builder.get_pipeline_builder()
    websocket_client = pipeline_builder.add_stage('WebSocket Client', type='origin').set_attributes(
        headers=[{'key': 'X-SDC-APPLICATION-ID', 'value': 'APPLICATION_ID'}],
        request_data='\n'.join(json.dumps(rec) for rec in INPUT_DATA),
        data_format='JSON',
        use_proxy=use_proxy
    )
    trash = pipeline_builder.add_stage('Trash')

    # Random hostname and port is used for 'resource_url' in case 'use_proxy' is enabled to verify
    # that user provided 'proxy_hostname' and 'proxy_port' values are used to make the connection
    if use_proxy:
        websocket_client.set_attributes(
            resource_url='ws://random:9999',
            proxy_hostname='localhost',
            proxy_port=9090
        )
    else:
        websocket_client.set_attributes(
            resource_url='ws://localhost:9090',
        )

    websocket_client >> trash

    client_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(client_pipeline)

    # Server pipeline creation
    pipeline_builder = sdc_builder.get_pipeline_builder()
    websocket_server = pipeline_builder.add_stage('WebSocket Server').set_attributes(websocket_listening_port=9090,
                                                                                     application_id='APPLICATION_ID',
                                                                                     data_format='JSON')
    wiretap = pipeline_builder.add_wiretap()

    websocket_server >> wiretap.destination

    server_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(server_pipeline)

    try:
        # Start WebSocket Server pipeline.
        logger.info("Starting websocket server pipeline..")
        sdc_executor.start_pipeline(server_pipeline)

        # Start WebSocket Client pipeline.
        logger.info("Starting websocket client origin pipeline..")
        sdc_executor.start_pipeline(client_pipeline)
        sdc_executor.wait_for_pipeline_metric(server_pipeline, 'input_record_count', 3, timeout_sec=120)

        records = wiretap.output_records
        assert 0 != len(records), 'Expected output records, but found none)'
        assert len(records) == 3, 'Expected exactly 3 output records'

        output_data = [{'name': rec.field['name'].value, 'country': rec.field['country'].value}
                       for rec in wiretap.output_records]
        assert sorted(INPUT_DATA, key=lambda x: x['name']) == sorted(output_data, key=lambda x: x['name']), \
            'Mismatch between input and output records'
    finally:
        # Stop the pipeline and verify pipeline's status
        sdc_executor.stop_pipeline(server_pipeline)
        sdc_executor.stop_pipeline(pipeline=client_pipeline, force=True)


@pytest.mark.parametrize('proxy_requires_credentials', [True, False])
def test_proxy_credentials(sdc_builder, sdc_executor, websocket, proxy_requires_credentials):
    """
    Test sending messages between Websocket Client and Websocket Server with proxy enabled and requiring credentials.

    We do so by sending some records with a Websocket Client Origin and checking that the websocket server
    correctly receives them and sends appropriate response.

    The pipelines look like this:

    Websocket Client Origin
        websocket_client >> wiretap
    """
    # Client pipeline creation
    pipeline_builder = sdc_builder.get_pipeline_builder()

    websocket_client = pipeline_builder.add_stage('WebSocket Client', type='origin').set_attributes(
        resource_url=websocket.uri,
        request_data='\n'.join(json.dumps(rec) for rec in INPUT_DATA),
        data_format='JSON',
        send_raw_response=True,
        use_proxy=True,
        proxy_hostname=websocket.proxy_hostname,
        proxy_port=websocket.proxy_port,
        proxy_requires_credentials=proxy_requires_credentials,
        proxy_username=websocket.proxy_username,
        proxy_password=websocket.proxy_password,
    )
    wiretap = pipeline_builder.add_wiretap()

    websocket_client >> wiretap.destination

    client_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(client_pipeline)

    # Start WebSocket Client pipeline.
    logger.info("Starting websocket client origin pipeline..")
    if proxy_requires_credentials:
        try:
            sdc_executor.start_pipeline(client_pipeline)
            sdc_executor.wait_for_pipeline_metric(client_pipeline, "input_record_count", 3)
            result = wiretap.output_records
            output_records = [{k: v for k, v in record.field.items()} for record in result]

            assert len(result) == 3, 'Expected exactly 3 output records'
            assert output_records == INPUT_DATA
        finally:
            sdc_executor.stop_pipeline(client_pipeline, force=True)
    else:
        try:
            sdc_executor.start_pipeline(client_pipeline)
            pytest.fail("Test should not reach here. It should have failed with StartError.")
        except Exception as e:
            assert "WEB_SOCKET_01" in e.message


def test_proxy_invalid_credentials(sdc_builder, sdc_executor, websocket):
    """
    The test is trying to connect to the WebSocket Client Origin Stage, but it displays and error as a consequence that
    the credentials are invalid.

    The pipelines look like this:

    Websocket Client Origin
        websocket_client >> wiretap
    """

    # Invalid credentials, invalid proxy username and password.
    proxy_username = 'Mistborn:TheFinalEmpire'
    proxy_password = 'Kelsier'

    # Client pipeline creation
    pipeline_builder = sdc_builder.get_pipeline_builder()

    websocket_client = pipeline_builder.add_stage('WebSocket Client', type='origin').set_attributes(
        resource_url=websocket.uri,
        request_data=json.dumps(INPUT_DATA),
        data_format='JSON',
        send_raw_response=True,
        use_proxy=True,
        proxy_hostname=websocket.proxy_hostname,
        proxy_port=websocket.proxy_port,
        proxy_requires_credentials=True,
        proxy_username=proxy_username,
        proxy_password=proxy_password,
    )
    wiretap = pipeline_builder.add_wiretap()

    websocket_client >> wiretap.destination

    client_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(client_pipeline)

    # Start WebSocket Client pipeline.
    logger.info("Starting websocket client origin pipeline..")
    try:
        sdc_executor.start_pipeline(client_pipeline)
        pytest.fail("Test should not reach here. It should have failed with StartError.")
    except Exception as e:
        assert "WEB_SOCKET_01" in e.message


def test_proxy_invalid_hostname_and_port(sdc_builder, sdc_executor, websocket):
    """
    The test is trying to connect to the WebSocket Client Origin Stage, but it displays and error as a consequence that
    the hostname and the port are invalid.

    The pipelines look like this:

    Websocket Client Origin
        websocket_client >> wiretap
    """

    # Invalid proxy hostname and port.
    proxy_hostname = 'houseRenoux'
    proxy_port = 1995

    # Client pipeline creation
    pipeline_builder = sdc_builder.get_pipeline_builder()

    websocket_client = pipeline_builder.add_stage('WebSocket Client', type='origin').set_attributes(
        resource_url=websocket.uri,
        request_data=json.dumps(INPUT_DATA),
        data_format='JSON',
        send_raw_response=True,
        use_proxy=True,
        proxy_hostname=proxy_hostname,
        proxy_port=proxy_port,
        proxy_requires_credentials=True,
        proxy_username=websocket.proxy_username,
        proxy_password=websocket.proxy_password,
    )
    wiretap = pipeline_builder.add_wiretap()

    websocket_client >> wiretap.destination

    client_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(client_pipeline)

    # Start WebSocket Client pipeline.
    logger.info("Starting websocket client origin pipeline..")
    try:
        sdc_executor.start_pipeline(client_pipeline)
        pytest.fail("Test should not reach here. It should have failed with StartError.")
    except Exception as e:
        assert "WEB_SOCKET_01" in e.message
