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

input_data = [{'name': 'Roger Federer', 'country': 'Switzerland'},
              {'name': 'Rafa Nadal', 'country': 'Spain'},
              {'name': 'Pete Sampras', 'country': 'USA'}]


@sdc_min_version("5.10.0")
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
        request_data='\n'.join(json.dumps(rec) for rec in input_data),
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
        assert sorted(input_data, key=lambda x: x['name']) == sorted(output_data, key=lambda x: x['name']), \
            'Mismatch between input and output records'
    finally:
        # Stop the pipeline and verify pipeline's status
        sdc_executor.stop_pipeline(server_pipeline)
        sdc_executor.stop_pipeline(pipeline=client_pipeline, force=True)


@websocket
@sdc_min_version("5.10.0")
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
        request_data=json.dumps(input_data[0]),
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
    try:
        sdc_executor.start_pipeline(client_pipeline)
        # We wait three seconds - One for sending request data and then some buffer time
        sleep(3)
    finally:
        sdc_executor.stop_pipeline(pipeline=client_pipeline, force=True)

    result = wiretap.output_records
    if proxy_requires_credentials:
        output_records = [{k: v for k, v in record.field.items()} for record in result]

        assert len(result) == 2, 'Expected exactly 2 output records'
        assert output_records[0] == {'name': 'Roger Federer', 'country': 'Switzerland'}
    else:
        assert len(result) == 0, 'Expected exactly 0 output record'
