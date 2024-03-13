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

import pytest

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

input_data = [{'name': 'Rafa Nadal', 'country': 'Spain'},
              {'name': 'Roger Federer', 'country': 'Switzerland'},
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
            proxy_port=8080
        )
    else:
        websocket_client.set_attributes(
            resource_url='ws://localhost:8080',
        )

    websocket_client >> trash

    client_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(client_pipeline)

    # Server pipeline creation
    pipeline_builder = sdc_builder.get_pipeline_builder()
    websocket_server = pipeline_builder.add_stage('WebSocket Server').set_attributes(websocket_listening_port=8080,
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
        sdc_executor.stop_pipeline(client_pipeline)


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy_credentials': False, 'use_proxy': True},
                                              {'use_proxy_credentials': True, 'use_proxy': True}])
def test_proxy_requires_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass
