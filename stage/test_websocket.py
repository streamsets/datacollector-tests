# Copyright 2017 StreamSets Inc.
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
from collections import namedtuple

import pytest


logger = logging.getLogger(__name__)

WEBSOCKETS_KEYSTORE_FILE_PATH = 'resources/websocket/websocket_keystore.p12'
WEBSOCKETS_TRUSTSTORE_FILE_PATH = 'resources/websocket/websocket_truststore.jks'
OUTPUT_DIR_PATH = '/tmp/websocket/'


@pytest.fixture(scope='module')
def websocket_server_pipeline(sdc_builder, sdc_executor):
    """Create a websocket server pipeline: websocket_server >> expression_evaluator >> trash

    Several pipeline parameters are exposed for further customization:
    - port: sets the websocket_server.port parameter (default: 8080).
    - appId: sets the websocket_server.application_id parameter (default: 'test').
    - use_tls: sets the websocket_server.use_tls parameter (default: False).

    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    websocket_server = pipeline_builder.add_stage('WebSocket Server')
    websocket_server.set_attributes(websocket_listening_port='${port}',
                                    application_id='${appId}',
                                    data_format='JSON',
                                    use_tls='${use_tls}',
                                    keystore_file=WEBSOCKETS_KEYSTORE_FILE_PATH,
                                    keystore_type='PKCS12',
                                    keystore_password='streamsets')

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    trash = pipeline_builder.add_stage('Trash')

    websocket_server >> expression_evaluator >> trash
    pipeline = pipeline_builder.build()
    pipeline.add_parameters(port=8080, appId='test', use_tls=False)
    sdc_executor.add_pipeline(pipeline)

    # Yield a namedtuple so that we can access instance names of the stages within the test.
    Pipeline = namedtuple('Pipeline', ['pipeline', 'websocket_server', 'expression_evaluator'])
    yield Pipeline(pipeline, websocket_server, expression_evaluator)


@pytest.fixture(scope='module')
def websocket_client_pipeline(sdc_builder, sdc_executor):
    """Create a websocket client pipeline: dev_raw_ds >> javascript_evaluator >> websocket_client

    Several pipeline parameters are exposed for further customization:
    - input_data: sets the dev_raw_ds.raw_data parameter (default: '{"f1": "abc"}{"f1": "xyz"}').
    - stop_after_first_batch: sets the dev_raw_ds.stop_after_first_batch parameter (default: False).
    - url: sets the websocket_client.resource_url parameter (default: 'ws://localhost:8080').
    - appId: sets the X-SDC-APPLICATION-ID header value required by the Websocket server (default: 'test').
    - use_tls: sets the websocket_client.use_tls parameter (default: False).

    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_ds = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_ds.set_attributes(raw_data='${input_data}',
                              data_format='JSON',
                              stop_after_first_batch='${stop_after_first_batch}')

    javascript_evaluator = pipeline_builder.add_stage('JavaScript Evaluator')

    websocket_client = pipeline_builder.add_stage('WebSocket Client', type='destination')
    websocket_client.set_attributes(resource_url='${url}',
                                    headers=[{'key': 'X-SDC-APPLICATION-ID', 'value': '${appId}'}],
                                    use_tls='${use_tls}',
                                    truststore_file=WEBSOCKETS_TRUSTSTORE_FILE_PATH,
                                    truststore_type='JKS',
                                    truststore_password='streamsets')

    dev_raw_ds >> javascript_evaluator >> websocket_client
    pipeline = pipeline_builder.build()
    pipeline.add_parameters(input_data='{"f1": "abc"}{"f1": "xyz"}', stop_after_first_batch=False,
                            url='ws://localhost:8080', appId='test', use_tls=False)
    sdc_executor.add_pipeline(pipeline)

    # Yield a namedtuple so that we can access instance names of the stages within the test.
    Pipeline = namedtuple('Pipeline', ['pipeline', 'dev_raw_data_source', 'javascript_evaluator',
                                       'websocket_client'])
    yield Pipeline(pipeline, dev_raw_ds, javascript_evaluator, websocket_client)


def test_websocket(sdc_executor, websocket_server_pipeline, websocket_client_pipeline):
    client_runtime_parameters = {'url': 'ws://localhost:9999', 'appId': 'APPLICATION_ID'}
    server_runtime_parameters = {'port': 9999, 'appId': 'APPLICATION_ID'}

    # Start WebSocket Server pipeline.
    sdc_executor.start_pipeline(websocket_server_pipeline.pipeline, server_runtime_parameters)
    pipeline_status = sdc_executor.get_pipeline_status(websocket_server_pipeline.pipeline).response.json()
    attributes = pipeline_status.get('attributes')
    assert attributes is not None
    assert attributes.get('RUNTIME_PARAMETERS') is not None
    assert attributes.get('RUNTIME_PARAMETERS').get('appId') is not None
    assert attributes.get('RUNTIME_PARAMETERS').get('appId') == 'APPLICATION_ID'

    # Start and capture snapshot for WebSocket Client pipeline.
    snapshot = sdc_executor.capture_snapshot(websocket_client_pipeline.pipeline,
                                             start_pipeline=True,
                                             runtime_parameters=client_runtime_parameters).snapshot
    origin_data = snapshot[websocket_client_pipeline.dev_raw_data_source.instance_name]
    processor_Data = snapshot[websocket_client_pipeline.javascript_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_Data.output) == 2
    assert origin_data.output[0].field['f1'].value == 'abc'
    assert origin_data.output[1].field['f1'].value == 'xyz'

    # Capture snapshot for WebSocket Server pipeline.
    snapshot = sdc_executor.capture_snapshot(websocket_server_pipeline.pipeline).snapshot
    origin_data = snapshot[websocket_server_pipeline.websocket_server.instance_name]
    processor_Data = snapshot[websocket_server_pipeline.expression_evaluator.instance_name]
    assert len(origin_data.output) == 1
    assert len(processor_Data.output) == 1
    assert (origin_data.output[0].field['f1'].value == 'abc' or
            origin_data.output[0].field['f1'].value == 'xyz')

    # Stop the pipeline and verify pipeline's status
    sdc_executor.stop_pipeline(websocket_client_pipeline.pipeline)
    sdc_executor.stop_pipeline(websocket_server_pipeline.pipeline)


def test_websocket_tls(sdc_executor, websocket_server_pipeline, websocket_client_pipeline):
    """Test sending messages between Websocket Client and Server stages with TLS enabled.

    We do so just sending some records with a Websocket Client stage and checking the Websocket Server stage
    correctly receives them. We use two pipelines, one for each websocket stage, defined in the respective
    fixtures.

    """
    input_data = [{'name': 'Rafa Nadal', 'country': 'Spain'},
                  {'name': 'Roger Federer', 'country': 'Switzerland'},
                  {'name': 'Pete Sampras', 'country': 'USA'}]

    client_pipeline_parameters = {'input_data': '\n'.join(json.dumps(rec) for rec in input_data),
                                  'stop_after_first_batch': True, 'url': 'wss://localhost:9999',
                                  'appId': 'APPLICATION_ID', 'use_tls': True}
    server_pipeline_parameters = {'port': 9999, 'appId': 'APPLICATION_ID', 'use_tls': True}

    try:
        snapshot_cmd = sdc_executor.capture_snapshot(websocket_server_pipeline.pipeline,
                                                     start_pipeline=True,
                                                     wait=False,
                                                     runtime_parameters=server_pipeline_parameters,
                                                     batch_size=1,
                                                     batches=3)
        sdc_executor.start_pipeline(websocket_client_pipeline.pipeline,
                                    client_pipeline_parameters).wait_for_finished()

        snapshot = snapshot_cmd.wait_for_finished().snapshot
        stage_name = websocket_server_pipeline.websocket_server.instance_name
        output_records = [batch[stage_name].output[0] for batch in snapshot.snapshot_batches]
        output_data = [{'name': rec.field['name'].value, 'country': rec.field['country'].value}
                       for rec in output_records]
        assert sorted(input_data, key=lambda x: x['name']) == sorted(output_data, key=lambda x: x['name'])

    finally:
        sdc_executor.stop_pipeline(websocket_server_pipeline.pipeline)
