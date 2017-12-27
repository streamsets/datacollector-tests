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


import logging
from collections import namedtuple

import pytest

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def websocket_server_pipeline(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    websocket_server = pipeline_builder.add_stage('WebSocket Server')
    websocket_server.websocket_listening_port = '${port}'
    websocket_server.application_id = '${appId}'
    websocket_server.data_format = 'JSON'

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')

    trash = pipeline_builder.add_stage('Trash')

    websocket_server >> expression_evaluator >> trash
    pipeline = pipeline_builder.build()
    pipeline.add_parameters(port='8080', appId='test')

    sdc_executor.add_pipeline(pipeline)

    # Yield a namedtuple so that we can access instance names of the stages within the test.
    yield namedtuple('Pipeline', ['pipeline',
                                  'websocket_server',
                                  'expression_evaluator'])(pipeline,
                                                           websocket_server,
                                                           expression_evaluator)


@pytest.fixture(scope='module')
def websocket_client_pipeline(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.raw_data = '{"f1": "abc"}{"f1": "xyz"}'
    dev_raw_data_source.data_format = 'JSON'

    javascript_evaluator = pipeline_builder.add_stage('JavaScript Evaluator')

    websocket_client = pipeline_builder.add_stage('WebSocket Client', type='destination')
    websocket_client.resource_url = 'ws://localhost:${port}'
    websocket_client.headers = [{'key': 'X-SDC-APPLICATION-ID', 'value': '${appId}'}]

    dev_raw_data_source >> javascript_evaluator >> websocket_client
    pipeline = pipeline_builder.build()
    pipeline.add_parameters(port='8080', appId='test')

    sdc_executor.add_pipeline(pipeline)

    yield namedtuple('Pipeline', ['pipeline',
                                  'dev_raw_data_source',
                                  'javascript_evaluator'])(pipeline,
                                                           dev_raw_data_source,
                                                           javascript_evaluator)


def test_websocket(sdc_executor, websocket_server_pipeline, websocket_client_pipeline):
    runtime_parameters = {'port': 9999, 'appId': 'APPLICATION_ID'}

    # Start WebSocket Server pipeline.
    sdc_executor.start_pipeline(websocket_server_pipeline.pipeline, runtime_parameters)
    pipeline_status = sdc_executor.get_pipeline_status(websocket_server_pipeline.pipeline).response.json()
    attributes = pipeline_status.get('attributes')
    assert attributes is not None
    assert attributes.get('RUNTIME_PARAMETERS') is not None
    assert attributes.get('RUNTIME_PARAMETERS').get('appId') is not None
    assert attributes.get('RUNTIME_PARAMETERS').get('appId') == 'APPLICATION_ID'

    # Start and capture snapshot for WebSocket Client pipeline.
    snapshot = sdc_executor.capture_snapshot(websocket_client_pipeline.pipeline,
                                             start_pipeline=True,
                                             runtime_parameters=runtime_parameters).snapshot
    origin_data = snapshot[websocket_client_pipeline.dev_raw_data_source.instance_name]
    processor_Data = snapshot[websocket_client_pipeline.javascript_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_Data.output) == 2
    assert origin_data.output[0].value['value']['f1']['value'] == 'abc'
    assert origin_data.output[1].value['value']['f1']['value'] == 'xyz'

    # Capture snapshot for WebSocket Server pipeline.
    snapshot = sdc_executor.capture_snapshot(websocket_server_pipeline.pipeline).snapshot
    origin_data = snapshot[websocket_server_pipeline.websocket_server.instance_name]
    processor_Data = snapshot[websocket_server_pipeline.expression_evaluator.instance_name]
    assert len(origin_data.output) == 1
    assert len(processor_Data.output) == 1
    assert (origin_data.output[0].value['value']['f1']['value'] == 'abc' or
            origin_data.output[0].value['value']['f1']['value'] == 'xyz')

    # Stop the pipeline and verify pipeline's status
    sdc_executor.stop_pipeline(websocket_client_pipeline.pipeline)
    sdc_executor.stop_pipeline(websocket_server_pipeline.pipeline)
