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

import json
import logging
from os.path import dirname, join

import pytest

from testframework import sdc, sdc_models

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture(scope='module')
def websocket_server_pipeline():
    websocket_server_pipeline = sdc_models.Pipeline(join(dirname(__file__),
                                                         'pipelines',
                                                         'websocket_server_to_trash.json'))
    yield websocket_server_pipeline


@pytest.fixture(scope='module')
def websocket_client_pipeline():
    websocket_client_pipeline = sdc_models.Pipeline(join(dirname(__file__),
                                                         'pipelines',
                                                         'dev_raw_source_to_websocket.json'))
    yield websocket_client_pipeline

@pytest.fixture(scope='module')
def dc(args, websocket_server_pipeline, websocket_client_pipeline):
    dc = sdc.DataCollector(version=args.sdc_version)
    dc.add_pipeline(websocket_server_pipeline)
    dc.add_pipeline(websocket_client_pipeline)
    dc.start()
    yield dc
    # After all tests or after an exception is encountered, stop and remove the SDC instance
    dc.tear_down()


def test_websocket(dc, websocket_server_pipeline, websocket_client_pipeline):
    runtime_parameters = {'port': 9999, 'appId': 'APPLICATION_ID'}

    # Start WebSocket Server Pipeline
    dc.start_pipeline(websocket_server_pipeline,
                      runtime_parameters).wait_for_status(status='RUNNING', timeout_sec=300)
    pipeline_status = dc.api_client.get_pipeline_status(
        websocket_server_pipeline.id).response.json()
    status = pipeline_status.get('status')
    assert status == 'RUNNING'
    attributes = pipeline_status.get('attributes')
    assert attributes is not None
    assert attributes.get('RUNTIME_PARAMETERS') is not None
    assert attributes.get('RUNTIME_PARAMETERS').get('appId') is not None
    assert attributes.get('RUNTIME_PARAMETERS').get('appId') == 'APPLICATION_ID'

    # Start and capture snapshot for WebSocket Client Target Pipeline
    snapshot = dc.capture_snapshot(websocket_client_pipeline, start_pipeline=True,
                                   runtime_parameters=runtime_parameters
                                   ).wait_for_finished().snapshot
    origin_data = snapshot['DevRawDataSource_01']
    processor_Data = snapshot['JavaScriptEvaluator_01']
    assert len(origin_data.output) == 2
    assert len(processor_Data.output) == 2
    assert origin_data.output[0].value['value']['f1']['value'] == 'abc'
    assert origin_data.output[1].value['value']['f1']['value'] == 'xyz'

    # Capture snapshot for WebSocket Server Target Pipeline
    snapshot = dc.capture_snapshot(websocket_server_pipeline).wait_for_finished().snapshot
    origin_data = snapshot['WebSocketServer_01']
    processor_Data = snapshot['ExpressionEvaluator_01']
    assert len(origin_data.output) == 1
    assert len(processor_Data.output) == 1
    assert (origin_data.output[0].value['value']['f1']['value'] == 'abc' or
            origin_data.output[0].value['value']['f1']['value'] == 'xyz')

    # Stop the pipeline and verify pipeline's status
    dc.stop_pipeline(websocket_client_pipeline).wait_for_stopped()
    dc.stop_pipeline(websocket_server_pipeline).wait_for_stopped()

