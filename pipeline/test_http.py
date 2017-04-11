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
def http_server_pipeline():
    http_server_pipeline = sdc_models.Pipeline(join(dirname(__file__),
                                                    'pipelines',
                                                    'http_server_to_trash.json'))
    yield http_server_pipeline


@pytest.fixture(scope='module')
def http_client_pipeline():
    http_client_pipeline = sdc_models.Pipeline(join(dirname(__file__),
                                                    'pipelines',
                                                    'dev_raw_source_to_http.json'))
    yield http_client_pipeline

@pytest.fixture(scope='module')
def dc(args, http_server_pipeline, http_client_pipeline):
    dc = sdc.DataCollector(version=args.sdc_version)
    dc.add_pipeline(http_server_pipeline)
    dc.add_pipeline(http_client_pipeline)
    dc.start()
    yield dc
    # After all tests or after an exception is encountered, stop and remove the SDC instance
    dc.tear_down()


def test_http(dc, http_server_pipeline, http_client_pipeline):
    # Start http Server Pipeline
    server_runtime_parameters = {
        'HTTP_PORT': 9999,
        'APPLICATION_ID': 'HTTP_APPLICATION_ID',
        'NEW_FIELD_NAME': 'javscriptField',
        'NEW_FIELD_VALUE': 5000
    }
    dc.start_pipeline(http_server_pipeline,
                      server_runtime_parameters).wait_for_status(status='RUNNING', timeout_sec=300)
    pipeline_status = dc.api_client.get_pipeline_status(
        http_server_pipeline.name).response.json()
    status = pipeline_status.get('status')
    assert status == 'RUNNING'
    attributes = pipeline_status.get('attributes')
    assert attributes.get('RUNTIME_PARAMETERS').get('APPLICATION_ID') == 'HTTP_APPLICATION_ID'

    # Start and capture snapshot for http Client Target Pipeline
    client_runtime_parameters = {
        'RAW_DATA': '{"f1": "abc"}{"f1": "xyz"}',
        'RESOURCE_URL': 'http://localhost:9999',
        'APPLICATION_ID': 'HTTP_APPLICATION_ID'
    }
    snapshot = dc.capture_snapshot(http_client_pipeline, start_pipeline=True,
                                   runtime_parameters=client_runtime_parameters
                                   ).wait_for_finished().snapshot
    origin_data = snapshot['DevRawDataSource_01']
    processor_data = snapshot['ExpressionEvaluator_01']
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].value['value']['f1']['value'] == 'abc'
    assert origin_data.output[1].value['value']['f1']['value'] == 'xyz'

    # Capture snapshot for http Server Target Pipeline
    snapshot = dc.capture_snapshot(http_server_pipeline).wait_for_finished().snapshot
    origin_data = snapshot['HTTPServer_01']
    processor_data = snapshot['JavaScriptEvaluator_01']
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].value['value']['f1']['value'] == 'abc'
    assert origin_data.output[1].value['value']['f1']['value'] == 'xyz'
    assert processor_data.output[0].value['value']['f1']['value'] == 'abc'
    assert processor_data.output[0].value['value']['javscriptField']['value'] == '5000'
    assert processor_data.output[1].value['value']['f1']['value'] == 'xyz'
    assert processor_data.output[1].value['value']['javscriptField']['value'] == '5000'

    # Stop the pipelines
    dc.stop_pipeline(http_client_pipeline).wait_for_stopped()
    dc.stop_pipeline(http_server_pipeline).wait_for_stopped()


def test_http_client_target_wrong_host(dc, http_client_pipeline):
    # Start http Client Target Pipeline with invalid resource URL
    client_runtime_parameters = {
        'RAW_DATA': '{"f1": "abc"}{"f1": "xyz"}',
        'RESOURCE_URL': 'http://localhost:9999',
        'APPLICATION_ID': 'HTTP_APPLICATION_ID'
    }
    snapshot = dc.capture_snapshot(http_client_pipeline, start_pipeline=True,
                                   runtime_parameters=client_runtime_parameters
                                   ).wait_for_finished().snapshot
    origin_data = snapshot['DevRawDataSource_01']
    processor_data = snapshot['ExpressionEvaluator_01']
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].value['value']['f1']['value'] == 'abc'
    assert origin_data.output[1].value['value']['f1']['value'] == 'xyz'

    # Since resource URL is invalid all the records should go to error in target stage
    target_data = snapshot['HTTPClient_01']
    assert len(target_data.error_records) == 2

    # Stop the pipeline
    dc.stop_pipeline(http_client_pipeline).wait_for_stopped()
