# Copyright 2017 StreamSets Inc.
#
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

import logging
from collections import namedtuple

import pytest

logger = logging.getLogger(__name__)

@pytest.fixture(scope='module')
def http_server_pipeline(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    http_server = pipeline_builder.add_stage('HTTP Server')
    http_server.application_id = '${APPLICATION_ID}'
    http_server.data_format = 'JSON'
    http_server.http_listening_port = '${HTTP_PORT}'

    javascript_evaluator = pipeline_builder.add_stage('JavaScript Evaluator')
    javascript_evaluator.script = """for(var i = 0; i < records.length; i++) {
                                       try {
                                         records[i].value['${NEW_FIELD_NAME}'] = ${NEW_FIELD_VALUE}
                                         output.write(records[i]);
                                       } catch (e) {
                                         // Send record to error
                                         error.write(records[i], e);
                                       }
                                     }
                                  """

    trash = pipeline_builder.add_stage('Trash')

    http_server >> javascript_evaluator >> trash
    pipeline = pipeline_builder.build()
    pipeline.add_parameters(HTTP_PORT='8000',
                            APPLICATION_ID='test',
                            NEW_FIELD_NAME='javscriptField',
                            NEW_FIELD_VALUE='5000')

    sdc_executor.add_pipeline(pipeline)

    # Yield a namedtuple so that we can access instance names of the stages within the test.
    yield namedtuple('Pipeline', ['pipeline', 'http_server', 'javascript_evaluator'])(pipeline,
                                                                                      http_server,
                                                                                      javascript_evaluator)


@pytest.fixture(scope='module')
def http_client_pipeline(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.raw_data = '${RAW_DATA}'
    dev_raw_data_source.data_format = 'JSON'

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')

    http_client = pipeline_builder.add_stage('HTTP Client', type='destination')
    http_client.resource_url = '${RESOURCE_URL}'
    http_client.headers = [{'key': 'X-SDC-APPLICATION-ID', 'value': '${APPLICATION_ID}'}]
    http_client.one_request_per_batch = True

    dev_raw_data_source >> expression_evaluator >> http_client
    pipeline = pipeline_builder.build()
    pipeline.add_parameters(RAW_DATA='{"f1": "abc"}{"f1": "xyz"}',
                            RESOURCE_URL='http://localhost:8000',
                            APPLICATION_ID='test')

    sdc_executor.add_pipeline(pipeline)

    yield namedtuple('Pipeline', ['pipeline',
                                  'dev_raw_data_source',
                                  'expression_evaluator',
                                  'http_client'])(pipeline,
                                                  dev_raw_data_source,
                                                  expression_evaluator,
                                                  http_client)


def test_http(sdc_executor, http_server_pipeline, http_client_pipeline):
    # Start HTTP Server pipeline.
    server_runtime_parameters = {'HTTP_PORT': 9999,
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID',
                                 'NEW_FIELD_NAME': 'javscriptField',
                                 'NEW_FIELD_VALUE': 5000}

    sdc_executor.start_pipeline(http_server_pipeline.pipeline,
                                server_runtime_parameters).wait_for_status(status='RUNNING', timeout_sec=300)
    pipeline_status = sdc_executor.api_client.get_pipeline_status(http_server_pipeline.pipeline.id).response.json()
    attributes = pipeline_status.get('attributes')
    assert attributes.get('RUNTIME_PARAMETERS').get('APPLICATION_ID') == 'HTTP_APPLICATION_ID'

    # Start and capture snapshot for HTTP Client pipeline.
    client_runtime_parameters = {'RAW_DATA': '{"f1": "abc"}{"f1": "xyz"}',
                                 'RESOURCE_URL': 'http://localhost:9999',
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID'}

    snapshot = sdc_executor.capture_snapshot(http_client_pipeline.pipeline,
                                             start_pipeline=True,
                                             runtime_parameters=client_runtime_parameters).wait_for_finished().snapshot
    origin_data = snapshot[http_client_pipeline.dev_raw_data_source.instance_name]
    processor_data = snapshot[http_client_pipeline.expression_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].value['value']['f1']['value'] == 'abc'
    assert origin_data.output[1].value['value']['f1']['value'] == 'xyz'

    # Capture snapshot for HTTP Server pipeline.
    snapshot = sdc_executor.capture_snapshot(http_server_pipeline.pipeline).wait_for_finished().snapshot
    origin_data = snapshot[http_server_pipeline.http_server.instance_name]
    processor_data = snapshot[http_server_pipeline.javascript_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].value['value']['f1']['value'] == 'abc'
    assert origin_data.output[1].value['value']['f1']['value'] == 'xyz'
    assert processor_data.output[0].value['value']['f1']['value'] == 'abc'
    assert processor_data.output[0].value['value']['javscriptField']['value'] == '5000'
    assert processor_data.output[1].value['value']['f1']['value'] == 'xyz'
    assert processor_data.output[1].value['value']['javscriptField']['value'] == '5000'

    # Stop the pipelines.
    sdc_executor.stop_pipeline(http_client_pipeline.pipeline).wait_for_stopped()
    sdc_executor.stop_pipeline(http_server_pipeline.pipeline).wait_for_stopped()


def test_http_client_target_wrong_host(sdc_executor, http_client_pipeline):
    # Start HTTP Client pipeline with invalid resource URL.
    client_runtime_parameters = {'RAW_DATA': '{"f1": "abc"}{"f1": "xyz"}',
                                 'RESOURCE_URL': 'http://localhost:9999',
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID'}
    snapshot = sdc_executor.capture_snapshot(http_client_pipeline.pipeline,
                                             start_pipeline=True,
                                             runtime_parameters=client_runtime_parameters).wait_for_finished().snapshot
    origin_data = snapshot[http_client_pipeline.dev_raw_data_source.instance_name]
    processor_data = snapshot[http_client_pipeline.expression_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].value['value']['f1']['value'] == 'abc'
    assert origin_data.output[1].value['value']['f1']['value'] == 'xyz'

    # Since resource URL is invalid, all the records should go to error in target stage.
    target_data = snapshot[http_client_pipeline.http_client.instance_name]
    assert len(target_data.error_records) == 2

    # Stop the pipeline.
    sdc_executor.stop_pipeline(http_client_pipeline.pipeline).wait_for_stopped()
