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

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import json
import logging
from collections import namedtuple

import pytest

from testframework.markers import http

logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, redefined-outer-name, too-many-locals


@pytest.fixture(scope='module')
def http_server_pipeline(sdc_builder, sdc_executor):
    """HTTP Server pipeline fixture."""
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

    sdc_executor.start_pipeline(http_server_pipeline.pipeline, server_runtime_parameters)
    pipeline_status = sdc_executor.get_pipeline_status(http_server_pipeline.pipeline).response.json()
    attributes = pipeline_status.get('attributes')
    assert attributes.get('RUNTIME_PARAMETERS').get('APPLICATION_ID') == 'HTTP_APPLICATION_ID'

    # Start and capture snapshot for HTTP Client pipeline.
    client_runtime_parameters = {'RAW_DATA': '{"f1": "abc"}{"f1": "xyz"}',
                                 'RESOURCE_URL': 'http://localhost:9999',
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID'}

    snapshot = sdc_executor.capture_snapshot(http_client_pipeline.pipeline, start_pipeline=True,
                                             runtime_parameters=client_runtime_parameters).snapshot
    origin_data = snapshot[http_client_pipeline.dev_raw_data_source.instance_name]
    processor_data = snapshot[http_client_pipeline.expression_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].value['value']['f1']['value'] == 'abc'
    assert origin_data.output[1].value['value']['f1']['value'] == 'xyz'

    # Capture snapshot for HTTP Server pipeline.
    snapshot = sdc_executor.capture_snapshot(http_server_pipeline.pipeline).snapshot
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
    sdc_executor.stop_pipeline(http_client_pipeline.pipeline)
    sdc_executor.stop_pipeline(http_server_pipeline.pipeline)


def test_http_client_target_wrong_host(sdc_executor, http_client_pipeline):
    # Start HTTP Client pipeline with invalid resource URL.
    client_runtime_parameters = {'RAW_DATA': '{"f1": "abc"}{"f1": "xyz"}',
                                 'RESOURCE_URL': 'http://localhost:9999',
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID'}
    snapshot = sdc_executor.capture_snapshot(http_client_pipeline.pipeline, start_pipeline=True,
                                             runtime_parameters=client_runtime_parameters).snapshot
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
    sdc_executor.stop_pipeline(http_client_pipeline.pipeline)


@http
def test_http_processor_get(sdc_builder, sdc_executor, http_client):
    """Test HTTP Lookup Processor for HTTP GET method. We do so by requesting to a pre-defined
    HTTP server endpoint (testGetJsonEndpoint) and get as expected data. The pipeline looks like:

        dev_raw_data_source >> http_client_processor >> trash
    """
    expected_dict = dict(latitude='37.7576948', longitude='-122.4726194')
    record_output_field = 'result'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='dummy')
    http_client_processor = builder.add_stage('HTTP Client', type='processor')
    http_client_processor.set_attributes(data_format='JSON', http_method='GET',
                                         resource_url=f'{http_client.http_server_url}/testGetJsonEndpoint',
                                         output_field=f'/{record_output_field}')
    trash = builder.add_stage('Trash')

    dev_raw_data_source >> http_client_processor >> trash
    pipeline = builder.build(title='HTTP Lookup GET Processor pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # ensure HTTP GET result is only stored to one record and assert the data
    assert len(snapshot[http_client_processor.instance_name].output) == 1
    record = snapshot[http_client_processor.instance_name].output[0].value['value']
    assert record[record_output_field]['value']['latitude']['value'] == expected_dict['latitude']
    assert record[record_output_field]['value']['longitude']['value'] == expected_dict['longitude']


@http
def test_http_processor_post(sdc_builder, sdc_executor, http_client):
    """Test HTTP Lookup Processor for HTTP POST method. We do so by posting to a pre-defined
    HTTP server endpoint (testPostJsonEndpoint) and get expected data. The pipeline looks like:

        dev_raw_data_source >> http_client_processor >> trash
    """
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    expected_dict = dict(latitude='37.7576948', longitude='-122.4726194')
    record_output_field = 'result'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
    http_client_processor = builder.add_stage('HTTP Client', type='processor')
    # for POST, we post 'raw_data' and expect 'expected_dict' as response data
    http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                         headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                         http_method='POST', request_data="${record:value('/text')}",
                                         resource_url=f'{http_client.http_server_url}/testPostJsonEndpoint',
                                         output_field=f'/{record_output_field}')
    trash = builder.add_stage('Trash')

    dev_raw_data_source >> http_client_processor >> trash
    pipeline = builder.build(title='HTTP Lookup POST Processor pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    # ensure HTTP POST result is only stored to one record and assert the data
    assert len(snapshot[http_client_processor.instance_name].output) == 1
    record = snapshot[http_client_processor.instance_name].output[0].value['value']
    assert record[record_output_field]['value']['latitude']['value'] == expected_dict['latitude']
    assert record[record_output_field]['value']['longitude']['value'] == expected_dict['longitude']
