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
import string
import http.client as httpclient
from collections import namedtuple

import pytest
from pretenders.common.constants import FOREVER
from streamsets.testframework.markers import http, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, redefined-outer-name, too-many-locals

#
# To start the mock HTTP server, run
#
#    ste start HTTP
#
# or, without ste
#
#    docker run -d --name myhttpmockserver --net=cluster pretenders/pretenders:1.4
#

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


@http
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
    assert origin_data.output[0].value2['f1'] == 'abc'
    assert origin_data.output[1].value2['f1'] == 'xyz'

    # Capture snapshot for HTTP Server pipeline.
    snapshot = sdc_executor.capture_snapshot(http_server_pipeline.pipeline).snapshot
    origin_data = snapshot[http_server_pipeline.http_server.instance_name]
    processor_data = snapshot[http_server_pipeline.javascript_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].value2['f1'] == 'abc'
    assert origin_data.output[1].value2['f1'] == 'xyz'
    assert processor_data.output[0].value2['f1'] == 'abc'
    assert processor_data.output[0].value2['javscriptField'] == 5000
    assert processor_data.output[1].value2['f1'] == 'xyz'
    assert processor_data.output[1].value2['javscriptField'] == 5000

    # Stop the pipelines.
    sdc_executor.stop_pipeline(http_client_pipeline.pipeline)
    sdc_executor.stop_pipeline(http_server_pipeline.pipeline)


@http
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
    assert origin_data.output[0].value2['f1'] == 'abc'
    assert origin_data.output[1].value2['f1'] == 'xyz'

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
    expected_data = json.dumps(expected_dict)
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='dummy')
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', http_method='GET',
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')
        trash = builder.add_stage('Trash')

        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title='HTTP Lookup GET Processor pipeline')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # ensure HTTP GET result is only stored to one record and assert the data
        assert len(snapshot[http_client_processor.instance_name].output) == 1
        record = snapshot[http_client_processor.instance_name].output[0].value2
        assert record[record_output_field]['latitude'] == expected_dict['latitude']
        assert record[record_output_field]['longitude'] == expected_dict['longitude']
    finally:
        http_mock.delete_mock()


@http
@pytest.mark.parametrize(('method'), [
    'POST',
    # Testing of SDC-10809
    'PATCH'
])
def test_http_processor(sdc_builder, sdc_executor, http_client, method):
    """Test HTTP Lookup Processor for various HTTP methods. We do so by
    sending a request to a pre-defined HTTP server endpoint
    (testPostJsonEndpoint) and getting expected data. The pipeline looks like:

        dev_raw_data_source >> http_client_processor >> trash
    """
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    expected_dict = dict(latitude='37.7576948', longitude='-122.4726194')
    # PATCH requests typically receive a 204 response with no body
    if method == 'POST':
        expected_data = json.dumps(expected_dict)
        expected_status = 200
    elif method == 'PATCH':
        expected_data = ''
        expected_status = 204
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(
            rule=f'{method} /{mock_path}',
            body=raw_data
        ).reply(
            body=expected_data,
            status=expected_status,
            times=FOREVER
        )
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        # for POST/PATCH, we post 'raw_data' and expect 'expected_dict' as response data
        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method=method, request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')
        trash = builder.add_stage('Trash')

        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title=f'HTTP Lookup {method} Processor pipeline')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # ensure HTTP POST/PATCH result is only stored to one record and assert the data
        assert len(snapshot[http_client_processor.instance_name].output) == 1
        record = snapshot[http_client_processor.instance_name].output[0].value2
        if expected_data:
            assert record[record_output_field]['latitude'] == expected_dict['latitude']
            assert record[record_output_field]['longitude'] == expected_dict['longitude']
    finally:
        http_mock.delete_mock()


@http
@pytest.mark.parametrize(('method'), [
    'POST',
    # Testing of SDC-10809
    'PATCH'
])
@pytest.mark.parametrize(('request_option'), [
    'one_request_per_batch',
    # Testing of SDC-10809
    'one_request_per_record'
])
def test_http_destination(sdc_builder, sdc_executor, http_client, method, request_option):
    """Test HTTP Client Destination for HTTP POST/PATCH method. We do so by posting to a pre-defined
    HTTP server endpoint (testPostJsonEndpoint) and get expected data. The pipeline looks like:

        dev_raw_data_source >> http_client_destination
    """
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    expected_dict = dict(latitude='37.7576948', longitude='-122.4726194')
    expected_data = json.dumps(expected_dict)
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'{method} /{mock_path}', body=raw_data).reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
        http_client_destination = builder.add_stage('HTTP Client', type='destination')
        # for POST/PATCH, we post 'raw_data' and expect 'expected_dict' as response data
        http_client_destination.set_attributes(data_format='JSON', default_request_content_type='application/json',
                                               headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                               http_method=method,
                                               resource_url=mock_uri,
                                               one_request_per_batch=(request_option=='one_request_per_batch'))

        dev_raw_data_source >> http_client_destination
        pipeline = builder.build(title=f'HTTP {method} Destination pipeline')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Check that the HTTP server got the expected data
        r = http_mock.get_request(0)
        assert r
        assert r.method == method
        assert r.url == f'/{mock_path}'
        assert r.body
        assert json.loads(r.body.decode("utf-8")) == raw_dict
    finally:
        http_mock.delete_mock()


@http
def test_http_client_source_simple(sdc_builder, sdc_executor, http_client):
    """Test HTTP Client source basic (single batch, not streaming) HTTP server endpoint and
    assert expected data. The pipeline looks like:

        http_client_source >> trash
    """
    raw_data = '{"first:": 1}'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(raw_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        http_client_source = builder.add_stage('HTTP Client', type='origin')
        http_client_source.set_attributes(data_format='JSON', resource_url=mock_uri)

        trash = builder.add_stage('Trash')

        http_client_source >> trash

        pipeline = builder.build(title='HTTP Client Simple')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, batches=1, batch_size=1,
                                                 start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        origin_stage_output = snapshot[http_client_source.instance_name]

        assert len(origin_stage_output.output) == 1
        assert json.dumps(origin_stage_output.output[0].value2) == raw_data
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("3.8.0")
def test_http_server_method_restriction(sdc_executor, http_server_pipeline):
    """HTTP Server Origin should actively disallow TRACE and TRACK HTTP request methods"""
    server_runtime_parameters = {'HTTP_PORT': 9999,
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID',
                                 'NEW_FIELD_NAME': 'javscriptField',
                                 'NEW_FIELD_VALUE': 5000}

    sdc_executor.start_pipeline(http_server_pipeline.pipeline, server_runtime_parameters)
    pipeline_status = sdc_executor.get_pipeline_status(http_server_pipeline.pipeline).response.json()
    attributes = pipeline_status.get('attributes')
    assert attributes.get('RUNTIME_PARAMETERS').get('APPLICATION_ID') == 'HTTP_APPLICATION_ID'

    # Try to push records using prohibited HTTP methods. Expecting HTTP status: 405 Method Not Allowed
    h1 = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
    h1.request('TRACE', '/', '{"f1": "abc"}{"f1": "xyz"}', {'X-SDC-APPLICATION-ID': 'HTTP_APPLICATION_ID'})
    resp = h1.getresponse()
    assert resp.status == 405

    h2 = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
    h2.request('TRACK', '/', '{"f1": "abc"}{"f1": "xyz"}', {'X-SDC-APPLICATION-ID': 'HTTP_APPLICATION_ID'})
    resp = h2.getresponse()
    assert resp.status == 405

    sdc_executor.stop_pipeline(http_server_pipeline.pipeline)