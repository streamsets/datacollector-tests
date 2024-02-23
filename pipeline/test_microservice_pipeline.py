# Copyright 2020 StreamSets Inc.
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

import requests
from requests.auth import HTTPBasicAuth
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import sdc_min_version

HTTP_LISTENING_PORT = 8234


def test_microservice_template_pipeline(sdc_executor):
    """Try creating Microservice pipeline using default template.
       Ensure that there are no validation or other other issues.
    """

    create_pipeline_response = sdc_executor.api_client.create_pipeline(
        pipeline_title="Sample Microservice pipeline using template",
        auto_generate_pipeline_id=True,
        pipeline_type="MICROSERVICE").response.json()
    assert create_pipeline_response is not None

    pipeline_id = create_pipeline_response['pipelineId']
    pipeline = sdc_executor.pipelines.get(id=pipeline_id)

    sdc_executor.validate_pipeline(pipeline)


@sdc_min_version('3.16.0')
def test_microservice_pipeline_response(sdc_builder, sdc_executor):
    """Test Microservice Pipeline Response. The pipeline would look like:

        rest_service_source >> send_response_target

    """

    pipeline = _create_microservice_pipeline(sdc_builder)

    if Version('3.14.0') <= Version(sdc_builder.version) < Version('3.17.0'):
        pipeline.origin_stage.list_of_application_ids = [{"appId": 'TEST_ID_FIRST'}]
    elif Version(sdc_builder.version) >= Version('3.17.0'):
        pipeline.origin_stage.list_of_application_ids = [{"credential": 'TEST_ID_FIRST'}]

    pipeline.stages[2].status_code = 201
    pipeline.stages[2].response_headers = [{'key': 'SAMPLE_RESPONSE_HEADER', 'value': 'test'}]
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.validate_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline)

        # Try a GET request using sample data with a valid Application-ID and we should expect
        # a custom response 201 and custom response header.
        rest_service_url = f'http://{sdc_executor.server_host}:{HTTP_LISTENING_PORT}'
        resp = requests.get(rest_service_url, headers={'X-SDC-APPLICATION-ID': 'TEST_ID_FIRST'})
        assert resp.status_code == 201
        assert resp.headers['SAMPLE_RESPONSE_HEADER'] == 'test'

    finally:
        sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('3.16.0')
def test_with_no_application_id(sdc_builder, sdc_executor):
    """Test Microservice Pipeline with no application Id. """

    pipeline = _create_microservice_pipeline(sdc_builder)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.validate_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline)
        rest_service_url = f'http://{sdc_executor.server_host}:{HTTP_LISTENING_PORT}'
        resp = requests.post(rest_service_url, json={"f1": "abc"})
        _validate_response_json(resp)
    finally:
        sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('3.16.0')
def test_rest_service_multiple_application_ids(sdc_builder, sdc_executor):
    """Test Microservice Pipeline with multiple application Ids. """

    pipeline = _create_microservice_pipeline(sdc_builder)

    if Version('3.14.0') <= Version(sdc_builder.version) < Version('3.17.0'):
        pipeline.origin_stage.list_of_application_ids = [{"appId": 'TEST_ID_FIRST'}, {"appId": 'TEST_ID_SECOND'}]
    elif Version(sdc_builder.version) >= Version('3.17.0'):
        pipeline.origin_stage.list_of_application_ids = [{"credential": 'TEST_ID_FIRST'},
                                                         {"credential": 'TEST_ID_SECOND'}]

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.validate_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline)
        rest_service_url = f'http://{sdc_executor.server_host}:{HTTP_LISTENING_PORT}'
        resp = requests.post(rest_service_url, headers={'X-SDC-APPLICATION-ID': 'TEST_ID_FIRST'}, json={"f1": "abc"})
        assert resp.status_code == 200

        resp = requests.post(rest_service_url, headers={'X-SDC-APPLICATION-ID': 'TEST_ID_SECOND'}, json={"f1": "abc"})
        assert resp.status_code == 200
    finally:
        sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('3.16.0')
def test_rest_service_with_gateway_and_no_auth(sdc_builder, sdc_executor):
    """Test Microservice Pipeline with gateway enabled and with no gateway authentication. """
    pipeline = _create_microservice_pipeline(sdc_builder)
    pipeline.origin_stage.use_api_gateway = True
    pipeline.origin_stage.gateway_service_name = 'service1'
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.validate_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline)
        rest_service_url = f'{sdc_executor.api_client.server_url}/public-rest/v1/gateway/service1'
        _validate_rest_service(rest_service_url, None)
    finally:
        sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('3.16.0')
def test_rest_service_with_gateway_and_auth(sdc_builder, sdc_executor):
    """Test Microservice Pipeline with gateway enabled and with gateway authentication. """
    pipeline = _create_microservice_pipeline(sdc_builder)
    pipeline.origin_stage.use_api_gateway = True
    pipeline.origin_stage.gateway_service_name = 'service2'
    pipeline.origin_stage.require_gateway_authentication = True
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.validate_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline)
        rest_service_url = f'{sdc_executor.api_client.server_url}/rest/v1/gateway/service2'
        _validate_rest_service(rest_service_url, HTTPBasicAuth('admin', 'admin'))
    finally:
        sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('3.16.0')
def test_calling_gateway_api_with_invalid_service_name(sdc_executor):
    """Test calling gateway REST API with invalid service name """
    rest_service_url = f'{sdc_executor.api_client.server_url}/public-rest/v1/gateway/invalid'
    resp = requests.get(rest_service_url)
    assert resp.status_code == 500
    resp_json = resp.json()
    assert resp_json['RemoteException'] is not None
    msg = 'java.lang.IllegalStateException: No entry found in the gateway registry for the service name: invalid'
    assert resp_json['RemoteException']['message'] == msg


@sdc_min_version('3.17.0')
def test_fetching_pipeline_with_square_brackets_id(sdc_executor):
    """Test fetching pipeline with square brackets in the pipeline Id.
       Passing auto_generate_pipeline_id=False, to generate pipelineId using pipeline title.
    """
    create_pipeline_response = sdc_executor.api_client.create_pipeline(
        pipeline_title="[Testing] Sample Microservice pipeline using template",
        auto_generate_pipeline_id=False,
        pipeline_type="MICROSERVICE").response.json()
    assert create_pipeline_response is not None

    pipeline_id = create_pipeline_response['pipelineId']
    pipeline = sdc_executor.pipelines.get(id=pipeline_id)
    assert pipeline is not None


@sdc_min_version('3.19.0')
def test_rest_service_with_gateway_and_query_string(sdc_builder, sdc_executor):
    """Test Microservice Pipeline with gateway enabled and with query string.
       Test for ESC-955 - querystring is empty when gateway is enabled for REST Service origin (microservice)
    """
    pipeline = _create_microservice_pipeline(sdc_builder)
    pipeline.origin_stage.use_api_gateway = True
    pipeline.origin_stage.gateway_service_name = 'service1'
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.validate_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline)
        rest_service_url = f'{sdc_executor.api_client.server_url}/public-rest/v1/gateway/service1?param1=val1'
        resp = requests.get(rest_service_url)
        assert resp.status_code == 200
        resp_json = resp.json()
        assert resp_json['httpStatusCode'] == 200
        assert resp_json['data'] is not None
        assert len(resp_json['data']) == 1
        assert resp_json['data'][0]['querystring'] == 'param1=val1'
    finally:
        sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.10.0')
def test_rest_service_to_error(sdc_builder, sdc_executor):
    """Test rest service origin generates error records when the payload fails to parse.

        rest_service_source >> wiretap
    """

    builder = sdc_builder.get_pipeline_builder()

    rest_service_source = builder.add_stage('REST Service')
    rest_service_source.http_listening_port = HTTP_LISTENING_PORT
    rest_service_source.on_record_error = 'TO_ERROR'

    wiretap = builder.add_wiretap()

    rest_service_source >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)

    rest_service_url = f'http://{sdc_executor.server_host}:{HTTP_LISTENING_PORT}'
    # build a malformed JSON string with 2 records. The string has an extra closing brace '}' on the second record.
    data = '{"username":"abc","password":"xyz"}{"username":"def","password":"ijk"}}'
    headers = {'Content-type': 'application/json'}
    response = requests.get(rest_service_url, headers=headers, data=data)
    assert response.status_code == 200, (f'Failed to GET rest service {rest_service_url}. '
                                         f'Status code: {response.status_code}')

    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    output_records = wiretap.output_records
    error_records = wiretap.error_records
    assert len(output_records) == 1, "Wrong number of output records"
    assert len(error_records) == 1, "Wrong number of error records"
    assert error_records[0].header['errorCode'] == "HTTP_SERVER_PUSH_01", (f'Wrong error code. '
          f'Expected: HTTP_SERVER_PUSH_01, Actual: {error_records[0].header["errorCode"]}')


def _create_microservice_pipeline(sdc_builder):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    rest_service_source = pipeline_builder.add_stage('REST Service')
    rest_service_source.http_listening_port = HTTP_LISTENING_PORT
    expression = pipeline_builder.add_stage('Expression Evaluator')
    expression.field_expressions = [
        {"fieldToSet": "/querystring", "expression": "${record:attribute('queryString')}"}
    ]
    send_response_target = pipeline_builder.add_stage('Send Response to Origin')
    rest_service_source >> expression >> send_response_target
    pipeline = pipeline_builder.build()
    return pipeline


def _validate_rest_service(rest_service_url, auth):
    resp = requests.get(rest_service_url, auth=auth)
    assert resp.status_code == 200

    resp = requests.post(rest_service_url, headers={'X-Requested-By': 'test'}, json={"f1": "abc"}, auth=auth)
    _validate_response_json(resp)

    resp = requests.put(rest_service_url, headers={'X-Requested-By': 'test'}, json={"f1": "abc"}, auth=auth)
    _validate_response_json(resp)

    resp = requests.delete(rest_service_url, headers={'X-Requested-By': 'test'}, auth=auth)
    assert resp.status_code == 200

    resp = requests.head(rest_service_url)
    assert resp.status_code == 200


def _validate_response_json(resp):
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json['httpStatusCode'] == 200
    assert resp_json['data'] is not None
    assert len(resp_json['data']) == 1
    assert resp_json['data'][0]['f1'] == 'abc'
