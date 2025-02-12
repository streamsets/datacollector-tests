####
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

import pytest
import string
import sqlalchemy
from streamsets.sdk.exceptions import RunError
from streamsets.testframework.markers import sdc_min_version, web_client, database
from streamsets.testframework.utils import get_random_string, Version

from stage import _wait_for_pipeline_statuses
from stage.test_webclient_origin import retry_parameters, per_status_action_parameters, \
    retry_parameters_unknown_status, per_status_action_parameters_unknown_status
from stage.utils.webclient import (
    deps,
    free_port,
    server,
    Endpoint,
    LIBRARY,
    PER_STATUS_ACTIONS,
    RELEASE_VERSION,
    WEB_CLIENT,
    verify_header,
)
from stage.utils.common import cleanup, test_name
from stage.utils.utils_migration import LegacyHandler as PipelineHandler

import logging
from random import randint

DEFAULT_TIMEOUT_IN_SEC = 30

logger = logging.getLogger(__name__)
pytestmark = [sdc_min_version(RELEASE_VERSION), web_client]


@pytest.fixture(autouse=True)
def skip_5_11_tests(sdc_builder):
    if Version(sdc_builder.version) == Version('5.11.0'):
        pytest.skip('This test is expected to fail in Version 5.11.0 as it got fixed in'
                    ' https://review.streamsets.net/c/datacollector/+/77887')


@pytest.mark.parametrize(
    "method, body, content_type, method_expression, body_expression",
    [
        ["Get", None, None, None, None],
        ["Post", '{"hello": "there"}', "application/json", None, "${record:value('/body')}"],
        ["Put", '{"hello": "there"}', "application/json", None, "${record:value('/body')}"],
        ["Delete", None, None, None, None],
        ["Expression", '{"hello": "there"}', "application/json", "${record:value('/method')}", "${record:value('/body')}"],
    ],
)
def test_http_methods(sdc_builder, sdc_executor, cleanup, server, test_name, method, body, content_type, method_expression, body_expression):

    """Test the HTTP methods of the processor.
    The body and optionally the method are taken from the input record."""

    from flask import json, request

    input_data = {
        "method": method if method != "Expression" else "POST",
        "body": body,
    }
    raw_data = json.dumps(input_data)
    default_response = {"general": "kenobi"}
    expected_request_body = json.loads(body) if body is not None else None
    dumped_response = json.dumps(default_response)

    def get():
        return dumped_response

    def post():
        request_body = {}
        try:
            request_body = json.loads(request.data)
        except:
            return None, 400
        if request_body == expected_request_body:
            return dumped_response
        else:
            return None, 404

    def put():
        return post()

    def delete():
        return get()

    def exp():
        return post()

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    endpoints = {
        "Get": Endpoint(get, ["GET"], "get"),
        "Post": Endpoint(post, ["POST"], "post"),
        "Put": Endpoint(put, ["PUT"], "put"),
        "Delete": Endpoint(delete, ["DELETE"], "delete"),
        "Expression": Endpoint(exp, ["POST"], "exp"),
    }
    endpoint = endpoints[method]

    expected_records = [default_response]

    server.start([endpoint])
    cleanup(server.stop)
    server.ready()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=raw_data,
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library=LIBRARY, request_endpoint=f"{server.url}/{endpoint.path}", method=method, output_field="/", per_status_actions=PER_STATUS_ACTIONS,
    )
    if body is not None:
        if body_expression is None:
            webclient_processor.set_attributes(request_body=json.dumps(body))
        else:
            webclient_processor.set_attributes(request_body=body_expression)
    if method_expression is not None:
        webclient_processor.set_attributes(method_expression=method_expression)
    if content_type is not None:
        # We don't need to set the header as JSON is set by default,
        # but let's make it explicit to make changing this test easier.
        webclient_processor.set_attributes(
            common_headers=[{"commonHeaderName": "Content-Type", "commonHeaderValue": "application/json"}]
        )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert len(records) == len(expected_records)
    assert all([er in records for er in expected_records])


@pytest.mark.parametrize("multiple_values_behavior", ["FIRST_ONLY", "ALL_AS_LIST", "SPLIT_INTO_MULTIPLE_RECORDS"])
@pytest.mark.parametrize("json_content", ["ARRAY_OBJECTS", "MULTIPLE_OBJECTS"])
def test_multiple_values_behavior(
    sdc_builder, sdc_executor, cleanup, server, test_name, multiple_values_behavior, json_content
):

    """Verify that the processor produces the expected records in the expected format for all the
    multiple values behaviour and json content combinations. The behavior is standard, and should
    be the same across all the processor stages that support it."""

    from flask import json

    result_field = "result"
    output_field = "output"
    input_data = {"a": "b", "c": "d"}
    raw_data = json.dumps([input_data])

    response_data = {result_field: [{"e": "f"}, {"g": "h"}, {"i": "j"}]}

    expected_records = []
    # fmt: off
    if json_content == "MULTIPLE_OBJECTS":
        if multiple_values_behavior in {"FIRST_ONLY", "SPLIT_INTO_MULTIPLE_RECORDS"}:
            expected_records.append({**input_data, **{output_field: response_data}})
        elif multiple_values_behavior == "ALL_AS_LIST":
            expected_records.append({**input_data, **{output_field: [response_data]}})
        else:
            pytest.fail(f"Invalid multiple_values_behavior: {multiple_values_behavior}.")
    elif json_content == "ARRAY_OBJECTS":
        if multiple_values_behavior == "FIRST_ONLY":
            expected_records.append({**input_data, **{output_field: {
                result_field: response_data[result_field][0]
            }}})
        elif multiple_values_behavior == "ALL_AS_LIST":
            expected_records.append(
                {**input_data, **{output_field: [
                    {result_field: item} for item in response_data[result_field]
                ]}}
            )
        elif multiple_values_behavior == "SPLIT_INTO_MULTIPLE_RECORDS":
            for item in response_data[result_field]:
                expected_records.append({**input_data, **{output_field: {result_field: item}}})
        else:
            pytest.fail(f"Invalid multiple_values_behavior: {multiple_values_behavior}.")
    else:
        pytest.fail(f"Invalid json_content: {json_content}.")
    # fmt: on

    def serve():
        response = None
        if json_content == "MULTIPLE_OBJECTS":
            response = response_data
        else:
            response = [{result_field: result} for result in response_data[result_field]]
        logger.error(response)
        return json.dumps(response)

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", json_content="ARRAY_OBJECTS", stop_after_first_batch=True, raw_data=raw_data
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library=LIBRARY,
        request_endpoint=url,
        json_content=json_content,
        output_field=f"/{output_field}",
        multiple_values_behavior=multiple_values_behavior,
        per_status_actions=PER_STATUS_ACTIONS,
    )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert len(records) == len(expected_records)
    assert all([er in records for er in expected_records])


def test_common_header(sdc_builder, sdc_executor, cleanup, server, test_name):
    """
    Verify common headers are included in the request.
    """

    from flask import json

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    success_message = 'Success! headers are present'

    endpoint = Endpoint(verify_header, ["GET"], 'verify-header')
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": 10})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        common_headers=[
            {
                "commonHeaderName": "header1",
                "commonHeaderValue": "some_value1"
            },
            {
                "commonHeaderName": "header2",
                "commonHeaderValue": "some_value2"
            }
        ],
        output_field="/field1",
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    assert success_message == output_records[0].field.get('field1')


def test_security_header(sdc_builder, sdc_executor, cleanup, server, test_name):
    """
    Verify security headers are included in the request.
    """

    from flask import json

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    success_message = 'Success! headers are present'

    endpoint = Endpoint(verify_header, ["GET"], 'verify-header')
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": 10})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        security_headers=[
            {
                "securityHeaderName": "header1",
                "securityHeaderValue": "some_value1"
            },
            {
                "securityHeaderName": "header2",
                "securityHeaderValue": "some_value2"
            }
        ],
        output_field="/field1",
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    assert success_message == output_records[0].field.get('field1')


def test_common_and_security_header(sdc_builder, sdc_executor, cleanup, server, test_name):
    """
    Verify common and security headers are included in the request.
    """

    from flask import json

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    success_message = 'Success! headers are present'

    endpoint = Endpoint(verify_header, ["GET"], 'verify-header')
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": 10})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        security_headers=[{"securityHeaderName": "header1", "securityHeaderValue": "some_value1"}],
        common_headers=[{"commonHeaderName": "header2", "commonHeaderValue": "some_value2"}],
        output_field="/field1",
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    assert success_message == output_records[0].field.get('field1')


def test_common_header_evaluation(sdc_builder, sdc_executor, cleanup, server, test_name):
    from flask import json, request

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    test_header = "testheader"
    expected_value = 10

    def serve():
        if request.headers[test_header] != f"{expected_value}":
            pytest.fail("Expected value not found in header.")
        return json.dumps({"field1": 10})

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": 10})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        common_headers=[{"commonHeaderName": test_header, "commonHeaderValue": f"${{record:value('/field1')}}"}],
        output_field="/field1",
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)


@sdc_min_version("5.11.0")
def test_endpoint_evaluation(sdc_builder, sdc_executor, cleanup, server, test_name,):
    from flask import json, Response

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    endpoint_id = randint(0, 10000)
    field_value = 10

    def serve():
        return Response('{"Hello": "World"}', status=200, mimetype="application/json")

    endpoint = Endpoint(serve, ["GET"], path=f"endpoint_{endpoint_id}")
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": field_value, "endpoint_id": endpoint_id})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=f"{server.url}/endpoint_${{record:value('/endpoint_id')}}",
        output_field="/field2",
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    logger.error(output_records[0].field.get('field1'))
    assert output_records[0].field.get('field1').value == field_value
    assert output_records[0].field.get('field2') == {"Hello": "World"}


@sdc_min_version("5.11.0")
@pytest.mark.parametrize(
    "per_status_actions, status, success, hits, err",
    per_status_action_parameters
)
def test_per_status_actions(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, success, hits, err
):
    """
    Tests for Per-Status Actions.
    """

    from flask import json, Response

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        return Response('{"Hello": "World"}', status=status, mimetype="application/json")

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()

    endpoint_id = randint(0, 10000)
    field_value = 10

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": field_value, "endpoint_id": endpoint_id})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library=LIBRARY,
        request_endpoint=endpoint.recv_url(),
        output_field="/field1",
        per_status_actions=per_status_actions,
    )
    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)

    if success:
        cleanup(handler.stop_work, work)
        handler.start_work(work)
        output_records = wiretap.output_records
        assert 1 == len(output_records)
        assert {"Hello": "World"} == output_records[0].field.get('field1')
    else:
        with pytest.raises(RunError) as exception_info:
            handler.start_work(work)
            handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
        assert err in exception_info.value.message


@sdc_min_version("6.1.0")
@pytest.mark.parametrize(
    "per_status_actions, status, success, hits, err",
    per_status_action_parameters_unknown_status
)
def test_per_status_actions_unknown_status(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, success, hits, err):
    """
        Tests for Per-Status Actions for Unknown Status.
    """

    test_per_status_actions(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, success, hits, err)


@sdc_min_version("6.0.0")
@pytest.mark.parametrize(
    "per_status_actions, status, hits, err",
    retry_parameters
)
def test_per_status_actions_retry(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, hits, err
):
    """
    Tests for Per-Status Actions with Retry action selected.
    """

    from flask import json, Response

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        return Response('{"Hello": "World"}', status=status, mimetype="application/json")

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()

    endpoint_id = randint(0, 10000)
    field_value = 10

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": field_value, "endpoint_id": endpoint_id})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library=LIBRARY,
        request_endpoint=endpoint.recv_url(),
        output_field="/field1",
        per_status_actions=per_status_actions,
    )
    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)

    cleanup(handler.stop_work, work)
    handler.start_work(work)
    _wait_for_pipeline_statuses(sdc_executor, pipeline, ["FINISHED"])
    assert 0 == len(wiretap.output_records)
    error_records = wiretap.error_records
    assert 1 == len(error_records)
    for error_record in error_records:
        assert error_record.header._data.get("errorMessage").startswith(err), \
            f'{err} was expected instead it failed with {error_records[0].header._data.get("errorCode")}'



@sdc_min_version("6.1.0")
@pytest.mark.parametrize(
    "per_status_actions, status, hits, err",
    retry_parameters_unknown_status
)
def test_per_status_actions_retry_unknown_status(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, hits, err):
    """
    Tests for Per-Status Actions for Unknown Status with Retry action selected.
    """

    test_per_status_actions_retry(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, hits, err)



@pytest.mark.parametrize('mime_type, response_data, data_format', 
    [
        ('applicaton/json', '{}', 'JSON'),
        ('text/plain', '', 'TEXT'),
        ('applicaton/json', '', 'JSON'),
    ]
)
@sdc_min_version("6.0.0")
def test_endpoint_with_empty_data_response(
        sdc_builder, sdc_executor, cleanup, server, test_name, mime_type, response_data, data_format):
    
    """
    Verify that Endpoint with Empty Data Response passes the record to the next stages 
    if per-status actions are set accordingly.
    """

    from flask import json, Response

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        return Response(response_data, mimetype=mime_type)

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()

    endpoint_id = randint(0, 10000)
    field_value = 10

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
       data_format="JSON",
       stop_after_first_batch=True,
       raw_data=json.dumps({"field1": field_value, "endpoint_id": endpoint_id})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library=LIBRARY,
        request_endpoint=endpoint.recv_url(),
        output_field="/field1",
        per_status_actions=[
            {
                "codes": ["Default"],
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 5,
                "failure": "Error",
            }
        ],
        response_data_format=data_format,
    )
    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)

    cleanup(handler.stop_work, work)
    handler.start_work(work)
    _wait_for_pipeline_statuses(sdc_executor, pipeline, ["FINISHED"])
    output_records = wiretap.output_records
    assert 1 == len(output_records)
    if data_format == 'JSON':
        assert f'{output_records[0].field.get("field1")}' == response_data
    else:
        assert output_records[0].field.get('field1') == response_data


PER_TIMEOUT_ACTIONS = [
        [
            {
                "types": [
                    "Default"
                ],
                "timeout": "${unit:toMilliseconds(10, second)}",
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 5,
                "failure": "Error"
            }
        ],
        [
            {
                "types": [
                    "Default"
                ],
                "timeout": "${unit:toMilliseconds(10, second)}",
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 5,
                "failure": "Error"
            },
            {
                "types": [
                    "Request"
                ],
                "timeout": "${unit:toMilliseconds(10, second)}",
                "action": "Error",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 5,
                "failure": "Record"
            }
        ],
        [
            {
                "types": [
                    "Default"
                ],
                "timeout": "${unit:toMilliseconds(10, second)}",
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 5,
                "failure": "Error"
            },
            {
                "types": [
                    "Response"
                ],
                "timeout": "${unit:toMilliseconds(10, second)}",
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 5,
                "failure": "Error"
            }
        ],
        [
            {
                "types": [
                    "Default"
                ],
                "timeout": "${unit:toMilliseconds(10, second)}",
                "action": "ConstantRetry",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 5,
                "failure": "Record"
            }
        ],
        [
            {
                "types": [
                    "Default"
                ],
                "timeout": "${unit:toMilliseconds(10, second)}",
                "action": "ConstantRetry",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 5,
                "failure": "Error"
            }
        ],
    ]


@pytest.mark.parametrize('per_timeout_actions, endpoint_url, response_status, result',
                         [
                             (PER_TIMEOUT_ACTIONS[0], 'http://127.0.0.1:5000/down_endpoint', None, "output"),
                             (PER_TIMEOUT_ACTIONS[0], 'http://127.0.0.1:4000/down_endpoint', None, "output"),
                             (PER_TIMEOUT_ACTIONS[0], 'http://1.0.0.1:5000/down_endpoint', None, "output"),
                             (PER_TIMEOUT_ACTIONS[0], 'https://127.0.0.1:5000/down_endpoint', None, "output"),
                             (PER_TIMEOUT_ACTIONS[1], '', 408, "error"),
                             (PER_TIMEOUT_ACTIONS[2], '', 503, "output"),
                             (PER_TIMEOUT_ACTIONS[2], '', 504, "output"),
                             (PER_TIMEOUT_ACTIONS[3], 'http://127.0.0.1:5000/down_endpoint', None, "output"),
                             (PER_TIMEOUT_ACTIONS[4], 'http://127.0.0.1:5000/down_endpoint', None, "error")
                         ]
                         )
@sdc_min_version("6.0.0")
def test_per_timeout_actions_down_endpoint(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_timeout_actions, endpoint_url, response_status, result):

    """
    Verify that even if the Endpoint is down, per timeout action performs correctly.
    """

    from flask import json

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    if response_status is not None:
        def serve():
            return '', response_status

        endpoint = Endpoint(serve, ["GET"])
        server.start([endpoint])
        cleanup(server.stop)
        server.ready()
        endpoint_url = endpoint.recv_url()

    endpoint_id = randint(0, 10000)
    field_value = 10

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON",
        stop_after_first_batch=True,
        raw_data=json.dumps({"field1": field_value, "endpoint_id": endpoint_id})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library=LIBRARY,
        request_endpoint=endpoint_url,
        output_field="/output",
        per_status_actions=PER_STATUS_ACTIONS,
        per_timeout_actions=per_timeout_actions
    )
    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> webclient_processor >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)

    cleanup(handler.stop_work, work)
    handler.start_work(work)
    _wait_for_pipeline_statuses(sdc_executor, pipeline, ["FINISHED"])
    if result == "output":
        output_records = wiretap.output_records
        assert 1 == len(output_records)
        output_record = output_records[0]
        assert output_record.field.get("field1") == field_value
        assert output_record.field.get("endpoint_id") == endpoint_id
        assert output_record.field.get("output") == ""
    else:
        error_records = wiretap.error_records
        assert 1 == len(error_records)
        error_record = error_records[0]
        assert error_record.field.get("field1") == field_value
        assert error_record.field.get("endpoint_id") == endpoint_id
        assert error_record.header._data.get("errorMessage").startswith("WEB_CLIENT_RUNTIME_0067"), \
            f'WEB_CLIENT_RUNTIME_0067 was expected instead it failed with {error_records[0].header._data.get("errorCode")}'


@sdc_min_version("5.11.0")
@pytest.mark.parametrize('event_type', ['start', 'next-page', 'finished'])
def test_webclient_events(sdc_builder, sdc_executor, cleanup, server, test_name, event_type):
    """
    We test that WebClient sends
        a) Start Event before starting the pipeline
        b) Next Page Event when the next page is loaded
        c) Finished Event when the pipeline finishes
    """

    from flask import json, Response

    content_type = "application/json"
    response_data = """
{
"id":1,"employee_name":"John Doe","employee_salary":320800,"employee_age":41
}
"""

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        return Response(response_data, content_type=content_type)

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    endpoint_id = randint(0, 10000)
    field_value = 10

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON",
        stop_after_first_batch=True,
        raw_data=json.dumps({"field1": field_value, "endpoint_id": endpoint_id})
    )

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library=LIBRARY,
        request_endpoint=url,
        output_field="/output",
        per_status_actions=PER_STATUS_ACTIONS,
        response_data_format="JSON",
    )

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> webclient_processor >> wiretap.destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.react_to_events = True
    finisher.event_type = event_type
    finisher.stage_record_preconditions = ["${record:eventType() == '" + event_type + "'}"]
    webclient_processor >= finisher

    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    output_records = wiretap.output_records
    assert 1 == len(output_records)
    output = output_records[0].field.get("output")
    assert 1 == output.get("id")
    assert "John Doe" == output.get("employee_name")
    assert 320800 == output.get("employee_salary")
    assert 41 == output.get("employee_age")


@database('mysql')
def test_to_verify_webclient_processes_offsets_correctly(sdc_builder, sdc_executor, cleanup, server, test_name, database):
    """
     Test to verify that the WebClient Processor processes the Initial Offset correctly i.e.
     a. input records successfully pass through the pipeline and gets augmented with the output obtained by invoking
        the specified URL using the WebClient Processor.
     b. it doesn't throw the below exception:
         StageException: OFFSET_023 - String offset for offset class WebClientOffset could not be deserialized neither
         with the current nor the legacy mode
     Related intervention : INT-3234
    """

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    num_records = 8
    input_data = [{'id': i, 'name': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} ORDER BY id ASC'

    # Create pipeline
    origin = pipeline_builder.add_stage('JDBC Query Consumer')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          max_batch_size_in_records=10)

    output_response = ''

    def serve():
        return output_response, 200

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    endpoint_url = endpoint.recv_url()

    webclient_processor = pipeline_builder.add_stage(WEB_CLIENT, type="processor")
    webclient_processor.set_attributes(
        library=LIBRARY,
        request_endpoint=endpoint_url,
        output_field="/output",
        per_status_actions=PER_STATUS_ACTIONS
    )

    wiretap = pipeline_builder.add_wiretap()
    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")

    origin >> webclient_processor >> wiretap.destination
    origin >= finisher

    pipeline = pipeline_builder.build(test_name).configure_for_environment(database)
    work = handler.add_pipeline(pipeline)

    try:
        # Create and populate table
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name,
                                 sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('name', sqlalchemy.String(32)))
        table.create(database.engine)
        connection = database.engine.connect()
        connection.execute(table.insert(), input_data)

        cleanup(handler.stop_work, work)
        handler.start_work(work)
        _wait_for_pipeline_statuses(sdc_executor, pipeline, ["FINISHED"])
        sdc_records = [record.field
                       for record in wiretap.output_records]
        assert len(sdc_records) == len(input_data)
        for record in sdc_records:
            for input in input_data:
                if record.get('id') == input.get('id'):
                    assert record.get('name') == input.get('name')
                    assert record.get('output') == output_response

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
