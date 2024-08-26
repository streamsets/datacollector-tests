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
from streamsets.testframework.markers import sdc_min_version, web_client
from streamsets.sdk.exceptions import RunError

from stage import _wait_for_pipeline_statuses
from stage.test_webclient_origin import constant_retry_parameters, per_status_action_parameters
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
import requests
from random import randint

DEFAULT_TIMEOUT_IN_SEC = 30

logger = logging.getLogger(__name__)
pytestmark = [sdc_min_version(RELEASE_VERSION), web_client]


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


@sdc_min_version("6.0.0")
@pytest.mark.parametrize(
    "per_status_actions, status, hits, err",
    constant_retry_parameters
)
def test_per_status_actions_constant_retry(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, hits, err
):

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
        assert error_record.header._data.get("errorMessage").startswith("WEB_CLIENT_RUNTIME_0067"), \
            f'WEB_CLIENT_RUNTIME_0067 was expected instead it failed with {error_records[0].header._data.get("errorCode")}'
