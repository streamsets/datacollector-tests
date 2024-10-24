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
from random import randint
from streamsets.testframework.markers import sdc_min_version, web_client
from streamsets.sdk.exceptions import RunError

from stage import _wait_for_pipeline_statuses
from stage.test_webclient_origin import per_status_action_parameters, per_status_action_parameters_unknown_status
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
from streamsets.testframework.utils import Version

import logging

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
        [
            "Expression",
            '{"hello": "there"}',
            "application/json",
            "${record:value('/method')}",
            "${record:value('/body')}",
        ],
    ],
)
def test_http_methods(
    sdc_builder,
    sdc_executor,
    cleanup,
    server,
    test_name,
    method,
    body,
    content_type,
    method_expression,
    body_expression,
):

    """Test the HTTP methods of the destination.
    The body and optionally the method are taken from the input record."""

    from flask import json, request

    timeout = DEFAULT_TIMEOUT_IN_SEC
    input_data = {"method": method if method != "Expression" else "POST", "body": body}
    raw_data = json.dumps(input_data)
    default_response = {"general": "kenobi"}
    expected_request_body = json.loads(body) if body is not None else None

    def get():
        # In this case we only want to ensure the request was received,
        # we don't care about the body.
        return json.dumps({}), 200

    def post():
        # We care about both the request and the request body.
        request_body = {}
        try:
            request_body = json.loads(request.data)
        except:
            pytest.fail(f"Failed to load request data: '{request.data}'.")
        if request_body == expected_request_body:
            return {}
        else:
            pytest.fail(f"Mismatched request data. Expected '{expected_request_body}', got '{request_body}'.")

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
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(data_format="JSON", stop_after_first_batch=True, raw_data=raw_data)

    webclient_destination = pipeline_builder.add_stage(WEB_CLIENT, type="destination")
    webclient_destination.set_attributes(
        library=LIBRARY,
        request_endpoint=f"{server.url}/{endpoint.path}",
        method=method,
        per_status_actions=PER_STATUS_ACTIONS,
    )
    if body is not None:
        if body_expression is None:
            webclient_destination.set_attributes(request_body=json.dumps(body))
        else:
            webclient_destination.set_attributes(request_body=body_expression)
    if method_expression is not None:
        webclient_destination.set_attributes(method_expression=method_expression)
    if content_type is not None:
        # We don't need to set the header as JSON is set by default,
        # but let's make it explicit to make changing this test easier.
        webclient_destination.set_attributes(
            common_headers=[{"commonHeaderName": "Content-Type", "commonHeaderValue": "application/json"}]
        )

    dev_raw_data_source >> webclient_destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)

    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=timeout)


def test_common_header(sdc_builder, sdc_executor, cleanup, server, test_name):
    """
    Verify common headers are included in the request.
    """

    from flask import json

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    endpoint = Endpoint(verify_header, ["GET"], 'verify-header')
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": 10})
    )

    webclient_destination = pipeline_builder.add_stage(WEB_CLIENT, type="destination")
    webclient_destination.set_attributes(
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
        per_status_actions=PER_STATUS_ACTIONS,
    )

    dev_raw_data_source >> webclient_destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)


def test_security_header(sdc_builder, sdc_executor, cleanup, server, test_name):
    """
    Verify security headers are included in the request.
    """

    from flask import json

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    endpoint = Endpoint(verify_header, ["GET"], 'verify-header')
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": 10})
    )

    webclient_destination = pipeline_builder.add_stage(WEB_CLIENT, type="destination")
    webclient_destination.set_attributes(
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
        per_status_actions=PER_STATUS_ACTIONS,
    )

    dev_raw_data_source >> webclient_destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)


def test_common_and_security_header(sdc_builder, sdc_executor, cleanup, server, test_name):
    """
    Verify common and security headers are included in the request.
    """

    from flask import json

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    endpoint = Endpoint(verify_header, ["GET"], 'verify-header')
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": 10})
    )

    webclient_destination = pipeline_builder.add_stage(WEB_CLIENT, type="destination")
    webclient_destination.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        security_headers=[{"securityHeaderName": "header1", "securityHeaderValue": "some_value1"}],
        common_headers=[{"commonHeaderName": "header2", "commonHeaderValue": "some_value2"}],
        per_status_actions=PER_STATUS_ACTIONS,
    )

    dev_raw_data_source >> webclient_destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)


@sdc_min_version("5.11.0")
@pytest.mark.parametrize(
    "per_status_actions, status, success, hits, err",
    per_status_action_parameters
)
def test_per_status_actions( sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status,
                             success, hits, err, retry=False):
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

    dev_raw_data_source = pipeline_builder.add_stage("Dev Raw Data Source")
    dev_raw_data_source.set_attributes(
        data_format="JSON", stop_after_first_batch=True, raw_data=json.dumps({"field1": 10})
    )

    webclient_destination = pipeline_builder.add_stage(WEB_CLIENT, type="destination")
    webclient_destination.set_attributes(
        library=LIBRARY,
        request_endpoint=endpoint.recv_url(),
        per_status_actions=per_status_actions,
    )

    dev_raw_data_source >> webclient_destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)

    if success:
        cleanup(handler.stop_work, work)
        handler.start_work(work)
        _wait_for_pipeline_statuses(sdc_executor, pipeline, ["FINISHED"])
        history = sdc_executor.get_pipeline_history(pipeline)
        if retry:
            assert history.latest.metrics.counter('stage.WebClient_01.outputRecords.counter').count == 0
            assert history.latest.metrics.counter('stage.WebClient_01.errorRecords.counter').count == 1
        else:
            assert history.latest.metrics.counter('stage.WebClient_01.outputRecords.counter').count == 1
            assert history.latest.metrics.counter('stage.WebClient_01.errorRecords.counter').count == 0
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
def test_per_status_actions_unknown_status( sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status,
                             success, hits, err):
    """
    Tests for Per-Status Actions for Unknown Status.
    """

    test_per_status_actions(sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, success, hits, err)


@sdc_min_version("6.0.0")
@pytest.mark.parametrize(
    "per_status_actions, status, success, hits, err",
    [
        [  # INT-3079. Testing the Case in Case of Bad Request Response
            [
                {
                    "codes": ["HTTP_201", "HTTP_202", "Successful"],
                    "action": "ConstantRetry",
                    "backoff": "${unit:toMilliseconds(1, second)}",
                    "retries": 5,
                    "failure": "Error",
                },
                {
                    "codes": ["Default"],
                    "action": "ConstantRetry",
                    "backoff": "${unit:toMilliseconds(1, second)}",
                    "retries": 5,
                    "failure": "Error",
                },
            ], 400, True, 1, None,
        ],
        [  # INT-3079. Testing the Case in Case of Success Response
            [
                {
                    "codes": ["HTTP_201", "HTTP_202", "Successful"],
                    "action": "ConstantRetry",
                    "backoff": "${unit:toMilliseconds(1, second)}",
                    "retries": 5,
                    "failure": "Error",
                },
                {
                    "codes": ["Default"],
                    "action": "ConstantRetry",
                    "backoff": "${unit:toMilliseconds(1, second)}",
                    "retries": 5,
                    "failure": "Error",
                },
            ], 200, True, 1, None,
        ]
    ]
)
def test_per_status_actions_retry(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, success, hits, err
):
    """
    Tests for Per-Status Actions with Retry action selected.
    """

    test_per_status_actions(sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, success, hits, err, True)



@sdc_min_version("6.1.0")
@pytest.mark.parametrize(
    "per_status_actions, status, success, hits, err",
    [
        [
            [
                {
                    "codes": [
                        "Default"
                    ],
                    "action": "ConstantRetry",
                    "backoff": "${unit:toMilliseconds(1, second)}",
                    "retries": 5,
                    "failure": "Error"
                }
            ], 599, True, 1, None
        ],
        [
            [
                {
                    "codes": [
                        "Default"
                    ],
                    "action": "Record",
                    "backoff": "${unit:toMilliseconds(1, second)}",
                    "retries": 5,
                    "failure": "Error"
                },
                {
                    "codes": [
                        "Unknown"
                    ],
                    "action": "ConstantRetry",
                    "backoff": "${unit:toMilliseconds(1, second)}",
                    "retries": 5,
                    "failure": "Error"
                }
            ], 599, True, 1, None
        ]
    ]
)
def test_per_status_actions_retry_unknown_status(
        sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, success, hits, err
):
    """
    Tests for Per-Status Actions for Unknown Status with Retry action selected.
    """

    test_per_status_actions(sdc_builder, sdc_executor, cleanup, server, test_name, per_status_actions, status, success, hits, err, True)


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

    webclient_destination = pipeline_builder.add_stage(WEB_CLIENT, type="destination")
    webclient_destination.set_attributes(
        library=LIBRARY,
        request_endpoint=url,
        per_status_actions=PER_STATUS_ACTIONS,
    )

    dev_raw_data_source >> webclient_destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.react_to_events = True
    finisher.event_type = event_type
    finisher.stage_record_preconditions = ["${record:eventType() == '" + event_type + "'}"]
    webclient_destination >= finisher

    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    history = sdc_executor.get_pipeline_history(pipeline)
    assert history.latest.metrics.counter('stage.WebClient_01.outputRecords.counter').count == 1
