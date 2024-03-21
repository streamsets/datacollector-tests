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

from stage.utils.webclient import deps, free_port, server, Endpoint, LIBRARY, RELEASE_VERSION, WEB_CLIENT
from stage.utils.common import cleanup, test_name
from stage.utils.utils_migration import LegacyHandler as PipelineHandler

import logging

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
        library=LIBRARY, request_endpoint=f"{server.url}/{endpoint.path}", method=method
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
