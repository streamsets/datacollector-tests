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
import json
import logging
import pytest
import requests
import string
import time

from pretenders.common.constants import FOREVER

from streamsets.sdk.exceptions import RunError
from streamsets.testframework.markers import sdc_min_version, sdc_min_version, web_client
from streamsets.testframework.utils import get_random_string, Version
from streamsets import sdk

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

WEB_CLIENT = "Web Client"
DEFAULT_TIMEOUT_IN_SEC = 30

logger = logging.getLogger(__name__)
pytestmark = [sdc_min_version(RELEASE_VERSION), web_client]


@pytest.fixture(autouse=True)
def skip_5_11_tests(sdc_executor):
    if Version(sdc_executor.version) in (Version('5.11.0'), Version('5.12.0')):
        pytest.skip('This test is expected to fail in Version 5.11.0 as it got fixed in'
                    ' https://review.streamsets.net/c/datacollector/+/77887')


def test_hello_world(sdc_builder, sdc_executor, cleanup, server, test_name):

    from flask import json

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        return json.dumps({"Hello": "There"})

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)


@pytest.mark.parametrize(
    "data_format, content_type, response_data",
    [
        (
            "DELIMITED",
            "application/delimited",
            """
1,John Doe,30,New York
2,Jane Smith,25,San Francisco
""",
        ),
        (
            "JSON",
            "application/json",
            """
{
"id":1,"employee_name":"John Doe","employee_salary":320800,"employee_age":41
}
""",
        ),
        (
            "TEXT",
            "application/text",
            """
This is a sample text document.
It can have multiple lines and characters.
""",
        ),
        (
            "XML",
            "application/xml",
            """
<person>
  <name>John Doe</name>
  <age>30</age>
  <city>New York</city>
</person>
""",
        ),
        ("LOG", "application/log", """200 [main] DEBUG org.StreamSets.Log4j unknown - This is a sample log message"""),
        ("BINARY", "application/octet-stream", """b'\x00\x01\x02\x03\x04'"""),
    ],
)
def test_webclient_origin_response_data_format(
    sdc_builder, sdc_executor, cleanup, server, test_name, data_format, content_type, response_data
):

    from flask import Response

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        return Response(response_data, content_type=content_type)

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        response_data_format=data_format,
        per_status_actions=PER_STATUS_ACTIONS,
    )
    # if log data format
    if data_format == "LOG":
        webclient_origin.log_format = "LOG4J"

    # if binary data format
    if data_format == "BINARY":
        webclient_origin.collect_mode = "Bytes"

    wiretap = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    response = requests.get(endpoint.url)
    assert response.status_code == 200
    assert response.text == response_data


@pytest.mark.parametrize('grant_type', ['ClientCredentials', 'OwnerCredentials', 'AccessToken'])
def test_webclient_origin_oauth2_authentication_scheme(sdc_builder, sdc_executor, cleanup, server, test_name,
                                                          web_client, grant_type):

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()


    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=f'{web_client.has_base_url}/oauth2/authenticate',
        authentication_scheme = 'OAuth2',
        grant_type = grant_type,
        token_endpoint=f'{web_client.has_base_url}/oauth2/server/token',
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        per_status_actions=PER_STATUS_ACTIONS,
    )

    if grant_type == 'ClientCredentials':
      webclient_origin.set_attributes(
          client_id=web_client.oauth2.client_id,
          client_secret=web_client.oauth2.client_secret,
          additional_parameters=[{"additionalParameterName": "scope",
                                  "additionalParameterValue": web_client.oauth2.scope}]
          )
    elif grant_type == 'OwnerCredentials':
      webclient_origin.set_attributes(
          owner_client_id=web_client.oauth2.client_id,
          owner_client_secret=web_client.oauth2.client_secret,
          owner_user=web_client.oauth2.resource_owner_username,
          owner_password=web_client.oauth2.resource_owner_password,
          additional_parameters=[{"additionalParameterName": "scope",
                                  "additionalParameterValue": web_client.oauth2.scope}]
          )
    elif grant_type == 'AccessToken':
      jwtHeader = web_client.oauth2.jwt.header
      jwtPayload = web_client.oauth2.jwt.payload
      webclient_origin.set_attributes(
          token_claims = f'{{"iss": "{jwtPayload.iss}", '
                         f'"sub": "{jwtPayload.sub}", '
                         f'"aud": "{jwtPayload.aud}", '
                         f'"exp": {jwtPayload.exp}}}' ,
          token_headers = f'{{"alg": "{jwtHeader.alg}", '
                          f'"typ": "{jwtHeader.typ}"}}',
          signing_algorithm= jwtHeader.alg,
          signing_key= web_client.oauth2.jwt.secret_key
          )
    wiretap = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    output_records = [record.field for record in wiretap.output_records]
    assert 'Successfully authenticated' in output_records[0]['message'].value


@sdc_min_version(RELEASE_VERSION)
@pytest.mark.parametrize('data_length', [-1, 2048, 256])
def test_okhttp_webclient_origin_response_data_max_size(sdc_builder, sdc_executor, cleanup, server, test_name, data_length):
    """
    Test to verify field Max Data Size (bytes) works. In first case, -1 represents that the field is not set, so the
    size wouldn't be checked. In second case, it checks as the configured limit is greater than 0 but let
    it pass through as the response size(1028) is within the configured limit(2048). In the last case, it fails as
    the size is exceeding the configured limit(256).
    """

    from flask import Response

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    builder = handler.get_pipeline_builder()

    response_data = """
{
    "id": 137,
    "uid": "ebfa3d12-f917-4c00-982c-4e3e0972b669",
    "password": "l8E9XqGHZV",
    "first_name": "Waylon",
    "last_name": "Miller",
    "username": "waylon.miller",
    "email": "waylon.miller@email.com",
    "avatar": "https://robohash.org/necessitatibusnostrumaperiam.png?size=300x300\u0026set=set1",
    "gender": "Male",
    "phone_number": "+257 261.650.5048",
    "social_insurance_number": "677711046",
    "date_of_birth": "1972-04-23",
    "employment": {"title": "Customer Retail Liaison", "key_skill": "Communication"},
    "address": {"city": "Hirtheport", "street_name": "Marvin Well", "street_address": "9970 Connelly Loaf", "zip_code": "52082-3266", "state": "Pennsylvania", "country": "United States", "coordinates": {"lat": 43.51951457479663, "lng": -87.25662671418463}},
    "credit_card": {"cc_number": "4439-3686-5474-1317"},
    "subscription": {"plan": "Essential", "status": "Idle", "payment_method": "Paypal", "term": "Monthly"}
}
"""

    def serve():
        return Response(response_data, content_type="application/json")

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    origin = builder.add_stage(WEB_CLIENT, type='origin', library=LIBRARY)
    origin.set_attributes(
        request_endpoint=url,
        request_data_format='JSON',
        method='Get',
        ingestion_mode='Batch',
        response_data_format='JSON',
        max_data_length_in_bytes=data_length,
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build(test_name)
    work = handler.add_pipeline(pipeline)

    if 0 < data_length < 1028:
        with pytest.raises(RunError) as exception_info:
            handler.sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert 'WEB_CLIENT_RUNTIME_0052' in exception_info.value.message, \
                f'Expected error WEB_CLIENT_RUNTIME_0052 due to Response Data exceeding configured bytes ({data_length})'
    else:
        cleanup(handler.stop_work, work)
        handler.start_work(work)
        handler.wait_for_metric(work, "output_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
        records = wiretap.output_records
        assert 0 != len(records), 'Expected output records, but found none)'


@pytest.mark.parametrize("pages, items_per_page", [[1, 1], [4, 3]])
def test_pagination_page(sdc_builder, sdc_executor, cleanup, server, test_name, pages, items_per_page):

    """We test the page pagination by checking pages are processed sequentially,
    and that every record in every page is generated.

    Pagination should stop at page number @pages."""

    from flask import json, request

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        page = int(request.args.get("page"))
        return json.dumps({"records": [{"i": page + i} for i in range(items_per_page)]})

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = f"{endpoint.recv_url()}?page=${{startAt}}"

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        pagination_mode="Page",
        result_field_path="/records",
        initial_page=0,
        final_page=pages,
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()
    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)

    handler.wait_for_metric(work, "input_record_count", pages, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    expected_records = [{"i": page + i} for page in range(pages) for i in range(items_per_page)]

    records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert len(records) == len(expected_records)
    assert all([er in records for er in expected_records])


@pytest.mark.parametrize("step, last", [[1, 1], [3, 12]])
def test_pagination_offset(sdc_builder, sdc_executor, cleanup, server, test_name, step, last):

    """We test the offset by ensuring the stage sends requests sequentially,
    checking that the offset is correctly incremented by @step and
    every record is present.

    Pagination should stop at record number @last."""

    from flask import json, request

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        try:
            logger.error(request.args)
            offset = int(request.args.get("offset"))
        except Exception as e:
            logger.error(e)
            raise e
        return json.dumps({"records": [{"i": i} for i in range(offset, offset + step)]})

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = f"{endpoint.recv_url()}?offset=${{startAt}}"

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    logger.error(url)
    logger.error(requests.get(url.replace("${startAt}", "0")).text)
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        pagination_mode="Offset",
        result_field_path="/records",
        initial_offset=0,
        final_offset=last,
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", last, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    expected_records = [{"i": i} for i in range(last)]

    records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert len(records) == len(expected_records)
    assert all([er in records for er in expected_records])


@pytest.mark.parametrize(
    "location, stop_condition, expected, generated",
    [
        ["body", "${false}", 4, 4],
        ["body", "${record:value('/page') == 3}", 3, 4],
        ["header", "${false}", 4, 4],
        ["header", "${record:value('/page') == 3}", 3, 4],
    ],
)
def test_linked_pagination(
    sdc_builder, sdc_executor, cleanup, server, test_name, location, stop_condition, expected, generated
):

    """Each response contains a link to the next request, either in the body or in the header."""

    from flask import json, Response

    # Each page will contain @sub_records records.
    sub_records = 3

    class LinkedEndpoint:
        def __init__(self, index, last=False):
            self.index = index
            self.last = last

        def serve_link_in_body(self):
            return json.dumps(
                {
                    "page": f"{self.index}",
                    "next": f"/link_{self.index + 1}" if not self.last else None,
                    "records": [{"page": self.index, "offset": self.index + i} for i in range(sub_records)],
                }
            )

        def serve_link_in_header(self):
            content = json.dumps(
                {
                    "page": f"{self.index}",
                    "records": [{"page": self.index, "offset": self.index + i} for i in range(sub_records)],
                }
            )
            response = Response(content)
            response.headers["next"] = f"/link_{self.index+1}" if not self.last else " "
            return response

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    linked_endpoints = [LinkedEndpoint(i, i >= generated) for i in range(1, generated + 1)]

    endpoints = [
        Endpoint(le.serve_link_in_body if location == "body" else le.serve_link_in_header, ["GET"], f"link_{le.index}")
        for le in linked_endpoints
    ]

    expected_records = [{"page": i, "offset": i + j} for i in range(1, expected + 1) for j in range(sub_records)]

    server.start(endpoints)
    cleanup(server.stop)
    server.ready()

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=f"{server.url}/link_1",
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        result_field_path="/records",
        next_page_link_base=server.url,
        stop_condition=stop_condition,
        per_status_actions=PER_STATUS_ACTIONS,
    )
    if location == "body":
        webclient_origin.set_attributes(pagination_mode="LinkInBody", next_page_link_field_path="/next")
    else:
        webclient_origin.set_attributes(pagination_mode="LinkInHeader", next_page_link_header="next")
    wiretap = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", expected, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert len(records) == len(expected_records)
    assert all([er in records for er in expected_records])


@pytest.mark.parametrize(
    "method, body, content_type, expression",
    [
        ["Get", None, None, None],
        ["Post", '{"hello": "there"}', "application/json", None],
        ["Put", '{"hello": "there"}', "application/json", None],
        ["Delete", None, None, None],
        ["Expression", '{"hello": "there"}', "application/json", "POST"],
    ],
)
def test_http_methods(sdc_builder, sdc_executor, cleanup, server, test_name, method, body, content_type, expression):

    from flask import json, request

    default_response = {"general": "kenobi"}
    expected_request_body = json.loads(body) if body is not None else None
    dumped_response = json.dumps(default_response)

    def get():
        return dumped_response

    def post():
        request_body = {}
        try:
            request_body = json.loads(request.json)
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

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=f"{server.url}/{endpoint.path}",
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10000,
        ingestion_mode="Batch",
        wait_time_between_requests_in_ms=10000,
        method=method,
        per_status_actions=PER_STATUS_ACTIONS,
    )
    if body is not None:
        webclient_origin.set_attributes(request_body=json.dumps(body))
    if expression is not None:
        webclient_origin.set_attributes(method_expression=expression)
    if content_type is not None:
        # We don't need to set the header as JSON is set by default,
        # but let's make it explicit to make changing this test easier.
        webclient_origin.set_attributes(
            common_headers=[{"commonHeaderName": "Content-Type", "commonHeaderValue": "application/json"}]
        )

    wiretap = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    records = [{k: v for k, v in record.field.items()} for record in wiretap.output_records]
    assert len(records) == len(expected_records)
    assert all([er in records for er in expected_records])


@sdc_min_version("5.10.0")
def test_no_more_data_event_on_pagination_none(sdc_builder, sdc_executor, cleanup, server, test_name):

    """
    We test that if pagination mode is NONE, WebClient will send a no-more-data event once all the records are
    fully processed.
    """

    from flask import Response

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

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10000,
        ingestion_mode="Batch",
        pagination_mode="None",
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()
    webclient_origin >> wiretap.destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.react_to_events = True
    finisher.on_record_error = "DISCARD"
    finisher.stage_record_preconditions = ["${record:eventType() == 'no-more-data'}"]
    webclient_origin >= finisher

    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    assert 1 == len(output_records)
    assert 1 == output_records[0].field.get("id")
    assert "John Doe" == output_records[0].field.get("employee_name")
    assert 320800 == output_records[0].field.get("employee_salary")
    assert 41 == output_records[0].field.get("employee_age")


@sdc_min_version("5.11.0")
@pytest.mark.parametrize('event_type', ['start', 'next-page', 'finished'])
def test_webclient_events(sdc_builder, sdc_executor, cleanup, server, test_name, event_type):
    """
    We test that WebClient sends
        a) Start Event before starting the pipeline
        b) Next Page Event when the next page is loaded
        c) Finished Event when the pipeline finishes
    """

    from flask import Response

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

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10000,
        ingestion_mode="Batch",
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()
    webclient_origin >> wiretap.destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.react_to_events = True
    finisher.event_type = event_type
    finisher.on_record_error = "DISCARD"
    finisher.stage_record_preconditions = ["${record:eventType() == '" + event_type + "'}"]
    webclient_origin >= finisher

    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    handler.start_work(work)
    if event_type == 'finished':
        handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    else:
        handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    if event_type == 'start':
        assert 0 == len(output_records)
    elif event_type == 'next-page':
        assert 1 == len(output_records)
        assert 1 == output_records[0].field.get("id")
        assert "John Doe" == output_records[0].field.get("employee_name")
        assert 320800 == output_records[0].field.get("employee_salary")
        assert 41 == output_records[0].field.get("employee_age")
    else:
        assert 1 == output_records[0].field.get("id")
        assert "John Doe" == output_records[0].field.get("employee_name")
        assert 320800 == output_records[0].field.get("employee_salary")
        assert 41 == output_records[0].field.get("employee_age")


def test_common_header(sdc_builder, sdc_executor, cleanup, server, test_name):
    """
    Verify common headers are included in the request.
    """

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    success_message = 'Success! headers are present'

    endpoint = Endpoint(verify_header, ["GET"], 'verify-header')
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
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
    wiretap = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    assert success_message == output_records[0].field


def test_security_header(sdc_builder, sdc_executor, cleanup, server, test_name):
    """
    Verify security headers are included in the request.
    """

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    success_message = 'Success! headers are present'

    endpoint = Endpoint(verify_header, ["GET"], 'verify-header')
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
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
    wiretap = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    assert success_message == output_records[0].field


def test_common_and_security_header(sdc_builder, sdc_executor, cleanup, server, test_name):
    """
    Verify security headers are included in the request.
    """

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    success_message = 'Success! headers are present'

    endpoint = Endpoint(verify_header, ["GET"], 'verify-header')
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = endpoint.recv_url()

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        security_headers=[{"securityHeaderName": "header1", "securityHeaderValue": "some_value1"}],
        common_headers=[{"commonHeaderName": "header2", "commonHeaderValue": "some_value2"}],
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    assert success_message == output_records[0].field


per_status_action_parameters = [
    [
        [  # Test groups.
            {
                "codes": ["Successful"],
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 3,
                "failure": "Abort",
            },
            {
                "codes": ["Default"],
                "action": "ConstantRetry",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 3,
                "failure": "Abort",
            },
        ],
        200, True, 1, None,
    ],
    [
        [  # Test another group..
            {
                "codes": ["ClientError"],
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 3,
                "failure": "Abort",
            },
            {
                "codes": ["Default"],
                "action": "ConstantRetry",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 3,
                "failure": "Abort",
            },
        ],
        400, True, 1, None,
    ],
    [  # Test individual behaviours
        [
            {
                "codes": ["HTTP_400"],
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 3,
                "failure": "Abort",
            },
            {
                "codes": ["Default"],
                "action": "Abort",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 3,
                "failure": "Abort",
            },
        ],
        400, True, 1, None,
    ],
    [  # Test abort.
        [
            {
                "codes": ["HTTP_200"],
                "action": "Abort",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 0,
                "failure": "Abort",
            },
            {
                "codes": ["Default"],
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 3,
                "failure": "Abort",
            },
        ], 200, False, 1, "WEB_CLIENT_RUNTIME_0069",
    ],
]


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

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=endpoint.recv_url(),
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        per_status_actions=per_status_actions,
    )
    wiretap = pipeline_builder.add_wiretap()
    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)

    if success:
        cleanup(handler.stop_work, work)
        handler.start_work(work)
        handler.wait_for_metric(work, "input_record_count", hits, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    else:
        with pytest.raises(RunError) as exception_info:
            handler.start_work(work)
            handler.wait_for_status(work, "FINISHED", timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
        assert err in exception_info.value.message


constant_retry_parameters = [
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
        ], 400, 1, None,
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
        ], 200, 1, None,
    ]
]


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

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=endpoint.recv_url(),
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10000,
        ingestion_mode="Batch",
        per_status_actions=per_status_actions,
    )
    wiretap = pipeline_builder.add_wiretap()
    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)

    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", hits, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    assert 0 == len(wiretap.output_records)
    error_records = wiretap.error_records
    assert 1 == len(error_records)
    for error_record in error_records:
        assert error_record.header._data.get("errorMessage").startswith("WEB_CLIENT_RUNTIME_0067"), \
        f'WEB_CLIENT_RUNTIME_0067 was expected instead it failed with {error_records[0].header._data.get("errorCode")}'


@sdc_min_version("6.0.0")
def test_endpoint_with_empty_data_response(
        sdc_builder, sdc_executor, cleanup, server, test_name):

    """
    Verify that Endpoint with Empty Data Response passes the record to the next stages
    if per-status actions are set accordingly.
    """

    from flask import Response

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        return Response('{}', mimetype='applicaton/json')

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=endpoint.recv_url(),
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        per_status_actions=[
            {
                "codes": ["Default"],
                "action": "Record",
                "backoff": "${unit:toMilliseconds(1, second)}",
                "retries": 5,
                "failure": "Error",
            }
        ],
        response_data_format='JSON',
    )
    wiretap = pipeline_builder.add_wiretap()
    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)

    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", 1, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    output_records = wiretap.output_records
    assert 0 < len(output_records)
    for output_record in output_records:
        assert f'{output_record.field}' == '{}'


@sdc_min_version("6.0.0")
@pytest.mark.parametrize('server_response, expected_records',
                         [
                             (
                                {
                                    "1": {"records": [{"some": "stuff"}]},
                                    "2": {}
                                },
                                [{"some": "stuff"}]
                            ),
                            (
                               {
                                   "1": {"records": [{"some": "stuff"}]},
                                   "2": {},
                                   "3": {"records": [{"other": "stuff2"}]},
                               },
                               [{"some": "stuff"}]
                            ),
                         ])
def test_pagination_abort_if_result_field_does_not_exist(sdc_builder, sdc_executor, cleanup, server, 
                                                         test_name, server_response, expected_records):

    """If the endpoint response does not contain the result field, the pipeline should stop"""

    from flask import json, request # type: ignore

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        return server_response[request.args.get("page")]

    endpoint = Endpoint(serve, ["GET"])
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = f"{endpoint.recv_url()}?page=${{startAt}}"

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        pagination_mode="Page",
        result_field_path="/records",
        on_missing_result_field="ABORT_PIPELINE",
        initial_page=1,
        final_page=4,
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()
    webclient_origin >> wiretap.destination
    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    with pytest.raises(sdk.exceptions.RunError) as run_error:
        handler.start_work(work)
        handler.wait_for_metric(work, "input_record_count", 2, timeout_sec=DEFAULT_TIMEOUT_IN_SEC)
    assert 'WEB_CLIENT_RUNTIME_0076' in str(run_error)
    wiretap_output_records = wiretap.output_records
    assert len(expected_records) == len(wiretap_output_records)
    assert all(expected_record == output_record.field 
               for expected_record, output_record in zip(expected_records, wiretap_output_records))


@sdc_min_version("6.0.0")
@pytest.mark.parametrize('server_response, expected_records',
                         [
                             (
                                {
                                    "1": {"records": [{"some": "stuff"}]},
                                    "2": {}
                                },
                                [{"some": "stuff"}]
                            ),
                            (
                               {
                                   "1": {"records": [{"some": "stuff"}]},
                                   "2": {},
                                   "3": {"records": [{"other": "stuff2"}]},
                               },
                               [{"some": "stuff"}]
                            ),
                         ])
def test_pagination_no_more_data_event_if_result_field_does_not_exist(sdc_builder, sdc_executor, cleanup, server, 
                                                         test_name, server_response, expected_records):

    """If the endpoint response does not contain the result field, the pipeline should stop"""

    from flask import json, request # type: ignore

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        return server_response[request.args.get("page")]

    endpoint = Endpoint(serve, ["GET"], capture_activity=True)
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = f"{endpoint.recv_url()}?page=${{startAt}}"

    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        pagination_mode="Page",
        result_field_path="/records",
        on_missing_result_field="NO_MORE_DATA_EVENT",
        initial_page=1,
        final_page=4,
        wait_time_between_requests_in_ms=500,
        per_status_actions=PER_STATUS_ACTIONS
    )
    wiretap = pipeline_builder.add_wiretap()
    finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    finisher.set_attributes(
        react_to_events=True,
        event_type='no-more-data',
        on_record_error='DISCARD',
        stage_record_preconditions=["${record:eventType() == 'no-more-data'}"]
    )

    webclient_origin >> wiretap.destination
    webclient_origin >= finisher

    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_status(work, "FINISHED", timeout_sec=30)

    endpoint_activity = endpoint.activity
    assert endpoint_activity.hits == 2
    assert "page=1" in endpoint_activity.activity_items[0].request_url 
    assert "page=2" in endpoint_activity.activity_items[1].request_url 

    wiretap_output_records = wiretap.output_records
    assert len(expected_records) == len(wiretap_output_records)
    assert all(expected_record == output_record.field 
               for expected_record, output_record in zip(expected_records, wiretap_output_records))

@sdc_min_version("6.0.0")
@pytest.mark.parametrize('server_response, expected_records,expected_number_missing_events',
                         [
                             (
                                {
                                    "1": {"records": [{"some": "stuff"}]},
                                    "2": {},
                                    "3": {},
                                    "4": {},
                                },
                                [{"some": "stuff"}],
                                3
                            ),
                            (
                               {
                                   "1": {"records": [{"some": "stuff"}]},
                                   "2": {},
                                   "3": {"records": [{"other": "stuff2"}]},
                                   "4": {},
                               },
                               [{"some": "stuff"}, {"other": "stuff2"}],
                               2
                            ),
                         ])
def test_pagination_missing_result_field_event_if_result_field_does_not_exist(sdc_builder, sdc_executor, cleanup, server, 
                                                                              test_name, server_response, expected_records, expected_number_missing_events):

    """If the endpoint response does not contain the result field, the pipeline should stop"""

    from flask import json, request # type: ignore

    handler = PipelineHandler(sdc_builder, sdc_executor, None, cleanup, test_name, logger)
    pipeline_builder = handler.get_pipeline_builder()

    def serve():
        page = request.args.get("page")
        return server_response[page] if page in server_response else {}

    endpoint = Endpoint(serve, ["GET"], capture_activity=True)
    server.start([endpoint])
    cleanup(server.stop)
    server.ready()
    url = f"{endpoint.recv_url()}?page=${{startAt}}"
    initial_page = 1
    final_page = 5
    webclient_origin = pipeline_builder.add_stage(WEB_CLIENT, type="origin")
    webclient_origin.set_attributes(
        library=LIBRARY,
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        pagination_mode="Page",
        result_field_path="/records",
        on_missing_result_field="MISSING_RESULT_FIELD_EVENT",
        initial_page=initial_page,
        final_page=final_page,
        per_status_actions=PER_STATUS_ACTIONS,
    )
    wiretap = pipeline_builder.add_wiretap()
    wiretap_events = pipeline_builder.add_wiretap()

    webclient_origin >> wiretap.destination
    webclient_origin >= wiretap_events.destination

    pipeline = pipeline_builder.build(test_name)

    work = handler.add_pipeline(pipeline)
    cleanup(handler.stop_work, work)
    handler.start_work(work)
    handler.wait_for_metric(work, "input_record_count", len(expected_records), timeout_sec=DEFAULT_TIMEOUT_IN_SEC)

    endpoint_activity = endpoint.activity
    assert endpoint_activity.hits == final_page - initial_page
    for i in range(initial_page, final_page):
        assert f"page={i}" in endpoint_activity.activity_items[i-1].request_url 

    wiretap_output_records = wiretap.output_records
    assert len(expected_records) == len(wiretap_output_records)
    assert all(expected_record == output_record.field 
               for expected_record, output_record in zip(expected_records, wiretap_output_records))

    missing_result_field_events = [r for r in wiretap_events.output_records 
                                   if r.header.values['sdc.event.type'] == 'missing-result-field']
    assert len(missing_result_field_events) == expected_number_missing_events
