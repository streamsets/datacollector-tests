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

from pretenders.common.constants import FOREVER

from streamsets.testframework.markers import sdc_min_version, sdc_min_version, web_client
from streamsets.testframework.utils import get_random_string
from streamsets.sdk.exceptions import RunError

from stage.utils.webclient import deps, free_port, server, Endpoint, LIBRARY, RELEASE_VERSION, WEB_CLIENT
from stage.utils.common import cleanup, test_name
from stage.utils.utils_migration import LegacyHandler as PipelineHandler

WEB_CLIENT = "Web Client"
DEFAULT_TIMEOUT_IN_SEC = 30

logger = logging.getLogger(__name__)
pytestmark = [sdc_min_version(RELEASE_VERSION), web_client]


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
        max_data_length_in_bytes=data_length
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
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        pagination_mode="None",
    )
    wiretap = pipeline_builder.add_wiretap()
    webclient_origin >> wiretap.destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.react_to_events = True
    finisher.on_record_error = "STOP_PIPELINE"
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
