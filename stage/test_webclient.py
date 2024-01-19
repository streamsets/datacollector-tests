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
from streamsets.testframework.markers import sdc_min_version

from stage.utils.webclient import deps, free_port, server, Endpoint
from stage.utils.common import cleanup, test_name
from stage.utils.utils_migration import LegacyHandler as PipelineHandler

import logging
import requests

RELEASE_VERSION = "5.10.0"
WEB_CLIENT = "Web Client"
DEFAULT_TIMEOUT_IN_SEC = 30
LIBRARY = "streamsets-datacollector-webclient-impl-okhttp-lib"

logger = logging.getLogger(__name__)
pytestmark = [sdc_min_version(RELEASE_VERSION)]


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
        ("DELIMITED", "application/delimited", """
1,John Doe,30,New York
2,Jane Smith,25,San Francisco
"""),
        ("JSON", "application/json", """
{
"id":1,"employee_name":"John Doe","employee_salary":320800,"employee_age":41
}
"""),
        ("TEXT", "application/text", """
This is a sample text document.
It can have multiple lines and characters.
"""),
        ("XML", "application/xml", """
<person>
  <name>John Doe</name>
  <age>30</age>
  <city>New York</city>
</person>
"""),
       ("LOG", "application/log", """200 [main] DEBUG org.StreamSets.Log4j unknown - This is a sample log message"""),
       ("BINARY", "application/octet-stream", """b'\x00\x01\x02\x03\x04'""")
 ],
)
def test_webclient_origin_response_data_format(sdc_builder, sdc_executor, cleanup, server, test_name,
                                               data_format, content_type, response_data ):

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
        library="streamsets-datacollector-webclient-impl-okhttp-lib",
        request_endpoint=url,
        max_batch_size_in_records=1,
        batch_wait_time_in_ms=10,
        ingestion_mode="Batch",
        response_data_format=data_format
    )
    #if log data format
    if data_format == "LOG":
        webclient_origin.log_format="LOG4J"

    #if binary data format
    if data_format == "BINARY":
        webclient_origin.collect_mode="Bytes"

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
