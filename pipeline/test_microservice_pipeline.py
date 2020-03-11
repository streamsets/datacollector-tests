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

import http.client as httpclient

from streamsets.testframework.markers import sdc_min_version

@sdc_min_version('3.14.0')
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


@sdc_min_version('3.14.0')
def test_microservice_pipeline_response(sdc_builder, sdc_executor):
    """Test Microservice Pipeline Response. The pipeline would look like:

        rest_service_source >> send_response_target

    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    rest_service_source = pipeline_builder.add_stage('REST Service')
    rest_service_source.http_listening_port = 8234
    rest_service_source.application_id = 'TEST_ID_FIRST'

    send_response_target = pipeline_builder.add_stage('Send Response to Origin')
    send_response_target.status_code = 201
    send_response_target.response_headers = [{'key': 'SAMPLE_RESPONSE_HEADER', 'value': 'test'}]

    rest_service_source >> send_response_target

    pipeline = pipeline_builder.build('REST Sample Pipeline')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.validate_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)

        # Try a GET request using sample data with a valid Application-ID and we should expect
        # a custom response 201 and custom response header.
        http_res = httpclient.HTTPConnection(sdc_executor.server_host, 8234)
        http_res.request('GET', '/', '{"f1": "abc"}', {'X-SDC-APPLICATION-ID': 'TEST_ID_FIRST'})
        resp = http_res.getresponse()
        assert resp.status == 201
        assert resp.getheader('SAMPLE_RESPONSE_HEADER') == 'test'

    finally:
        sdc_executor.stop_pipeline(pipeline)
