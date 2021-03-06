# Copyright 2021 StreamSets Inc.
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
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import Version

APPLICATION_ID = 'keanu'
HTTP_LISTENING_PORT = 8000
STATUS_CODE = 867

@sdc_min_version('3.4.0')
def test_status_code(sdc_builder, sdc_executor):
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()

        rest_service = pipeline_builder.add_stage('REST Service')

        if Version(sdc_builder.version) < Version('3.16.0'):
            rest_service.application_id = APPLICATION_ID
        else:
            rest_service.list_of_application_ids = [{"credential": APPLICATION_ID}]

        rest_service.http_listening_port = HTTP_LISTENING_PORT

        send_response_to_origin = pipeline_builder.add_stage('Send Response to Origin')
        send_response_to_origin.status_code = STATUS_CODE

        rest_service >> send_response_to_origin
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        protocol = 'https' if sdc_executor.https else 'http'
        rest_service_url = f'{protocol}://{sdc_executor.server_host}:{HTTP_LISTENING_PORT}'
        assert requests.get(rest_service_url,
                            headers={'X-SDC-APPLICATION-ID': APPLICATION_ID}).status_code == STATUS_CODE
    finally:
        sdc_executor.stop_pipeline(pipeline)

