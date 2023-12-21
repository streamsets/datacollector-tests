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

import pytest
import string

from pretenders.common.constants import FOREVER
from streamsets.testframework.markers import http, sdc_min_version
from streamsets.testframework.utils import get_random_string
from streamsets.sdk.exceptions import RunError

LIBRARY = 'streamsets-datacollector-webclient-impl-okhttp-lib'

JSON_DATA = {
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


@sdc_min_version("5.9.0")
@pytest.mark.parametrize('data_length', [-1, 2048, 256])
@http
def test_okhttp_webclient_origin_response_data_max_size(sdc_builder, sdc_executor, http_client, data_length):
    """
    Test to verify field Max Data Size (bytes) works. In first case, -1 represents that the field is not set, so the
    size wouldn't be checked. In second case, it checks as the configured limit is greater than 0 but let
    it pass through as the response size(1028) is within the configured limit(2048). In the last case, as the size is
    exceeding the configured limit(256).
    """
    expected_data = json.dumps(JSON_DATA)
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()
    http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
    mock_uri = f'{http_mock.pretend_url}/{mock_path}'
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Web Client', type='origin', library=LIBRARY)
    origin.set_attributes(
        request_endpoint=mock_uri,
        request_data_format='JSON',
        method='Get',
        ingestion_mode='Batch',
        response_data_format='JSON',
        max_data_length_in_bytes=data_length
    )
    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        if 0 < data_length < 1028:
            with pytest.raises(RunError) as exception_info:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert 'WEB_CLIENT_RUNTIME_0052' in exception_info.value.message, \
                f'Expected error WEB_CLIENT_RUNTIME_0052 due to Response Data exceeding configured bytes ({data_length})'
        else:
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 1, timeout_sec=120)
            sdc_executor.stop_pipeline(pipeline)
            records = wiretap.output_records
            assert 0 != len(records), 'Expected output records, but found none)'
    finally:
        sdc_executor.remove_pipeline(pipeline)
        http_mock.delete_mock()
