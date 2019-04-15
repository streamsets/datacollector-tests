# Copyright 2019 StreamSets Inc.
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

import http.client
import pytest
from streamsets.testframework.markers import sdc_min_version

pytestmark = sdc_min_version('3.8.0')

# Main SDC WebServerTask should refuse connection requests
# using HTTP methods TRACK and TRACE
@pytest.mark.parametrize(('method'), [
    'TRACK',
    'TRACE'
])
def test_restricted_http_methods(sdc_executor, method):
    h1 = http.client.HTTPConnection(sdc_executor.api_client.server_url.split('://')[-1])
    h1.request(method, '/')

    resp = h1.getresponse()
    assert resp.status == 405

@pytest.mark.parametrize('endpoint', [
    '/',
    '/collector/assets/assets/assets/favicon.png'
])
def test_frame_options(sdc_executor, endpoint):
    h1 = http.client.HTTPConnection(sdc_executor.api_client.server_url.split('://')[-1])
    h1.request('GET', endpoint)

    resp = h1.getresponse()
    assert 'DENY' == resp.getheader('X-Frame-Options')
