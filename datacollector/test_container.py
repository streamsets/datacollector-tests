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


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        # Test only config properties and environmental values
        data_collector.sdc_properties['test.property'] = 'some value'
        data_collector.sdc_properties['test.password'] = 'super secrete value'
        data_collector.docker_env_vars['TEST_PROPERTY'] = 'some value'
        # Configuring UI (for test purpose)
        data_collector.sdc_properties['ui.header.title'] = '${env("TEST_PROPERTY")}'
    return hook

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


# SDC-11914: Disable directory listing for static resources
def test_directory_listing(sdc_executor):
    h1 = http.client.HTTPConnection(sdc_executor.api_client.server_url.split('://')[-1])
    h1.request('GET', '/asserts')

    resp = h1.getresponse()
    assert 404 == resp.status


def test_configuration(sdc_executor):
    """Ensure that we can retrieve configuration properties and sensitive ones are properly masked."""
    configuration = sdc_executor.sdc_configuration

    assert configuration['test.property'] == 'some value'
    assert configuration['test.password'] == '-- MASKED --'


def test_ui_configuration(sdc_executor):
    """Ensure that we can retrieve the UI configuration properties and they are not masked in any way."""
    configuration = sdc_executor.api_client.get_sdc_ui_configuration().response.json()

    assert 'ui.debug' in configuration
    assert 'ui.server.timezone' in configuration
    assert configuration['ui.header.title'] == 'some value'