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
import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True}])
def test_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connection_timeout_in_ms(sdc_builder, sdc_executor):
    pass


@stub
def test_control_hub_api_url(sdc_builder, sdc_executor):
    pass


@stub
def test_control_hub_user_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': False}, {'enable_request_logging': True}])
def test_enable_request_logging(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_headers(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'http_method': 'DELETE'},
                                              {'http_method': 'EXPRESSION'},
                                              {'http_method': 'GET'},
                                              {'http_method': 'POST'},
                                              {'http_method': 'PUT'}])
def test_http_method(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'http_method': 'EXPRESSION'}])
def test_http_method_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_key_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keystore_type': 'JKS', 'use_tls': True},
                                              {'keystore_type': 'PKCS12', 'use_tls': True}])
def test_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': True, 'log_level': 'ALL'},
                                              {'enable_request_logging': True, 'log_level': 'CONFIG'},
                                              {'enable_request_logging': True, 'log_level': 'FINE'},
                                              {'enable_request_logging': True, 'log_level': 'FINER'},
                                              {'enable_request_logging': True, 'log_level': 'FINEST'},
                                              {'enable_request_logging': True, 'log_level': 'INFO'},
                                              {'enable_request_logging': True, 'log_level': 'OFF'},
                                              {'enable_request_logging': True, 'log_level': 'SEVERE'},
                                              {'enable_request_logging': True, 'log_level': 'WARNING'}])
def test_log_level(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': True}])
def test_max_entity_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_maximum_parallel_requests(sdc_builder, sdc_executor):
    pass


@stub
def test_maximum_request_time_in_sec(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_output_field(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{}, {'use_proxy': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_uri(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_read_timeout_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'http_method': 'DELETE'},
                                              {'http_method': 'EXPRESSION'},
                                              {'http_method': 'POST'},
                                              {'http_method': 'PUT'}])
def test_request_data(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True}])
def test_transport_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_truststore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_truststore_trust_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'truststore_type': 'JKS', 'use_tls': True},
                                              {'truststore_type': 'PKCS12', 'use_tls': True}])
def test_truststore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True},
                                              {'use_default_cipher_suites': True, 'use_tls': True}])
def test_use_default_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True},
                                              {'use_default_protocols': True, 'use_tls': True}])
def test_use_default_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': False}, {'use_proxy': True}])
def test_use_proxy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': True, 'verbosity': 'HEADERS_ONLY'},
                                              {'enable_request_logging': True, 'verbosity': 'PAYLOAD_ANY'},
                                              {'enable_request_logging': True, 'verbosity': 'PAYLOAD_TEXT'}])
def test_verbosity(sdc_builder, sdc_executor, stage_attributes):
    pass

