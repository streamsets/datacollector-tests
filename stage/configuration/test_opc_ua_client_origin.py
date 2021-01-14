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
@pytest.mark.parametrize('stage_attributes', [{'nodeid_fetch_mode': 'MANUAL'}])
def test_(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_application_name(sdc_builder, sdc_executor):
    pass


@stub
def test_application_uri(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True}])
def test_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_client_private_key_alias(sdc_builder, sdc_executor):
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
def test_max_array_length(sdc_builder, sdc_executor):
    pass


@stub
def test_max_chunk_count(sdc_builder, sdc_executor):
    pass


@stub
def test_max_chunk_size(sdc_builder, sdc_executor):
    pass


@stub
def test_max_message_size(sdc_builder, sdc_executor):
    pass


@stub
def test_max_string_length(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'nodeid_fetch_mode': 'BROWSE'},
                                              {'nodeid_fetch_mode': 'FILE'},
                                              {'nodeid_fetch_mode': 'MANUAL'}])
def test_nodeid_fetch_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'nodeid_fetch_mode': 'FILE'}])
def test_nodeid_file_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'nodeid_fetch_mode': 'BROWSE'}])
def test_nodeid_refresh_interval_in_sec(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'processing_mode': 'POLLING'}])
def test_polling_interval_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'processing_mode': 'BROWSE_NODES'},
                                              {'processing_mode': 'POLLING'},
                                              {'processing_mode': 'SUBSCRIBE'}])
def test_processing_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_request_timeout(sdc_builder, sdc_executor):
    pass


@stub
def test_resource_url(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'nodeid_fetch_mode': 'BROWSE'}])
def test_root_node_identifier(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'nodeid_fetch_mode': 'BROWSE', 'root_node_identifier_type': 'NUMERIC'},
                                              {'nodeid_fetch_mode': 'BROWSE', 'root_node_identifier_type': 'OPAQUE'},
                                              {'nodeid_fetch_mode': 'BROWSE', 'root_node_identifier_type': 'STRING'},
                                              {'nodeid_fetch_mode': 'BROWSE', 'root_node_identifier_type': 'UUID'}])
def test_root_node_identifier_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'nodeid_fetch_mode': 'BROWSE'}])
def test_root_node_namespace_index(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'security_policy': 'BASIC_128_RSA_15'},
                                              {'security_policy': 'BASIC_256'},
                                              {'security_policy': 'BASIC_256_SHA_256'},
                                              {'security_policy': 'NONE'}])
def test_security_policy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_session_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True}])
def test_transport_protocols(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass

