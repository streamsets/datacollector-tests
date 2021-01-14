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
def test_advanced_environment_settings(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_mode': 'BUCKET'}, {'authentication_mode': 'USER'}])
def test_authentication_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_bucket(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_mode': 'BUCKET'}])
def test_bucket_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connect_timeout_in_ms(sdc_builder, sdc_executor):
    pass


@stub
def test_disconnect_timeout_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_type': 'KV'}])
def test_document_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_key_value_timeout_in_ms(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'lookup_type': 'KV'}, {'lookup_type': 'N1QL'}])
def test_lookup_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_value_behavior': 'ERROR'}, {'missing_value_behavior': 'PASS'}])
def test_missing_value_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_type': 'N1QL', 'multiple_value_behavior': 'FIRST'},
                                              {'lookup_type': 'N1QL', 'multiple_value_behavior': 'MULTI'}])
def test_multiple_value_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_type': 'N1QL'}])
def test_n1ql_query(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_node_list(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_mode': 'USER'}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_type': 'KV', 'return_properties': True}, {'lookup_type': 'N1QL'}])
def test_property_mappings(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_type': 'N1QL'}])
def test_query_timeout_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_type': 'KV', 'return_properties': False},
                                              {'lookup_type': 'KV', 'return_properties': True}])
def test_return_properties(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_type': 'KV', 'return_properties': False}])
def test_sdc_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_type': 'N1QL', 'submit_as_prepared_statement': False},
                                              {'lookup_type': 'N1QL', 'submit_as_prepared_statement': True}])
def test_submit_as_prepared_statement(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
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
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_mode': 'USER'}])
def test_user_name(sdc_builder, sdc_executor, stage_attributes):
    pass

