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
@pytest.mark.parametrize('stage_attributes', [{'authentication_provider': 'DSE_PLAINTEXT'},
                                              {'authentication_provider': 'KERBEROS'},
                                              {'authentication_provider': 'NONE'},
                                              {'authentication_provider': 'PLAINTEXT'}])
def test_authentication_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'batch_type': 'LOGGED'}, {'batch_type': 'UNLOGGED'}])
def test_batch_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_cassandra_contact_points(sdc_builder, sdc_executor):
    pass


@stub
def test_cassandra_port(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True}])
def test_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'compression': 'LZ4'},
                                              {'compression': 'NONE'},
                                              {'compression': 'SNAPPY'}])
def test_compression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connection_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'consistency_level': 'ALL'},
                                              {'consistency_level': 'ANY'},
                                              {'consistency_level': 'EACH_QUORUM'},
                                              {'consistency_level': 'LOCAL_ONE'},
                                              {'consistency_level': 'LOCAL_QUORUM'},
                                              {'consistency_level': 'LOCAL_SERIAL'},
                                              {'consistency_level': 'ONE'},
                                              {'consistency_level': 'QUORUM'},
                                              {'consistency_level': 'SERIAL'},
                                              {'consistency_level': 'THREE'},
                                              {'consistency_level': 'TWO'}])
def test_consistency_level(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'disable_batch_insert': False}, {'disable_batch_insert': True}])
def test_disable_batch_insert(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_field_to_column_mapping(sdc_builder, sdc_executor):
    pass


@stub
def test_fully_qualified_table_name(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'log_slow_queries': False}, {'log_slow_queries': True}])
def test_log_slow_queries(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_batch_size(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_provider': 'DSE_PLAINTEXT'},
                                              {'authentication_provider': 'PLAINTEXT'}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'protocol_version': 'V1'},
                                              {'protocol_version': 'V2'},
                                              {'protocol_version': 'V3'},
                                              {'protocol_version': 'V4'},
                                              {'protocol_version': 'V5'}])
def test_protocol_version(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_read_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'disable_batch_insert': True}])
def test_request_timeout(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'log_slow_queries': True}])
def test_slow_query_logging_threshold(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_provider': 'DSE_PLAINTEXT'},
                                              {'authentication_provider': 'PLAINTEXT'}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass

