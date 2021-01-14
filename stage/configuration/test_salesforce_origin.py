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
def test_api_version(sdc_builder, sdc_executor):
    pass


@stub
def test_auth_endpoint(sdc_builder, sdc_executor):
    pass


@stub
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'subscribe_for_notifications': True, 'subscription_type': 'CDC'}])
def test_change_data_capture_object(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': True,
                                               'use_bulk_api': True,
                                               'use_pk_chunking': True}])
def test_chunk_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'create_salesforce_attributes': False},
                                              {'create_salesforce_attributes': True}])
def test_create_salesforce_attributes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'disable_query_validation': False}, {'disable_query_validation': True}])
def test_disable_query_validation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'include_deleted_records': False, 'query_existing_data': True},
                                              {'include_deleted_records': True, 'query_existing_data': True}])
def test_include_deleted_records(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': True}])
def test_initial_offset(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': True}])
def test_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': True}])
def test_keystore_key_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': True}])
def test_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keystore_type': 'JKS', 'use_mutual_authentication': True},
                                              {'keystore_type': 'PKCS12', 'use_mutual_authentication': True}])
def test_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mismatched_types_behavior': 'PRESERVE_DATA'},
                                              {'mismatched_types_behavior': 'ROUND_DATA'},
                                              {'mismatched_types_behavior': 'TRUNCATE_DATA'}])
def test_mismatched_types_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': True}])
def test_offset_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_password(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'subscribe_for_notifications': True,
                                               'subscription_type': 'PLATFORM_EVENT'}])
def test_platform_event_api_name(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_hostname(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_port(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_realm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': False, 'use_proxy': True},
                                              {'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_requires_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_username(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'subscribe_for_notifications': True, 'subscription_type': 'PUSH_TOPIC'}])
def test_push_topic(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': False}, {'query_existing_data': True}])
def test_query_existing_data(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': True,
                                               'repeat_query': 'FULL',
                                               'subscribe_for_notifications': False},
                                              {'query_existing_data': True,
                                               'repeat_query': 'INCREMENTAL',
                                               'subscribe_for_notifications': False}])
def test_query_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': True,
                                               'repeat_query': 'FULL',
                                               'subscribe_for_notifications': False},
                                              {'query_existing_data': True,
                                               'repeat_query': 'INCREMENTAL',
                                               'subscribe_for_notifications': False},
                                              {'query_existing_data': True,
                                               'repeat_query': 'NO_REPEAT',
                                               'subscribe_for_notifications': False}])
def test_repeat_query(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'replay_option': 'ALL_EVENTS',
                                               'subscribe_for_notifications': True,
                                               'subscription_type': 'PLATFORM_EVENT'},
                                              {'replay_option': 'NEW_EVENTS',
                                               'subscribe_for_notifications': True,
                                               'subscription_type': 'PLATFORM_EVENT'}])
def test_replay_option(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'create_salesforce_attributes': True}])
def test_salesforce_attribute_prefix(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': True}])
def test_soql_query(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': True,
                                               'use_bulk_api': True,
                                               'use_pk_chunking': True}])
def test_start_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'subscribe_for_notifications': False},
                                              {'subscribe_for_notifications': True}])
def test_subscribe_for_notifications(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'subscribe_for_notifications': True, 'subscription_type': 'CDC'},
                                              {'subscribe_for_notifications': True,
                                               'subscription_type': 'PLATFORM_EVENT'},
                                              {'subscribe_for_notifications': True, 'subscription_type': 'PUSH_TOPIC'}])
def test_subscription_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': True, 'use_bulk_api': False},
                                              {'query_existing_data': True, 'use_bulk_api': True}])
def test_use_bulk_api(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': False}, {'use_mutual_authentication': True}])
def test_use_mutual_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'query_existing_data': True,
                                               'use_bulk_api': True,
                                               'use_pk_chunking': False},
                                              {'query_existing_data': True,
                                               'use_bulk_api': True,
                                               'use_pk_chunking': True}])
def test_use_pk_chunking(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': False}, {'use_proxy': True}])
def test_use_proxy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_username(sdc_builder, sdc_executor):
    pass

