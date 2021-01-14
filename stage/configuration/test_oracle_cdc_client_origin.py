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
@pytest.mark.parametrize('stage_attributes', [{'add_unsupported_fields_to_records': False},
                                              {'add_unsupported_fields_to_records': True}])
def test_add_unsupported_fields_to_records(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'buffer_changes_locally': False}, {'buffer_changes_locally': True}])
def test_buffer_changes_locally(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'buffer_changes_locally': True, 'buffer_location': 'IN_MEMORY'},
                                              {'buffer_changes_locally': True, 'buffer_location': 'ON_DISK'}])
def test_buffer_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'case_sensitive_names': False}, {'case_sensitive_names': True}])
def test_case_sensitive_names(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connection_health_test_query(sdc_builder, sdc_executor):
    pass


@stub
def test_connection_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'convert_timestamp_to_string': False},
                                              {'convert_timestamp_to_string': True}])
def test_convert_timestamp_to_string(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_db_time_zone(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'dictionary_source': 'DICT_FROM_ONLINE_CATALOG'},
                                              {'dictionary_source': 'DICT_FROM_REDO_LOGS'}])
def test_dictionary_source(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'buffer_changes_locally': True,
                                               'discard_old_uncommitted_transactions': False},
                                              {'buffer_changes_locally': True,
                                               'discard_old_uncommitted_transactions': True}])
def test_discard_old_uncommitted_transactions(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enforce_read_only_connection': False},
                                              {'enforce_read_only_connection': True}])
def test_enforce_read_only_connection(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_idle_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'include_nulls': False}, {'include_nulls': True}])
def test_include_nulls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_init_query(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'initial_change': 'DATE'},
                                              {'initial_change': 'LATEST'},
                                              {'initial_change': 'SCN'}])
def test_initial_change(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_jdbc_connection_string(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_driver_class_name(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_fetch_size_for_current_window(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_fetch_size_for_past_windows(sdc_builder, sdc_executor):
    pass


@stub
def test_logminer_session_window(sdc_builder, sdc_executor):
    pass


@stub
def test_max_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@stub
def test_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
def test_maximum_pool_size(sdc_builder, sdc_executor):
    pass


@stub
def test_maximum_transaction_length(sdc_builder, sdc_executor):
    pass


@stub
def test_minimum_idle_connections(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_operations(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'parse_sql_query': False}, {'parse_sql_query': True}])
def test_parse_sql_query(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'buffer_changes_locally': True, 'parse_sql_query': True}])
def test_parsing_thread_pool_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_pdb(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'send_redo_query_in_headers': False},
                                              {'send_redo_query_in_headers': True}])
def test_send_redo_query_in_headers(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'initial_change': 'DATE'}])
def test_start_date(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'initial_change': 'SCN'}])
def test_start_scn(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_tables(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'transaction_isolation': 'DEFAULT'},
                                              {'transaction_isolation': 'TRANSACTION_READ_COMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_READ_UNCOMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_REPEATABLE_READ'},
                                              {'transaction_isolation': 'TRANSACTION_SERIALIZABLE'}])
def test_transaction_isolation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'unsupported_field_type': 'DISCARD'},
                                              {'unsupported_field_type': 'SEND_TO_PIPELINE'},
                                              {'unsupported_field_type': 'TO_ERROR'}])
def test_unsupported_field_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'parse_sql_query': True, 'use_peg_parser_in_beta': False},
                                              {'parse_sql_query': True, 'use_peg_parser_in_beta': True}])
def test_use_peg_parser_in_beta(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass

