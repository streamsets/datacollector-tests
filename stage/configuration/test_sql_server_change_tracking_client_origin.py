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
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'auto_commit': False}, {'auto_commit': True}])
def test_auto_commit(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'per_batch_strategy': 'SWITCH_TABLES'}])
def test_batches_from_result_set(sdc_builder, sdc_executor, stage_attributes):
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
def test_data_time_zone(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enforce_read_only_connection': False},
                                              {'enforce_read_only_connection': True}])
def test_enforce_read_only_connection(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_fetch_size(sdc_builder, sdc_executor):
    pass


@stub
def test_idle_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'include_the_latest_data_in_the_record': False},
                                              {'include_the_latest_data_in_the_record': True}])
def test_include_the_latest_data_in_the_record(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_init_query(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'initial_table_order_strategy': 'ALPHABETICAL'},
                                              {'initial_table_order_strategy': 'NONE'}])
def test_initial_table_order_strategy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_jdbc_connection_string(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_driver_class_name(sdc_builder, sdc_executor):
    pass


@stub
def test_max_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@stub
def test_max_blob_size_in_bytes(sdc_builder, sdc_executor):
    pass


@stub
def test_max_clob_size_in_characters(sdc_builder, sdc_executor):
    pass


@stub
def test_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
def test_maximum_pool_size(sdc_builder, sdc_executor):
    pass


@stub
def test_minimum_idle_connections(sdc_builder, sdc_executor):
    pass


@stub
def test_new_table_discovery_interval(sdc_builder, sdc_executor):
    pass


@stub
def test_no_more_data_event_generation_delay_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
def test_number_of_retries_on_sql_error(sdc_builder, sdc_executor):
    pass


@stub
def test_number_of_threads(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'per_batch_strategy': 'PROCESS_ALL_AVAILABLE_ROWS_FROM_TABLE'},
                                              {'per_batch_strategy': 'SWITCH_TABLES'}])
def test_per_batch_strategy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_queries_per_second(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'per_batch_strategy': 'SWITCH_TABLES'}])
def test_result_set_cache_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_table_configs(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass

