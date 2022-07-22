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
import logging
import string

from streamsets.sdk.utils import Version
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_NAME = 'dbo'


@stub
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'allow_late_tables': False}, {'allow_late_tables': True}])
def test_allow_late_tables(sdc_builder, sdc_executor, stage_attributes):
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


@database('sqlserver')
@sdc_min_version('4.0.0')
@pytest.mark.parametrize('enable_schema_changes_event', [False, True])
@pytest.mark.parametrize('combine_update_records', [True, False])
def test_enable_schema_changes_event(
        sdc_builder,
        sdc_executor,
        database,
        enable_schema_changes_event,
        combine_update_records
):
    """Test for SQL Server CDC origin stage 'Enable Schema Changes Event' parameter.
    Create a table and enable CDC on it, then add some data and change the table schema. If the parameter is true,
    we should receive an extra event indicating the schema has changed.
    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    And for events:
        sql_server_cdc_origin >= wiretap
    """

    if Version(sdc_builder.version) < Version('5.2.0') and combine_update_records:
        pytest.skip('The Combine Update Records option in not available until version 5.2.0.')

    num_of_tables = 1
    schema_name = DEFAULT_SCHEMA_NAME
    table_prefix = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('SQL Server CDC Client')
    origin.enable_schema_changes_event = enable_schema_changes_event
    origin.table_configs = [{'capture_instance': f'dbo_{table_prefix}_%'}]
    if Version(sdc_builder.version) >= Version('5.2.0'):
        origin.combine_update_records = combine_update_records

    wiretap_out = builder.add_wiretap()
    wiretap_event = builder.add_wiretap()

    origin >> wiretap_out.destination
    origin >= wiretap_event.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    connection = database.engine.connect()
    try:
        test_value = 42

        table_name = f"{table_prefix}_{get_random_string(string.ascii_lowercase, 20)}"
        connection.execute(f"CREATE TABLE {schema_name}.{table_name}(id INT)")
        _enable_cdc(connection, schema_name, table_name)
        connection.execute(f"INSERT INTO {schema_name}.{table_name} VALUES({test_value})")


        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap_out.output_records
        assert len(records) == 1
        record_field_data = records[0].field['Data'] if combine_update_records else records[0].field
        assert record_field_data['id'] == test_value

        # Add a new column to the tables
        logger.info('Adding the column new_column varchar(10) on %s.%s...', schema_name, table_name)
        connection.execute(f'ALTER TABLE {table_name} ADD new_column INT')

        # Reset record accumulation
        wiretap_out.reset()
        wiretap_event.reset()

        # Insert new data to the tables
        logger.info("Inserting new row into %s", table_name)
        connection.execute(f"INSERT INTO {schema_name}.{table_name} VALUES({test_value}, {test_value})")

        logger.info("Waiting on pipeline to finish processing all records")
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        # Check that the insert was read and the number of times the schema change event was recorded
        records = wiretap_out.output_records
        events = wiretap_event.output_records

        assert len(records) == num_of_tables
        record_field_data = records[0].field['Data'] if combine_update_records else records[0].field
        assert record_field_data['id'] == test_value

        changes = 0
        event_message = ['source-table-name']
        for i in range(0, len(events)):
            if any(substring in str(events[i]) for substring in event_message):
                changes += 1

        # If 'enable_schema_changes_event' is true, the event should be registered once, otherwise it should not appear
        if enable_schema_changes_event:
            assert changes == 1
        else:
            assert changes == 0

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        _disable_cdc(connection, schema_name, table_name)
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE {schema_name}.{table_name}")


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
def test_maximum_transaction_length(sdc_builder, sdc_executor):
    pass


@stub
def test_minimum_idle_connections(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'allow_late_tables': True}])
def test_new_table_discovery_interval(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'reconnect_for_each_query': False}, {'reconnect_for_each_query': True}])
def test_reconnect_for_each_query(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'use_direct_table_query': False}, {'use_direct_table_query': True}])
def test_use_direct_table_query(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass


def _enable_cdc(connection, schema_name, table_name, capture_instance=None):
    if capture_instance is None:
        capture_instance = f"{schema_name}_{table_name}"

    logger.info('Enabling CDC on %s.%s into table %s...', schema_name, table_name, capture_instance)
    connection.execute(f'EXEC sys.sp_cdc_enable_table '
                       f'@source_schema=N\'{schema_name}\', '
                       f'@source_name=N\'{table_name}\','
                       f'@role_name = NULL, '
                       f'@capture_instance={capture_instance}')


def _disable_cdc(connection, schema_name, table_name, capture_instance=None):
    if capture_instance is None:
        capture_instance = f"{schema_name}_{table_name}"

    logger.info('Disabling CDC on %s.%s from table %s...', schema_name, table_name, capture_instance)
    connection.execute(
        f'EXEC sys.sp_cdc_disable_table '
        f'@source_schema=N\'{schema_name}\', '
        f'@source_name=N\'{table_name}\','
        f'@capture_instance={capture_instance}')