# Copyright 2020 StreamSets Inc.
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
import logging
import string

import pytest
import sqlalchemy
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Lionel Messi'},
    {'id': 2, 'name': 'Christiano Ronaldo'},
    {'id': 3, 'name': 'Paul Pogba'}
]


@stub
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'auto_commit': False}, {'auto_commit': True}])
def test_auto_commit(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'create_jdbc_header_attributes': False},
                                              {'create_jdbc_header_attributes': True}])
def test_create_jdbc_header_attributes(sdc_builder, sdc_executor, stage_attributes):
    pass


@database
@pytest.mark.parametrize('stage_attributes', [{'disable_query_validation': False}, {'disable_query_validation': True}])
def test_disable_query_validation(sdc_builder, sdc_executor, stage_attributes, database):
    """The validation disable is really useful only for non-compliant databases that we don't actually support, so this
       test focuses on making sure that the origin still works with both query validation set on/off. It doesn't test
       what that config option does (or leads to).
    """
    _test_sql_query(sdc_builder, sdc_executor, database, stage_attributes)


@stub
@pytest.mark.parametrize('stage_attributes', [{'enforce_read_only_connection': False},
                                              {'enforce_read_only_connection': True}])
def test_enforce_read_only_connection(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_idle_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': False}, {'incremental_mode': True}])
def test_incremental_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_init_query(sdc_builder, sdc_executor):
    pass


@stub
def test_initial_offset(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_connection_string(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_driver_class_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'create_jdbc_header_attributes': True}])
def test_jdbc_header_prefix(sdc_builder, sdc_executor, stage_attributes):
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
def test_max_transaction_size(sdc_builder, sdc_executor):
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
def test_new_table_discovery_interval(sdc_builder, sdc_executor):
    pass


@stub
def test_no_more_data_event_generation_delay_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
def test_number_of_retries_on_sql_error(sdc_builder, sdc_executor):
    pass


@stub
def test_offset_column(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_unknown_type': 'CONVERT_TO_STRING'},
                                              {'on_unknown_type': 'STOP_PIPELINE'}])
def test_on_unknown_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_query_interval(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'root_field_type': 'LIST'},
                                              {'root_field_type': 'LIST_MAP'},
                                              {'root_field_type': 'MAP'}])
def test_root_field_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_sql_query(sdc_builder, sdc_executor):
    pass


@stub
def test_transaction_id_column_name(sdc_builder, sdc_executor):
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


def _test_sql_query(sdc_builder, sdc_executor, database, stage_attributes=None):
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    sql_query = f'SELECT * FROM {table_name} WHERE id > ${{OFFSET}} ORDER BY id'
    mysql_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    mysql_query_consumer.set_attributes(sql_query=sql_query,
                                        initial_offset='0',
                                        offset_column='id',
                                        **stage_attributes if stage_attributes else {})
    wiretap = pipeline_builder.add_wiretap()
    mysql_query_consumer >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32), quote=True)
    )
    try:
        logger.info('Creating table %s ...', table_name)
        table.create(database.engine)

        logger.info('Adding three rows into database ...')
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
        sdc_executor.stop_pipeline(pipeline)

        rows_from_snapshot = [record.field['name']
                              for record in wiretap.output_records]
        assert rows_from_snapshot == [row['name'] for row in ROWS_IN_DATABASE]

    finally:
        _clean_up(database, table, table_name)


def _clean_up(database, table, table_name):
    logger.info('Dropping table %s in %s database...', table_name, database.type)
    table.drop(database.engine)
