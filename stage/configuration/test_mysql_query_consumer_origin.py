import logging
import string

import pytest
import sqlalchemy
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import category, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Lionel Messi'},
    {'id': 2, 'name': 'Christiano Ronaldo'},
    {'id': 3, 'name': 'Paul Pogba'}
]

pytestmark = [pytest.mark.sdc_min_version('3.18.0'), pytest.mark.database('mysql')]
pytestmark = pytest.mark.skip("All tests are skipped since branded JDBC stages are not ready yet.")


@stub
@category('advanced')
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'auto_commit': False}, {'auto_commit': True}])
def test_auto_commit(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'ssl_mode': 'VERIFY_CA'}, {'ssl_mode': 'VERIFY_IDENTITY'}])
def test_ca_certificate_pem(sdc_builder, sdc_executor, database, stage_attributes):
    if database.ca_certificate_file_contents is None:
        pytest.skip("The database isn't configured to support SSL.")

    stage_attributes.update({'ca_certificate_pem': database.ca_certificate_file_contents})
    _test_sql_query(sdc_builder, sdc_executor, database, stage_attributes)


@stub
@category('advanced')
def test_connection_health_test_query(sdc_builder, sdc_executor, database):
    pass


@category('basic')
def test_connection_string(sdc_builder, sdc_executor, database):
    _test_sql_query(sdc_builder, sdc_executor, database)


@stub
@category('advanced')
def test_connection_timeout_in_seconds(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'convert_timestamp_to_string': False},
                                              {'convert_timestamp_to_string': True}])
def test_convert_timestamp_to_string(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'create_jdbc_header_attributes': False},
                                              {'create_jdbc_header_attributes': True}])
def test_create_jdbc_header_attributes(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'disable_query_validation': False}, {'disable_query_validation': True}])
def test_disable_query_validation(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'enforce_read_only_connection': False},
                                              {'enforce_read_only_connection': True}])
def test_enforce_read_only_connection(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_idle_timeout_in_seconds(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': False}, {'incremental_mode': True}])
def test_incremental_mode(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_init_query(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
def test_initial_offset(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_jdbc_driver_class_name(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'create_jdbc_header_attributes': True}])
def test_jdbc_header_prefix(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_max_batch_size_in_records(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_max_blob_size_in_bytes(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_max_clob_size_in_characters(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_max_connection_lifetime_in_seconds(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_max_transaction_size(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_maximum_pool_size(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_maximum_transaction_length(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_minimum_idle_connections(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_new_table_discovery_interval(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_no_more_data_event_generation_delay_in_seconds(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_number_of_retries_on_sql_error(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
def test_offset_column(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'on_unknown_type': 'CONVERT_TO_STRING'},
                                              {'on_unknown_type': 'STOP_PIPELINE'}])
def test_on_unknown_type(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, database, stage_attributes):
    _test_sql_query(sdc_builder, sdc_executor, database, stage_attributes)


@stub
@category('advanced')
def test_query_interval(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'root_field_type': 'LIST'},
                                              {'root_field_type': 'LIST_MAP'},
                                              {'root_field_type': 'MAP'}])
def test_root_field_type(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@category('basic')
def test_sql_query(sdc_builder, sdc_executor, database):
    _test_sql_query(sdc_builder, sdc_executor, database)


@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'ssl_mode': 'DISABLED'},
                                              {'ssl_mode': 'REQUIRED'},
                                              {'ssl_mode': 'VERIFY_CA'},
                                              {'ssl_mode': 'VERIFY_IDENTITY'}])
def test_ssl_mode(sdc_builder, sdc_executor, database, stage_attributes):
    if database.ca_certificate_file_contents is None:
        pytest.skip("The database isn't configured to support SSL.")

    if stage_attributes['ssl_mode'] in ('DISABLED', 'REQUIRED'):
        _test_sql_query(sdc_builder, sdc_executor, database, stage_attributes)
    elif stage_attributes['ssl_mode'] in ('VERIFY_CA', 'VERIFY_IDENTITY'):
        stage_attributes.update({'ca_certificate_pem': database.ca_certificate_file_contents})
        _test_sql_query(sdc_builder, sdc_executor, database, stage_attributes)


@stub
@category('advanced')
def test_transaction_id_column_name(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'transaction_isolation': 'DEFAULT'},
                                              {'transaction_isolation': 'TRANSACTION_READ_COMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_READ_UNCOMMITTED'},
                                              {'transaction_isolation': 'TRANSACTION_REPEATABLE_READ'},
                                              {'transaction_isolation': 'TRANSACTION_SERIALIZABLE'}])
def test_transaction_isolation(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, database, stage_attributes):
    _test_sql_query(sdc_builder, sdc_executor, database, stage_attributes)


def _test_sql_query(sdc_builder, sdc_executor, database, stage_attributes=None):
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_name = '{}_{}'.format(src_table_prefix, get_random_string(string.ascii_lowercase, 20))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    sql_query = f'SELECT * FROM {table_name} WHERE id > ${{OFFSET}} ORDER BY id'
    mysql_query_consumer = pipeline_builder.add_stage('MySQL Query Consumer')
    mysql_query_consumer.set_attributes(sql_query=sql_query,
                                        initial_offset='0',
                                        offset_column='id',
                                        **stage_attributes if stage_attributes else {})
    trash = pipeline_builder.add_stage('Trash')
    mysql_query_consumer >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    try:
        logger.info('Creating table %s ...', table_name)
        table.create(database.engine)

        logger.info('Adding three rows into database ...')
        connection = database.engine.connect()
        connection.execute(table.insert(), ROWS_IN_DATABASE)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        rows_from_snapshot = [record.field['name']
                              for record in snapshot[pipeline.stages[0]].output]
        assert rows_from_snapshot == [row['name'] for row in ROWS_IN_DATABASE]

    finally:
        _clean_up(database, table, table_name)


def _clean_up(database, table, table_name):
    logger.info('Dropping table %s in %s database...', table_name, database.type)
    table.drop(database.engine)
